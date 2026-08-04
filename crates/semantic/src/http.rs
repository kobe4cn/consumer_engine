//! HTTP LLM + embedding clients (feature `semantic-llm`, spec 13 §4).
//!
//! Real network clients behind the same trait seams as the deterministic stubs,
//! with a timeout and a single retry (AGENTS.md § Resource Limits). A failure
//! surfaces as an error so the caller decides: the Profiler degrades to a stub
//! description / zero vector, the IntentRag propagates
//! `Error::CatalogueUnavailable` (the agent must not guess columns).

use std::time::Duration;

use async_trait::async_trait;
use consumer_engine_core::{Error, LlmConfig, Result};
use serde_json::json;

use crate::llm::{EmbeddingModel, LlmClient};

/// Per-call budget (spec 13 §4: LLM calls have a timeout + retry budget).
const CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Retries after a transient failure.
const RETRIES: usize = 1;

/// An OpenAI-compatible chat-completions LLM client (description generation).
pub struct HttpLlm {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl std::fmt::Debug for HttpLlm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpLlm")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl HttpLlm {
    /// Build an HTTP LLM client from config.
    #[must_use]
    pub fn new(cfg: &LlmConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: cfg.base_url.clone(),
            api_key: cfg.api_key.clone(),
        }
    }

    async fn complete(&self, prompt: &str) -> Result<String> {
        let body = json!({
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 64,
        });
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let mut last = None;
        for _ in 0..=RETRIES {
            let res = tokio::time::timeout(CALL_TIMEOUT, self.request(&url, &body)).await;
            match res {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) => last = Some(e),
                Err(_) => last = Some(Error::CatalogueUnavailable),
            }
        }
        Err(last.unwrap_or(Error::CatalogueUnavailable))
    }

    async fn request(&self, url: &str, body: &serde_json::Value) -> Result<String> {
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .map_err(|e| Error::Execution(Box::from(e)))?;
        if !resp.status().is_success() {
            return Err(Error::CatalogueUnavailable);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Execution(Box::from(e)))?;
        v["choices"][0]["message"]["content"]
            .as_str()
            .map(str::to_string)
            .ok_or(Error::CatalogueUnavailable)
    }
}

#[async_trait]
impl LlmClient for HttpLlm {
    async fn describe_column(
        &self,
        system: &str,
        table: &str,
        column: &str,
        data_type: &str,
        sample: &[String],
    ) -> Result<String> {
        let prompt = format!(
            "Describe the '{column}' column ({data_type}) of {system}.{table} in one sentence for \
             a marketing analyst. Example values: {sample:?}"
        );
        self.complete(&prompt).await
    }
}

/// An OpenAI-compatible embeddings client.
pub struct HttpEmbedding {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    dim: usize,
}

impl std::fmt::Debug for HttpEmbedding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpEmbedding")
            .field("base_url", &self.base_url)
            .field("dim", &self.dim)
            .finish_non_exhaustive()
    }
}

impl HttpEmbedding {
    /// Build an HTTP embedding client from config.
    #[must_use]
    pub fn new(cfg: &LlmConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: cfg.base_url.clone(),
            api_key: cfg.api_key.clone(),
            dim: cfg.embedding_dim,
        }
    }

    /// POST `/embeddings` and parse `data[0].embedding`.
    async fn request(&self, url: &str, body: &serde_json::Value) -> Result<Vec<f32>> {
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .map_err(|e| Error::Execution(Box::from(e)))?;
        if !resp.status().is_success() {
            return Err(Error::CatalogueUnavailable);
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Execution(Box::from(e)))?;
        let arr = v["data"][0]["embedding"]
            .as_array()
            .ok_or(Error::CatalogueUnavailable)?;
        let mut out = Vec::with_capacity(arr.len());
        for x in arr {
            out.push(x.as_f64().ok_or(Error::CatalogueUnavailable)? as f32);
        }
        Ok(out)
    }
}

#[async_trait]
impl EmbeddingModel for HttpEmbedding {
    fn dim(&self) -> usize {
        self.dim
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let body = json!({"input": text, "model": "text-embedding-3-small"});
        let url = format!("{}/embeddings", self.base_url.trim_end_matches('/'));
        let mut last = None;
        for _ in 0..=RETRIES {
            let res = tokio::time::timeout(CALL_TIMEOUT, self.request(&url, &body)).await;
            match res {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) => last = Some(e),
                Err(_) => last = Some(Error::CatalogueUnavailable),
            }
        }
        Err(last.unwrap_or(Error::CatalogueUnavailable))
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    #[tokio::test]
    async fn test_should_http_embed_parse_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"embedding": [0.1, 0.2, 0.3]}]
            })))
            .mount(&server)
            .await;
        let cfg = LlmConfig {
            base_url: server.uri(),
            api_key: "k".into(),
            embedding_dim: 3,
        };
        let model = HttpEmbedding::new(&cfg);
        let v = model.embed("hello").await.expect("embed");
        assert_eq!(v.len(), 3);
        assert!((v[0] - 0.1).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_should_http_embed_fail_on_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let cfg = LlmConfig {
            base_url: server.uri(),
            api_key: "k".into(),
            embedding_dim: 3,
        };
        let model = HttpEmbedding::new(&cfg);
        assert!(
            model.embed("hello").await.is_err(),
            "5xx must surface as an error"
        );
    }
}
