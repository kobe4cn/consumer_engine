//! L0 Profiler — runs once per source onboarding (D4), samples a `raw_*` table,
//! classifies columns, redacts PII, and builds `CatalogRow`s for the agent.
//!
//! Bounded by `sample_rows` / `sample_value_bytes` (spec 13 §3 I2). PII sample
//! values are redacted **before** any LLM description or embedding is generated
//! (I4): only the description is embedded, never PII values.

use std::sync::Arc;

use consumer_engine_core::{
    CatalogRow, READ_ONLY_CATALOG_ALIAS, Result, SemanticType, validate_ident,
};
use consumer_engine_execution::Reader;
use serde_json::Value;

use crate::{
    limits::{DEFAULT_SAMPLE_ROWS, DEFAULT_SAMPLE_VALUE_BYTES, MAX_SAMPLE_VALUES},
    llm::{EmbeddingModel, LlmClient},
};

/// The L0 onboarding profiler. Cheap to share via `Arc`.
pub struct Profiler {
    reader: Reader,
    llm: Arc<dyn LlmClient>,
    embed: Arc<dyn EmbeddingModel>,
    sample_rows: usize,
    sample_value_bytes: usize,
}

impl std::fmt::Debug for Profiler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Profiler")
            .field("sample_rows", &self.sample_rows)
            .field("sample_value_bytes", &self.sample_value_bytes)
            .finish_non_exhaustive()
    }
}

impl Profiler {
    /// Build a profiler over `reader` with M3 stub clients and default bounds.
    #[must_use]
    pub fn new(reader: Reader, llm: Arc<dyn LlmClient>, embed: Arc<dyn EmbeddingModel>) -> Self {
        Self {
            reader,
            llm,
            embed,
            sample_rows: DEFAULT_SAMPLE_ROWS,
            sample_value_bytes: DEFAULT_SAMPLE_VALUE_BYTES,
        }
    }

    /// Override the sample-row cap (I2). Builder-style.
    #[must_use]
    pub fn with_sample_rows(mut self, n: usize) -> Self {
        self.sample_rows = n.max(1);
        self
    }

    /// Override the per-value byte cap (I2). Builder-style.
    #[must_use]
    pub fn with_sample_value_bytes(mut self, n: usize) -> Self {
        self.sample_value_bytes = n.max(1);
        self
    }

    /// Profile `system`.`table`: bounded sample, classify, redact PII, describe,
    /// embed, and return one `CatalogRow` per column. The caller writes them
    /// via the single ingestion writer (spec 13 §2 — onboarding-only writes).
    ///
    /// Raw tables store every column as `VARCHAR` (see `storage::ingest_raw`),
    /// so `data_type` is reported as `"VARCHAR"`; all sampled values are strings.
    ///
    /// # Errors
    /// - `consumer_engine_core::Error::InvalidInput` on a bad identifier or a missing raw table.
    /// - `consumer_engine_core::Error::Execution` on a reader failure.
    pub async fn onboard(&self, system: &str, table: &str) -> Result<Vec<CatalogRow>> {
        validate_ident(system)?;
        validate_ident(table)?;
        let sql = format!(
            "SELECT * FROM {READ_ONLY_CATALOG_ALIAS}.raw_{system}_{table} LIMIT {}",
            self.sample_rows
        );
        let qr = self.reader.query_with_params(&sql, Vec::new()).await?;
        let columns = qr.columns;

        // Per-column sample values: distinct non-null strings, bounded.
        let per_column_samples = sample_columns(&qr.rows, &columns, self.sample_value_bytes);

        let mut rows = Vec::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            let semantic_type = classify(column);
            let pii_flag = semantic_type == SemanticType::Pii;

            // I4: redact PII samples before any LLM/embedding touch them.
            let stored_samples: Vec<String> = if pii_flag {
                vec!["[redacted]".to_string()]
            } else {
                per_column_samples.get(idx).cloned().unwrap_or_default()
            };
            // The LLM never sees PII values: pass an empty sample list for PII.
            let llm_samples: Vec<String> = if pii_flag {
                Vec::new()
            } else {
                per_column_samples.get(idx).cloned().unwrap_or_default()
            };

            let description = match self
                .llm
                .describe_column(system, table, column, "VARCHAR", &llm_samples)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        %system,
                        %table,
                        %column,
                        "LLM description failed; using stub description"
                    );
                    format!("Column '{column}' of {system}.{table}")
                }
            };
            // I4: only the description is embedded, never PII values.
            let embedding = self.embed.embed(&description);

            rows.push(CatalogRow {
                entity_type: "column".into(),
                system: system.into(),
                table_name: table.into(),
                column_name: Some(column.clone()),
                semantic_type,
                data_type: "VARCHAR".into(),
                description,
                pii_flag,
                sample_values: Value::Array(
                    stored_samples.into_iter().map(Value::String).collect(),
                ),
                embedding,
            });
        }
        Ok(rows)
    }
}

/// Collect distinct, byte-bounded sample values per column (≤`MAX_SAMPLE_VALUES`).
fn sample_columns(rows: &[Vec<Value>], columns: &[String], value_bytes: usize) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = (0..columns.len()).map(|_| Vec::new()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            // `idx < out.len()` by construction (checked below); get_mut keeps
            // the no-indexing lint set clean.
            let Some(bucket) = out.get_mut(idx) else {
                break;
            };
            if bucket.len() >= MAX_SAMPLE_VALUES {
                continue;
            }
            if let Value::String(s) = cell {
                let truncated = truncate_bytes(s, value_bytes).to_string();
                if !bucket.iter().any(|existing| existing == &truncated) {
                    bucket.push(truncated);
                }
            }
        }
    }
    out
}

/// Truncate `s` to at most `max` bytes at a UTF-8 char boundary.
fn truncate_bytes(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Heuristically classify a column by its name into a [`SemanticType`].
fn classify(name: &str) -> SemanticType {
    let l = name.to_ascii_lowercase();
    if is_pii(&l) {
        SemanticType::Pii
    } else if l == "id" || l.ends_with("_id") {
        SemanticType::Identifier
    } else if l == "ts"
        || l.ends_with("_at")
        || l.ends_with("_ts")
        || l.contains("timestamp")
        || l.ends_with("date")
    {
        SemanticType::EventTs
    } else if is_measure(&l) {
        SemanticType::Measure
    } else {
        SemanticType::Dimension
    }
}

/// PII name-substring allowlist (a heuristic; conservative — over-flagging is
/// safe because redaction is the default posture).
fn is_pii(l: &str) -> bool {
    const KEYS: &[&str] = &[
        "email", "phone", "ssn", "address", "password", "token", "name",
    ];
    KEYS.iter().any(|k| l.contains(k))
}

/// Measure name-substring allowlist.
fn is_measure(l: &str) -> bool {
    const KEYS: &[&str] = &[
        "amount", "count", "total", "sum", "price", "qty", "quantity", "score", "value", "revenue",
        "cost",
    ];
    KEYS.iter().any(|k| l.contains(k))
}

#[cfg(test)]
mod tests {
    use consumer_engine_storage::{Writer, open_reader, read_only_attach_sql};

    use super::*;
    use crate::llm::{StubEmbed, StubLlm};

    fn tmp_reader() -> (tempfile::TempDir, Reader) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &[
                    "user_id".into(),
                    "email".into(),
                    "amount".into(),
                    "created_at".into(),
                ],
                &[
                    vec![
                        Some("u1".into()),
                        Some("a@x.com".into()),
                        Some("10".into()),
                        Some("2025-01-01".into()),
                    ],
                    vec![
                        Some("u1".into()),
                        Some("b@x.com".into()),
                        Some("20".into()),
                        Some("2025-01-02".into()),
                    ],
                ],
            )
            .expect("ingest");
        let conn =
            open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read attach");
        let attach = read_only_attach_sql(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        let reader = Reader::start(
            conn,
            attach,
            consumer_engine_execution::ReaderLimits::default(),
        )
        .expect("reader");
        (tmp, reader)
    }

    #[tokio::test]
    async fn test_should_redact_pii_sample_values() {
        let (_tmp, reader) = tmp_reader();
        let profiler = Profiler::new(reader, Arc::new(StubLlm), Arc::new(StubEmbed::default()));
        let rows = profiler.onboard("erp", "orders").await.expect("onboard");
        let email = rows
            .iter()
            .find(|r| r.column_name.as_deref() == Some("email"))
            .expect("email column profiled");
        assert!(email.pii_flag, "email must be flagged PII");
        // PII values must never appear in the stored sample.
        let sample = serde_json::to_string(&email.sample_values).expect("ser");
        assert!(
            !sample.contains('@'),
            "PII email value must be redacted: {sample}"
        );
    }

    #[tokio::test]
    async fn test_should_bound_sample_size() {
        let (_tmp, reader) = tmp_reader();
        let profiler = Profiler::new(reader, Arc::new(StubLlm), Arc::new(StubEmbed::default()))
            .with_sample_value_bytes(3);
        let rows = profiler.onboard("erp", "orders").await.expect("onboard");
        let amount = rows
            .iter()
            .find(|r| r.column_name.as_deref() == Some("amount"))
            .expect("amount column profiled");
        // Each sample value truncated to ≤3 bytes.
        for v in amount.sample_values.as_array().expect("array") {
            assert!(
                v.as_str().is_none_or(|s| s.len() <= 3),
                "sample value must be byte-bounded: {v}"
            );
        }
    }

    #[tokio::test]
    async fn test_should_classify_event_ts_and_identifier() {
        let (_tmp, reader) = tmp_reader();
        let profiler = Profiler::new(reader, Arc::new(StubLlm), Arc::new(StubEmbed::default()));
        let rows = profiler.onboard("erp", "orders").await.expect("onboard");
        let by_name = |n: &str| {
            rows.iter()
                .find(|r| r.column_name.as_deref() == Some(n))
                .expect("column profiled")
                .semantic_type
        };
        assert_eq!(by_name("user_id"), SemanticType::Identifier);
        assert_eq!(by_name("created_at"), SemanticType::EventTs);
        assert_eq!(by_name("amount"), SemanticType::Measure);
        assert_eq!(by_name("email"), SemanticType::Pii);
    }

    #[tokio::test]
    async fn test_should_reject_missing_table() {
        let (_tmp, reader) = tmp_reader();
        let profiler = Profiler::new(reader, Arc::new(StubLlm), Arc::new(StubEmbed::default()));
        // A table that was never onboarded → reader error.
        assert!(profiler.onboard("erp", "ghosts").await.is_err());
    }
}
