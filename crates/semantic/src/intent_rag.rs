//! L1 Intent RAG — at query time, embeds the operator utterance and retrieves a
//! bounded candidate set of tables/columns from `semantic_catalog` (spec 13 §2).
//!
//! Retrieval is bounded (I3): at most `k` hits (default 20) are returned, so the
//! agent never enumerates the whole catalogue. An empty catalogue yields an empty
//! candidate set (the agent must then not invent columns).

use std::sync::Arc;

use consumer_engine_core::{CatalogHit, READ_ONLY_CATALOG_ALIAS, Result, SemanticType};
use consumer_engine_execution::Reader;
use serde_json::Value;

use crate::{limits::DEFAULT_K, llm::EmbeddingModel};

/// The L1 retrieval engine. Cheap to share via `Arc`.
pub struct IntentRag {
    reader: Reader,
    embed: Arc<dyn EmbeddingModel>,
    default_k: usize,
}

impl std::fmt::Debug for IntentRag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntentRag")
            .field("default_k", &self.default_k)
            .finish_non_exhaustive()
    }
}

impl IntentRag {
    /// Build a retriever over `reader` with the M3 stub embedding model.
    #[must_use]
    pub fn new(reader: Reader, embed: Arc<dyn EmbeddingModel>) -> Self {
        Self {
            reader,
            embed,
            default_k: DEFAULT_K,
        }
    }

    /// Override the default retrieval cap (I3). Builder-style.
    #[must_use]
    pub fn with_default_k(mut self, k: usize) -> Self {
        self.default_k = k.max(1);
        self
    }

    /// Retrieve up to `k` candidate catalogue hits for `utterance`, ranked by
    /// cosine similarity of the utterance embedding to each row's description
    /// embedding. `k == 0` uses `default_k`. An empty catalogue returns
    /// an empty vector.
    ///
    /// # Errors
    /// `consumer_engine_core::Error::Execution` on a reader failure.
    pub async fn retrieve(&self, utterance: &str, k: usize) -> Result<Vec<CatalogHit>> {
        let k = if k == 0 { self.default_k } else { k };
        let query = format!(
            "SELECT system, table_name, column_name, semantic_type, description, embedding FROM \
             {READ_ONLY_CATALOG_ALIAS}.semantic_catalog"
        );
        let qr = self.reader.query_with_params(&query, Vec::new()).await?;

        let utterance_emb = self.embed.embed(utterance);
        let mut scored: Vec<(f64, CatalogHit)> = Vec::new();
        for row in &qr.rows {
            let system = cell_string(row.first());
            let table_name = cell_string(row.get(1));
            let column_name = cell_opt_string(row.get(2));
            let semantic_type = row
                .get(3)
                .and_then(Value::as_str)
                .and_then(SemanticType::parse)
                .unwrap_or(SemanticType::Dimension);
            let description = cell_string(row.get(4));
            let row_emb = row.get(5).map(embedding_to_vec).unwrap_or_default();
            let score = cosine(&utterance_emb, &row_emb);
            scored.push((
                score,
                CatalogHit {
                    system,
                    table_name,
                    column_name,
                    semantic_type,
                    description,
                    score,
                },
            ));
        }
        // Highest similarity first; ties broken by table/column name for a
        // deterministic ordering (M3 = deterministic stubs).
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.system.cmp(&b.1.system))
                .then_with(|| a.1.table_name.cmp(&b.1.table_name))
                .then_with(|| a.1.column_name.cmp(&b.1.column_name))
        });
        // I3: bound the candidate set.
        Ok(scored.into_iter().take(k).map(|(_, hit)| hit).collect())
    }
}

/// Extract a string from a `JSON` cell, defaulting to empty for null/non-string.
fn cell_string(v: Option<&Value>) -> String {
    v.and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default()
}

/// Extract an optional string (column names may be absent for table-level rows).
fn cell_opt_string(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str).map(str::to_string)
}

/// Convert a `FLOAT[]` `JSON` cell into a `Vec<f32>`.
fn embedding_to_vec(v: &Value) -> Vec<f32> {
    v.as_array()
        .map(|arr| arr.iter().filter_map(num_to_f32).collect())
        .unwrap_or_default()
}

/// Map a `JSON` number to `f32`.
fn num_to_f32(v: &Value) -> Option<f32> {
    v.as_f64().map(|f| f as f32)
}

/// Cosine similarity; 0.0 if either vector has zero norm.
fn cosine(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let dot: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let na: f64 = a
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    let nb: f64 = b
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

#[cfg(test)]
mod tests {
    use consumer_engine_core::{CatalogRow, SemanticType};
    use consumer_engine_storage::{Writer, open_reader, read_only_attach_sql};

    use super::*;
    use crate::llm::StubEmbed;

    fn tmp_with_catalog() -> (tempfile::TempDir, Reader) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let embed = StubEmbed::default();
        let rows = vec![
            CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some("user_id".into()),
                semantic_type: SemanticType::Identifier,
                data_type: "VARCHAR".into(),
                description: "The user identifier of the orders table".into(),
                pii_flag: false,
                sample_values: Value::Array(vec![]),
                embedding: embed.embed("The user identifier of the orders table"),
            },
            CatalogRow {
                entity_type: "column".into(),
                system: "erp".into(),
                table_name: "orders".into(),
                column_name: Some("amount".into()),
                semantic_type: SemanticType::Measure,
                data_type: "VARCHAR".into(),
                description: "The monetary amount spent on an order".into(),
                pii_flag: false,
                sample_values: Value::Array(vec![]),
                embedding: embed.embed("The monetary amount spent on an order"),
            },
        ];
        writer.write_catalog_rows(&rows).expect("write catalog");
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
    async fn test_should_retrieve_bounded_candidates() {
        let (_tmp, reader) = tmp_with_catalog();
        let rag = IntentRag::new(reader, Arc::new(StubEmbed::default()));
        // I3: retrieval must be bounded by k even when the catalogue is larger.
        let hits = rag
            .retrieve("show me the user id", 1)
            .await
            .expect("retrieve");
        assert_eq!(hits.len(), 1, "k=1 must return at most 1 hit");
        // The returned hit must be a real catalogued column.
        let name = hits[0].column_name.as_deref();
        assert!(
            matches!(name, Some("user_id") | Some("amount")),
            "unexpected hit: {name:?}"
        );
        assert!(
            !hits[0].description.is_empty(),
            "hit must carry a description"
        );

        // k larger than the catalogue returns every catalogued row.
        let hits = rag.retrieve("orders", 10).await.expect("retrieve");
        assert_eq!(hits.len(), 2, "must return all 2 catalogued columns");

        // Determinism: the stub embedding has no RNG, so identical queries rank
        // identically across calls.
        let a = rag.retrieve("orders", 2).await.expect("retrieve");
        let b = rag.retrieve("orders", 2).await.expect("retrieve");
        assert_eq!(a, b, "retrieval must be deterministic");
    }

    #[tokio::test]
    async fn test_should_return_empty_for_empty_catalog() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        writer.ensure_semantic_catalog_table().expect("ensure");
        let conn =
            open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read attach");
        let attach = read_only_attach_sql(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        let reader = Reader::start(
            conn,
            attach,
            consumer_engine_execution::ReaderLimits::default(),
        )
        .expect("reader");
        let rag = IntentRag::new(reader, Arc::new(StubEmbed::default()));
        let hits = rag.retrieve("anything", 5).await.expect("retrieve");
        assert!(hits.is_empty(), "empty catalogue must yield no candidates");
    }
}
