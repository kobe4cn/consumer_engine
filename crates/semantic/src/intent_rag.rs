//! L1 Intent RAG — at query time, embeds the operator utterance and retrieves a
//! bounded candidate set of tables/columns from `semantic_catalog` (spec 13 §2).
//!
//! Retrieval is bounded (I3): at most `k` hits (default 20) are returned, so the
//! agent never enumerates the whole catalogue. An empty catalogue yields an empty
//! candidate set (the agent must then not invent columns).

use std::sync::Arc;

use consumer_engine_core::{CatalogHit, Error, READ_ONLY_CATALOG_ALIAS, Result, SemanticType};
use consumer_engine_execution::Reader;
use serde_json::Value;

use crate::{limits::DEFAULT_K, llm::EmbeddingModel};

/// Defensive cap on the number of catalogue rows read per retrieval (spec 13
/// §3 I3: bounded retrieval; AGENTS.md § Resource Limits: bound every
/// collection). Far larger than any M3 catalogue, so top-k is unaffected.
const MAX_CATALOG_SCAN: usize = 10_000;

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
    /// Fetch the NEWEST catalogue entry for one column (issue #23: description
    /// editing preserves the original row's semantics and re-stamps only the
    /// description + embedding + version).
    ///
    /// # Errors
    /// `consumer_engine_core::Error::Execution` on a reader failure.
    pub async fn catalogue_entry(
        &self,
        system: &str,
        entity: &str,
        column: &str,
        tenant: &str,
    ) -> Result<Option<consumer_engine_core::CatalogRow>> {
        let sql = format!(
            "SELECT entity_type, system, table_name, column_name, semantic_type, data_type, \
             description, pii_flag, sample_values, embedding, source_epoch FROM \
             {READ_ONLY_CATALOG_ALIAS}.semantic_catalog WHERE system = ? AND table_name = ? AND \
             column_name = ? AND tenant_id = ? ORDER BY source_epoch DESC LIMIT 1"
        );
        let qr = self
            .reader
            .query_with_params(
                &sql,
                vec![
                    duckdb::types::Value::Text(system.to_string()),
                    duckdb::types::Value::Text(entity.to_string()),
                    duckdb::types::Value::Text(column.to_string()),
                    duckdb::types::Value::Text(tenant.to_string()),
                ],
            )
            .await?;
        let row = match qr.rows.first() {
            Some(r) => r,
            None => return Ok(None),
        };
        // The stored semantic_type is engine-written (validated); an unparseable
        // value is catalogue corruption — surface it rather than silently
        // rewriting the row's type on an edit (issue #23).
        let semantic_type = row
            .get(4)
            .and_then(Value::as_str)
            .and_then(SemanticType::parse)
            .ok_or_else(|| {
                Error::InvalidInput("catalogue row has an unknown semantic_type".into())
            })?;
        Ok(Some(consumer_engine_core::CatalogRow {
            entity_type: cell_opt_string(row.first()).unwrap_or_else(|| "column".to_string()),
            system: cell_string(row.get(1)),
            table_name: cell_string(row.get(2)),
            column_name: cell_opt_string(row.get(3)),
            semantic_type,
            data_type: cell_string(row.get(5)),
            description: cell_string(row.get(6)),
            pii_flag: row.get(7).and_then(Value::as_bool).unwrap_or(false),
            sample_values: row.get(8).cloned().unwrap_or(Value::Array(Vec::new())),
            embedding: row.get(9).map(embedding_to_vec).unwrap_or_default(),
            source_epoch: row.get(10).and_then(Value::as_i64).unwrap_or(0),
        }))
    }

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
    pub async fn retrieve(
        &self,
        utterance: &str,
        k: usize,
        tenant: &str,
    ) -> Result<Vec<CatalogHit>> {
        let k = if k == 0 { self.default_k } else { k };
        // The read is bounded with a defensive row cap (AGENTS.md § Resource
        // Limits: bound every collection). M3 catalogues are far smaller than
        // this, so top-k over the full catalogue is unaffected.
        // Only the NEWEST catalogue row per (system, table, column) is
        // retrievable (issue #18 / spec 13 I5): a re-onboard appends a versioned
        // delta, and the newest `source_epoch` wins — stale descriptions never
        // rank against the fresh ones. The catalogue is scoped to the caller's
        // tenant (issue #22): a tenant only retrieves its own rows.
        let query = format!(
            "SELECT system, table_name, column_name, semantic_type, description, embedding FROM \
             {READ_ONLY_CATALOG_ALIAS}.semantic_catalog WHERE tenant_id = ? QUALIFY row_number() \
             OVER (PARTITION BY system, table_name, column_name ORDER BY source_epoch DESC) = 1 \
             LIMIT {MAX_CATALOG_SCAN}"
        );
        let qr = self
            .reader
            .query_with_params(&query, vec![duckdb::types::Value::Text(tenant.to_string())])
            .await?;

        // A failed utterance embedding means retrieval cannot be trusted —
        // surface `CatalogueUnavailable` so the agent never guesses columns
        // (spec 13 §4).
        let utterance_emb = self
            .embed
            .embed(utterance)
            .await
            .map_err(|_| Error::CatalogueUnavailable)?;
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

    async fn tmp_with_catalog() -> (tempfile::TempDir, Reader) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        // Embeddings are deterministic with the stub.
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
                embedding: embed
                    .embed("The user identifier of the orders table")
                    .await
                    .expect("embed"),
                source_epoch: 0,
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
                embedding: embed
                    .embed("The monetary amount spent on an order")
                    .await
                    .expect("embed"),
                source_epoch: 0,
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
        let (_tmp, reader) = tmp_with_catalog().await;
        let rag = IntentRag::new(reader, Arc::new(StubEmbed::default()));
        // I3: retrieval must be bounded by k even when the catalogue is larger.
        let hits = rag
            .retrieve("show me the user id", 1, "default")
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
        let hits = rag
            .retrieve("orders", 10, "default")
            .await
            .expect("retrieve");
        assert_eq!(hits.len(), 2, "must return all 2 catalogued columns");

        // Determinism: the stub embedding has no RNG, so identical queries rank
        // identically across calls.
        let a = rag
            .retrieve("orders", 2, "default")
            .await
            .expect("retrieve");
        let b = rag
            .retrieve("orders", 2, "default")
            .await
            .expect("retrieve");
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
        let hits = rag
            .retrieve("anything", 5, "default")
            .await
            .expect("retrieve");
        assert!(hits.is_empty(), "empty catalogue must yield no candidates");
    }
}

#[cfg(test)]
mod version_tests {
    use consumer_engine_core::{CatalogRow, SemanticType};
    use consumer_engine_storage::{Writer, open_reader, read_only_attach_sql};

    use super::*;

    #[tokio::test]
    async fn test_should_retrieve_newest_catalogue_version() {
        // Issue #23: an edited description (newer source_epoch) supersedes the
        // original in retrieval — QUALIFY must pick the newest row per column.
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        let mk = |epoch: i64, desc: &str| CatalogRow {
            entity_type: "column".into(),
            system: "erp".into(),
            table_name: "orders".into(),
            column_name: Some("user_id".into()),
            semantic_type: SemanticType::Identifier,
            data_type: "VARCHAR".into(),
            description: desc.into(),
            pii_flag: false,
            sample_values: serde_json::json!([]),
            embedding: vec![0.0; 4],
            source_epoch: epoch,
        };
        writer
            .write_catalog_rows(&[mk(1, "original description")])
            .expect("v1");
        writer
            .write_catalog_rows(&[mk(2, "edited marker zebra")])
            .expect("v2");
        let conn = open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read");
        let attach = read_only_attach_sql(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        let reader = Reader::start(
            conn,
            attach,
            consumer_engine_execution::ReaderLimits::default(),
        )
        .expect("reader");
        let rag = IntentRag::new(reader, Arc::new(crate::llm::StubEmbed::default()));
        let hits = rag.retrieve("zebra", 5, "default").await.expect("retrieve");
        let user = hits
            .iter()
            .find(|h| h.column_name.as_deref() == Some("user_id"))
            .expect("user_id hit");
        assert_eq!(
            user.description, "edited marker zebra",
            "retrieval must pick the NEWEST catalogue version: {hits:?}"
        );
    }
}
