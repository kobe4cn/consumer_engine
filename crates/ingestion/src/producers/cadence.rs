//! The `cadence.regularity` SQL producer with point-in-time correctness (I3).
//!
//! Reads a raw events table `dro.raw_{system}_{entity}` bounded by `as_of`
//! (`WHERE ts <= ?`, enforced at SQL level — VARCHAR ISO-8601 strings compare
//! lexicographically = chronologically), then computes a per-user *cadence
//! regularity* score in Rust: `regularity = max(0, 1 − cv)` where `cv` is the
//! coefficient of variation of the inter-event intervals. Regular buyers (even
//! spacing) score high; erratic buyers score low.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use consumer_engine_core::{Dataset, FeatureRow, READ_ONLY_CATALOG_ALIAS, Result};
use consumer_engine_execution::Reader;
use duckdb::types::Value;

use crate::producer::{FeatureProducer, ProducerOutput};

/// A defensive cap on rows scanned per producer run (spec 20 I3 + AGENTS.md §
/// Resource Limits: bound every read crossing the storage boundary).
const SCAN_ROW_CAP: usize = 1_000_000;

/// The feature name emitted by this producer.
pub const FEATURE_NAME: &str = "cadence.regularity";

/// Computes per-user purchase cadence regularity over a raw events table.
pub struct CadenceRegularityProducer {
    reader: Reader,
    orders: Dataset,
    producer_id: String,
}

impl std::fmt::Debug for CadenceRegularityProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CadenceRegularityProducer")
            .field("orders", &self.orders)
            .field("producer_id", &self.producer_id)
            .finish_non_exhaustive()
    }
}

impl CadenceRegularityProducer {
    /// Build a producer that reads `orders` (`raw_{system}_{entity}`) via
    /// `reader` and registers under the id `"cadence_sql"`.
    #[must_use]
    pub fn new(reader: Reader, orders: Dataset) -> Self {
        Self {
            reader,
            orders,
            producer_id: "cadence_sql".to_string(),
        }
    }

    /// Override the producer id (e.g. for test isolation). Builder-style.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.producer_id = id.into();
        self
    }
}

#[async_trait]
impl FeatureProducer for CadenceRegularityProducer {
    fn id(&self) -> &str {
        &self.producer_id
    }

    async fn run(&self, as_of: &str) -> Result<ProducerOutput> {
        consumer_engine_core::validate_ident(&self.orders.system)?;
        consumer_engine_core::validate_ident(&self.orders.entity)?;
        // Fetch one more row than the cap so an over-cap source is detected
        // (fail loudly) rather than silently truncated — silent truncation would
        // corrupt the cadence scores of users past the cap.
        let sql = format!(
            "SELECT user_id, ts FROM {READ_ONLY_CATALOG_ALIAS}.raw_{}_{} WHERE ts <= ? LIMIT {}",
            self.orders.system,
            self.orders.entity,
            SCAN_ROW_CAP + 1,
        );
        let qr = self
            .reader
            .query_with_params(&sql, vec![Value::Text(as_of.to_string())])
            .await?;
        if qr.rows.len() > SCAN_ROW_CAP {
            return Err(consumer_engine_core::Error::InvalidInput(format!(
                "{}.{} has >{SCAN_ROW_CAP} matching rows at as_of {as_of}; raise the producer \
                 scan cap or narrow as_of",
                self.orders.system, self.orders.entity,
            )));
        }

        // Group valid timestamps (epoch seconds) by user.
        let mut by_user: HashMap<String, Vec<i64>> = HashMap::new();
        for row in &qr.rows {
            let (Some(user), Some(ts)) = (
                row.first().and_then(serde_json::Value::as_str),
                row.get(1).and_then(serde_json::Value::as_str),
            ) else {
                continue;
            };
            if let Some(epoch) = parse_epoch(ts) {
                by_user.entry(user.to_string()).or_default().push(epoch);
            }
        }

        let mut rows = Vec::with_capacity(by_user.len());
        for (user, mut times) in by_user {
            times.sort_unstable();
            let regularity = regularity(&times);
            rows.push(FeatureRow {
                user_id: user,
                feature_name: FEATURE_NAME.into(),
                num_value: regularity,
                as_of_ts: as_of.to_string(),
                producer_id: self.producer_id.clone(),
            });
        }
        Ok(ProducerOutput { rows })
    }
}

/// The cadence regularity score for a sorted list of event epochs.
///
/// - `< 2` events → `0.0` (no cadence to measure).
/// - zero mean interval (all events simultaneous) → `0.0` (degenerate, avoids division by zero).
/// - otherwise `max(0, 1 − cv)` where `cv = stddev_pop(intervals) / mean`.
#[must_use]
pub fn regularity(times: &[i64]) -> f64 {
    if times.len() < 2 {
        return 0.0;
    }
    let intervals: Vec<f64> = times.windows(2).map(|w| (w[1] - w[0]) as f64).collect();
    let n = intervals.len() as f64;
    let mean = intervals.iter().sum::<f64>() / n;
    if mean == 0.0 {
        return 0.0;
    }
    let variance = intervals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    let stddev = variance.sqrt();
    let cv = stddev / mean;
    (1.0 - cv).max(0.0)
}

/// Parse a timestamp string to epoch seconds. Tries RFC-3339, then
/// `YYYY-MM-DDTHH:MM:SS`, then `YYYY-MM-DD` (midnight UTC). Returns `None` for
/// an unparseable value (the caller skips it rather than panicking).
fn parse_epoch(ts: &str) -> Option<i64> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
        return Some(dt.timestamp());
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S") {
        return Some(ndt.and_utc().timestamp());
    }
    if let Ok(nd) = NaiveDate::parse_from_str(ts, "%Y-%m-%d") {
        return Some(nd.and_hms_opt(0, 0, 0)?.and_utc().timestamp());
    }
    None
}

#[cfg(test)]
mod tests {
    use consumer_engine_core::Dataset;
    use consumer_engine_storage::{Writer, open_reader, read_only_attach_sql};

    use super::*;
    use crate::producer::FeatureProducer;

    fn tmp_reader_with_orders() -> (tempfile::TempDir, Reader) {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        writer
            .ingest_raw(
                "erp",
                "orders",
                &["user_id".into(), "ts".into()],
                &[
                    // Regular buyer: weekly.
                    vec![Some("reg".into()), Some("2025-01-01T00:00:00Z".into())],
                    vec![Some("reg".into()), Some("2025-01-08T00:00:00Z".into())],
                    vec![Some("reg".into()), Some("2025-01-15T00:00:00Z".into())],
                    // Erratic buyer: one gap is huge, one is tiny.
                    vec![Some("err".into()), Some("2025-01-01T00:00:00Z".into())],
                    vec![Some("err".into()), Some("2025-06-01T00:00:00Z".into())],
                    vec![Some("err".into()), Some("2025-06-02T00:00:00Z".into())],
                    // Future-only buyer: only event is after T2.
                    vec![Some("fut".into()), Some("2025-07-01T00:00:00Z".into())],
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
    async fn test_should_run_producer_point_in_time_bounded() {
        let (_tmp, reader) = tmp_reader_with_orders();
        let producer = CadenceRegularityProducer::new(
            reader,
            Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
        );
        // as_of = T2 (2025-06-02): the future-only buyer (T3 = 2025-07-01) is
        // excluded by the SQL bound → absent from the output (proves I3).
        let out = producer.run("2025-06-02T00:00:00Z").await.expect("run");
        assert!(
            out.rows.iter().all(|r| r.user_id != "fut"),
            "a user whose only event is after as_of must be absent (I3): {:?}",
            out.rows
        );
        assert!(
            out.rows.iter().any(|r| r.user_id == "reg"),
            "regular buyer must be present"
        );
    }

    #[tokio::test]
    async fn test_should_score_regular_buyers_high() {
        let (_tmp, reader) = tmp_reader_with_orders();
        let producer = CadenceRegularityProducer::new(
            reader,
            Dataset {
                system: "erp".into(),
                entity: "orders".into(),
            },
        );
        let out = producer.run("2025-12-31T00:00:00Z").await.expect("run");
        let score = |u: &str| {
            out.rows
                .iter()
                .find(|r| r.user_id == u)
                .map(|r| r.num_value)
                .unwrap_or(-1.0)
        };
        assert!(
            score("reg") > 0.7,
            "regular buyer must score high: {}",
            score("reg")
        );
        assert!(
            score("err") < 0.3,
            "erratic buyer must score low: {}",
            score("err")
        );
    }

    #[test]
    fn test_should_regularity_for_single_event_is_zero() {
        assert_eq!(regularity(&[100]), 0.0);
        assert_eq!(regularity(&[]), 0.0);
    }
}
