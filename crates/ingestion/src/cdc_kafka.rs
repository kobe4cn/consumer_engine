//! Kafka/Debezium CDC adapter (feature `ingestion-cdc`).
//!
//! Consumes Debezium change envelopes (JSON) from a Kafka topic and yields
//! [`SourceBatch`]es whose `cdc_offset` is the consumed message's Kafka offset.
//! Offsets are committed by the engine in the same catalog transaction as the
//! data (specs/20 I2), so `auto.commit` is disabled and restart resumes by
//! seeking to the stored offset (at-least-once from Kafka; the writer's
//! per-key MERGE dedup makes the catalog effectively-once).

use std::time::Duration;

use async_trait::async_trait;
use consumer_engine_core::{Error, Result};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use serde::Deserialize;

use crate::cdc::{SourceAdapter, SourceBatch, source_key};

/// How long `next_batch` waits for a message before reporting "nothing new"
/// (the pump then sleeps its own poll interval).
const RECV_TIMEOUT: Duration = Duration::from_millis(500);
/// Max Debezium messages accumulated into one SourceBatch (71 §4 batching).
const MAX_BATCH_MESSAGES: usize = 500;

/// The Debezium change envelope (JSON). Both the top-level and the
/// `{"payload": {...}}` nesting are accepted (Debezium version drift — the
/// survey's R1).
#[derive(Debug, Deserialize)]
struct DebeziumEnvelope {
    /// The change op: `c`/`u` (upsert `after`), `d` (delete `before`).
    op: Option<String>,
    /// The row after the change (upserts).
    after: Option<serde_json::Value>,
    /// The row before the change (deletes).
    before: Option<serde_json::Value>,
    /// Nested payload form (`{"payload": {...}}`).
    #[serde(rename = "payload")]
    payload: Option<Box<DebeziumEnvelope>>,
}

/// A Kafka/Debezium source adapter. `columns` are the raw table columns;
/// `key` is the merge key (must be one of `columns`).
pub struct KafkaCdcAdapter {
    consumer: StreamConsumer,
    source: String,
    mapper: DebeziumMapper,
}

impl std::fmt::Debug for KafkaCdcAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaCdcAdapter")
            .field("source", &self.source)
            .field("mapper", &self.mapper)
            .finish_non_exhaustive()
    }
}

impl KafkaCdcAdapter {
    /// Build a consumer for `topic` mapped to `system`.`entity`, with
    /// `columns`/`key` describing the raw table. `auto.commit` is disabled —
    /// the engine's catalog transaction owns offset durability (I2).
    ///
    /// # Errors
    /// [`Error::Ingestion`] if the Kafka client cannot be created or the topic
    /// cannot be subscribed.
    pub fn new(
        brokers: &str,
        group_id: &str,
        topic: &str,
        system: &str,
        entity: &str,
        columns: Vec<String>,
        key: &str,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .create()
            .map_err(|e| Error::Ingestion(Box::from(format!("kafka client: {e}"))))?;
        consumer
            .subscribe(&[topic])
            .map_err(|e| Error::Ingestion(Box::from(format!("kafka subscribe: {e}"))))?;
        Ok(Self {
            consumer,
            source: source_key(system, entity),
            mapper: DebeziumMapper::new(system, entity, columns, key),
        })
    }
}

/// Maps Debezium change envelopes to [`SourceBatch`]es for one source
/// (system/entity/columns/key). Bundles the mapping context so the adapter and
/// tests share one shape without a live Kafka consumer.
#[derive(Debug, Clone)]
pub struct DebeziumMapper {
    system: String,
    entity: String,
    columns: Vec<String>,
    key: String,
}

impl DebeziumMapper {
    /// A mapper for one raw table.
    #[must_use]
    pub fn new(system: &str, entity: &str, columns: Vec<String>, key: &str) -> Self {
        Self {
            system: system.into(),
            entity: entity.into(),
            columns,
            key: key.into(),
        }
    }

    /// Map a Debezium envelope + Kafka offset to a [`SourceBatch`] for this
    /// mapping. Rows are strings; missing columns map to `None`.
    fn map(&self, env: &DebeziumEnvelope, partition: i32, offset: i64) -> SourceBatch {
        let env = env.payload.as_deref().unwrap_or(env);
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        match env.op.as_deref() {
            // `r` = Debezium snapshot record — same shape as a create/update.
            Some("c") | Some("u") | Some("r") | None => {
                if let Some(after) = &env.after {
                    upserts.push(self.row_from_json(after));
                }
            }
            Some("d") => {
                if let Some(before) = &env.before
                    && let Some(k) = before.get(&self.key).map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                {
                    deletes.push(k);
                }
            }
            Some(other) => {
                tracing::warn!(op = other, source = %source_key(&self.system, &self.entity), "unknown Debezium op; skipped");
            }
        }
        SourceBatch {
            system: self.system.clone(),
            entity: self.entity.clone(),
            columns: self.columns.clone(),
            key: self.key.clone(),
            upserts,
            deletes,
            offsets: vec![(partition, offset)],
        }
    }

    /// Extract one row (aligned with `columns`) from a Debezium `after`/`before`
    /// JSON object; missing/null values map to `None`.
    fn row_from_json(&self, obj: &serde_json::Value) -> Vec<Option<String>> {
        self.columns
            .iter()
            .map(|c| {
                obj.get(c).and_then(|v| match v {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                })
            })
            .collect()
    }
}

#[async_trait]
impl SourceAdapter for KafkaCdcAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    async fn resume(&mut self, offsets: &[(i32, i64)]) -> Result<()> {
        // The group assignment is populated asynchronously after subscribe —
        // wait for it so the seek is not a silent no-op (which would replay
        // the whole topic via auto.offset.reset=earliest).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut assignment = self
            .consumer
            .assignment()
            .map_err(|e| Error::Ingestion(Box::from(format!("kafka assignment: {e}"))))?;
        while assignment.elements().is_empty() && std::time::Instant::now() < deadline {
            // `recv` drives the StreamConsumer's event loop (group join).
            let _ =
                tokio::time::timeout(std::time::Duration::from_millis(100), self.consumer.recv())
                    .await;
            assignment = self
                .consumer
                .assignment()
                .map_err(|e| Error::Ingestion(Box::from(format!("kafka assignment: {e}"))))?;
        }
        for tp in assignment.elements() {
            if let Some((_, offset)) = offsets
                .iter()
                .find(|(partition, _)| *partition == tp.partition())
            {
                self.consumer
                    .seek(
                        tp.topic(),
                        tp.partition(),
                        rdkafka::Offset::Offset(*offset),
                        rdkafka::util::Timeout::After(std::time::Duration::from_secs(5)),
                    )
                    .map_err(|e| Error::Ingestion(Box::from(format!("kafka seek: {e}"))))?;
            }
        }
        Ok(())
    }

    async fn next_batch(&mut self) -> Result<Option<SourceBatch>> {
        // Accumulate up to MAX_BATCH_MESSAGES into one batch (71 §4: the flush
        // is batched, not one transaction per Kafka message), skipping
        // tombstones (Debezium's null-payload delete markers) and unparseable
        // messages with a warning — a bad message must never kill the pump.
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        let mut offsets: std::collections::BTreeMap<i32, i64> = std::collections::BTreeMap::new();
        let deadline = std::time::Instant::now() + RECV_TIMEOUT;
        loop {
            if upserts.len() + deletes.len() >= MAX_BATCH_MESSAGES {
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match tokio::time::timeout(remaining, self.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let partition = msg.partition();
                    let offset = msg.offset();
                    offsets.insert(partition, offset);
                    let Some(payload) = msg.payload() else {
                        // Tombstone (delete marker): nothing to parse; the
                        // offset still advances so we never re-read it.
                        continue;
                    };
                    let env: DebeziumEnvelope = match serde_json::from_slice(payload) {
                        Ok(env) => env,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                source = %self.source,
                                partition,
                                offset,
                                "unparseable Debezium message skipped"
                            );
                            continue;
                        }
                    };
                    let batch = self.mapper.map(&env, partition, offset);
                    upserts.extend(batch.upserts);
                    deletes.extend(batch.deletes);
                }
                Ok(Err(_e)) => break, // transient poll error: yield what we have
                Err(_) => break,      // timeout: nothing new — yield what we have
            }
        }
        if upserts.is_empty() && deletes.is_empty() && offsets.is_empty() {
            return Ok(None);
        }
        Ok(Some(SourceBatch {
            system: self.mapper.system.clone(),
            entity: self.mapper.entity.clone(),
            columns: self.mapper.columns.clone(),
            key: self.mapper.key.clone(),
            upserts,
            deletes,
            offsets: offsets.into_iter().collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_should_consume_from_kafka_mock_cluster() {
        // Kafka e2e without a real broker: rdkafka's in-process MockCluster.
        use rdkafka::{
            mocking::MockCluster,
            producer::{FutureProducer, FutureRecord},
        };

        const TOPIC: &str = "erp.orders.cdc";
        let cluster = MockCluster::new(1).expect("mock cluster");
        cluster.create_topic(TOPIC, 1, 1).expect("create topic");
        let producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .set("message.timeout.ms", "5000")
            .create()
            .expect("producer");
        let body = r#"{"payload": {"op": "c", "after": {"user_id": "u1", "sku": "A"}}}"#;
        producer
            .send_result(FutureRecord::to(TOPIC).payload(body).key("k"))
            .expect("produce");

        let brokers = cluster.bootstrap_servers();
        let mut adapter = KafkaCdcAdapter::new(
            &brokers,
            "ce-test-group",
            TOPIC,
            "erp",
            "orders",
            vec!["user_id".into(), "sku".into()],
            "user_id",
        )
        .expect("adapter");
        // The consumer joins the group asynchronously; poll until the message
        // arrives (bounded).
        let mut batch = None;
        for _ in 0..50 {
            if let Some(b) = adapter.next_batch().await.expect("next") {
                batch = Some(b);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let batch = batch.expect("consumed a batch from the mock cluster");
        assert_eq!(
            batch.upserts,
            vec![vec![Some("u1".into()), Some("A".into())]]
        );
        assert!(batch.deletes.is_empty());
        assert_eq!(batch.offsets, vec![(0, 0)]);
    }

    #[test]
    fn test_should_map_debezium_envelopes_to_batches() {
        // Pure mapping (no broker): the envelope → batch logic.
        let mapper = DebeziumMapper::new(
            "erp",
            "orders",
            vec!["user_id".into(), "sku".into()],
            "user_id",
        );
        let env: DebeziumEnvelope = serde_json::from_str(
            r#"{"payload": {"op": "c", "after": {"user_id": "u1", "sku": "A"}}}"#,
        )
        .expect("parse");
        let batch = mapper.map(&env, 0, 42);
        assert_eq!(
            batch.upserts,
            vec![vec![Some("u1".into()), Some("A".into())]]
        );
        assert!(batch.deletes.is_empty());
        assert_eq!(batch.offsets, vec![(0, 42)]);

        let env: DebeziumEnvelope =
            serde_json::from_str(r#"{"op": "d", "before": {"user_id": "u9", "sku": "Z"}}"#)
                .expect("parse");
        let batch = mapper.map(&env, 0, 43);
        assert!(batch.upserts.is_empty());
        assert_eq!(batch.deletes, vec!["u9".to_string()]);
    }
}
