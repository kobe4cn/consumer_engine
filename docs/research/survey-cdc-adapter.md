# Survey: CDC adapter choice for source ingestion

Status: Done · Owner: platform · Date: 2025-08-03 (sources current as of date) · Kind: **survey**

No vendoring — this is ecosystem-state research. Sources: DuckDB/Debezium docs
+ 2025–2026 industry write-ups (Tavily, 2025-08-03). Surveys age fast; re-check
crate versions before implementation.

## Question

[20-ingestion.md](../../specs/20-ingestion.md) and [D5](../../specs/99-key-decisions.md)
require the engine to be **CDC-capable** but fall back to **batch** when a
source cannot emit changes. What is the concrete CDC stack, and how realistic
is "many sources are batch-only"?

## Findings

1. ✅ **Debezium + Kafka is the de-facto CDC transport.** Debezium reads
   database transaction logs (Postgres WAL, MySQL binlog, etc.) and emits
   change envelopes to Kafka with minimal source impact. This is the standard
   the industry has converged on (Conduktor, Debezium 2026 posts).
2. ✅ **Rust consumes Kafka fine.** `rdkafka` (librdkafka bindings) is the
   mature Rust Kafka client; a consumer parses Debezium envelopes (JSON/Avro)
   into our `SourceBatch{ table, rows, op, cdc_offset }` ([20 §2](../../specs/20-ingestion.md)).
3. ⚠ **Many enterprise source systems do NOT expose CDC.** Legacy ERPs,
   spreadsheets, third-party SaaS exports, and DBs without log access can only
   provide **periodic bulk dumps**. [D5](../../specs/99-key-decisions.md)'s
   "batch fallback" is realistic, not defensive — expect a **mix** per source.
4. The freshness SLA ([71 §2](../../specs/71-performance-budgets.md)) is therefore
   **per-source**: CDC sources hit ≤ 5 min; batch sources are bounded by their
   dump cadence and must surface `freshness.lagHours`.

## Decision

**GO.** The `SourceAdapter` trait ([20 §2](../../specs/20-ingestion.md)) stays
abstract; two concrete impls:

- **`CdcKafkaAdapter`** (feature `ingestion-cdc`, default **off** until a real
  CDC source exists): `rdkafka` consumer → Debezium envelope parser →
  `SourceBatch`. Offset committed atomically with the DuckLake write ([20 I2](../../specs/20-ingestion.md)).
- **`BatchAdapter`** (default): file/pull ingest → `SourceBatch` with
  `op = upsert`/`delete` derived from the dump's diff vs prior snapshot.

Freshness grading (D5) is implemented by the adapter tagging each batch with
its source type; the query layer reports the worst-case lag.

## Risks identified

- **R1**: Debezium envelope schema (Avro vs JSON, `op` field semantics) drift
  across Debezium versions — pin the envelope format the parser expects; add a
  parser regression test per supported source connector.
- **R2 (open)**: exactly-once vs at-least-once — Kafka is at-least-once; we
  dedup by PK via MERGE ([spike-ducklake-merge.md](./spike-ducklake-merge.md)
  finding 3), so the net is effectively-once w.r.t. the catalog. Document this
  explicitly in [20](../../specs/20-ingestion.md).
- **R3 (open)**: schema drift in a batch dump (new column) must route through
  the L0 Profiler onboarding ([13](../../specs/13-semantic-layer.md)), not be
  silently inserted — the batch adapter must detect schema change and refuse +
  re-onboard.
