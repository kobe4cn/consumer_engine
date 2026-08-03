# 20-ingestion: Source Adapters, Producer Registry, Materialisation

Status: draft · Depends on: [10](./10-data-model.md), [11](./11-runtime-core.md)

## 1. Purpose

Fills DuckLake. Owns the **`IngestionActor` internals**: Q1 source ingest
(CDC + batch), the **Feature Store producer registry** (D9), snapshot
materialisation (Q2), and DuckLake write-limit handling (MERGE constraints).
This is where the "ML-ready seam" physically lives.

## 2. Interface

```text
pub trait SourceAdapter: Send + 'static {              // CDC or batch, impl per system
    async fn next_batch(&mut self) -> Result<Option<SourceBatch>>;
}
pub trait FeatureProducer: Send + Sync {               // SQL (v1) or ML (phase 2) — same contract
    fn id(&self) -> &str;
    async fn run(&self, as_of: Timestamp) -> Result<ProducerOutput>; // (user_id, feature, value, as_of_ts)
}
pub struct IngestionHandle { tx_q1, tx_q2, tx_q3 }     // cheap clone senders into the actor
```

## 2a. Ingestion & materialisation flow

```text
  SourceAdapter (CDC: Debezium/Kafka consumer | Batch: file/pull)
        │  SourceBatch{ table, rows, op(upsert/delete), cdc_offset }
        ▼
  ┌──────────────────────────────────────────────────────────┐
  │ IngestionActor — Q1                                      │
  │  micro-batcher (rows/age) ──▶ DuckLake MERGE/INSERT      │
  │  on schema-bearing batch: hand to Profiler (13)          │
  │  commit cdc_offset in same catalogue transaction         │
  └──────────────┬───────────────────────────────────────────┘
                 │ after raw_* commit
                 ▼
  FeatureProducer.run(as_of) ──▶ feature_store rows ──▶ Q1 (as data)
                                                  └─▶ refresh feature_wide_<family> pivot

  QueryEngine.materialize ──▶ Q2 ──▶ audience_snapshot (D10/D11)
  POST /suppression ───────▶ Q3 ──▶ suppression
```

## 3. Invariants

- **I1 Single-writer enforcement.** All three queues are serviced by the one
  `IngestionActor`; adapters/producers never hold a write connection
  (AGENTS.md — message passing over shared state).
- **I2 Offset commit is atomic with data.** CDC offset advances **in the same
  catalogue transaction** as the data write, so restart is exactly-once w.r.t.
  the catalogue (at-least-once from the source, deduped by PK MERGE).
- **I3 Producer point-in-time correctness.** A producer's `run(as_of)` may only
  read raw/feature rows with `as_of_ts ≤ as_of` (anti-leakage, D9). The actor
  supplies `as_of` = the source snapshot timestamp, never wall-clock.
- **I4 Materialisation atomicity.** A snapshot is visible only after its full
  row-set + `hit_reason` is committed in one catalogue transaction; partial
  snapshots are never observable (D10/I2 in [10](./10-data-model.md)).

## 4. Behaviour — DuckLake MERGE limits (confirmed by spike)

Confirmed on DuckDB v1.5.4 — [spike-ducklake-merge.md](../docs/research/spike-ducklake-merge.md):

- **DuckLake rejects `PRIMARY KEY`/`UNIQUE` constraints entirely** (not just
  unenforced). Tables carry no constraints ([10](./10-data-model.md)); the
  adapter enforces identity and dedup.
- **MERGE allows exactly one `WHEN MATCHED` clause.**
  - **Upsert** (update-or-insert) = **one** MERGE: `WHEN MATCHED THEN UPDATE
    … WHEN NOT MATCHED THEN INSERT …`.
  - **Update + delete** for matched keys = **two** MERGEs in one catalog
    transaction (parser rejects two `WHEN MATCHED`).
- **No dedup**: duplicate keys accumulate. The adapter MUST dedup a source
  batch by key before MERGE.
- **Event appends** (orders, behaviour): plain `INSERT`; never MERGE.
- **Deletes**: logical `MERGE … WHEN MATCHED THEN DELETE` (delete-file in the
  catalog; Parquet untouched).
- **Throughput**: local INSERT ≈ 37 M rows/s (1 M rows / 0.027 s) — not the
  bottleneck; file count is
  ([spike-microbatch-compaction.md](../docs/research/spike-microbatch-compaction.md)).
- **Open risks**: object-storage per-file latency and Postgres-catalog
  multi-writer contention not benched here (R3/R4 in the spike memo).

## 5. Suppression rules (consumed by Exclude)

`Exclude { suppression.of(campaign) }` ([12](./12-query-engine.md)) is governed
by a small rule engine over the `suppression` table:

- **per-campaign no-repeat** (default on): a user with any `action ∈
  {targeted,delivered}` for `campaign_id` is excluded from that campaign.
- **global frequency cap** (configurable): a user targeted ≥ N times in the
  last D days across campaigns is excluded.
- Rules are config, not code; changes are versioned and audited.

## 6. Cross-references

- ← Depends on: [10](./10-data-model.md), [11](./11-runtime-core.md),
  [13](./13-semantic-layer.md) (Profiler for new tables).
- → Consumed by: [12](./12-query-engine.md) (Q2), [21](./21-rest-api.md) (Q3).
- ↔ Phase 0 spike: MERGE limits → [91](./91-impl-plan.md).
- ↔ ML producers (phase 2): [90](./90-roadmap.md), D8/D9.
- Norms: AGENTS.md § Async (actor, channels), § Safety (PK enforcement at
  adapter boundary, not trusting source).
