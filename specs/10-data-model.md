# 10-data-model: DuckLake Tables, DSL AST, REST Wire Shapes

Status: draft · Depends on: [00](./00-prd.md)

## 1. Purpose

The single source of truth for the shapes every other component sees: the
DuckLake tables, the DSL AST the agent composes, and the REST wire envelopes.
Lock this early — drift here cascades into every crate. Naming conventions are
binding ([00 §7](./00-prd.md#7-naming-conventions-binding)).

## 2. DuckLake tables

Catalogue in Postgres; data as Parquet on object storage. All engine-owned
tables use `#[serde(rename_all = "camelCase")]` on their Rust DTOs and
TIMESTAMPTZ for all instants.

> **No `PRIMARY KEY`/`UNIQUE` constraints on any DuckLake table.** DuckLake
> rejects them outright (`Not implemented Error`, see
> [spike-ducklake-merge.md](../docs/research/spike-ducklake-merge.md) finding 1).
> Identity/dedup is enforced by `IngestionActor` via `MERGE ON(key)`, never by
> the lake. Tables below therefore show logical keys as comments only.

```text
raw_<system>_<entity>          -- mirrored source tables; schemas discovered by L0 Profiler
  (varies)                     -- e.g. raw_erp_users, raw_erp_order_lines
                               -- append-only for events; MERGE-upsert for dims (see 20 §4)

feature_store                  -- the ML-ready seam (D9). SCALAR features only (EAV form);
                               -- pivot views expose wide form for cheap scans.
  user_id        TEXT          -- pseudonymous (D12)
  feature_name   TEXT          -- namespaced: "rfm.monetary_12m", "cadence.regularity"
  num_value      DOUBLE        -- scalar feature value
  as_of_ts       TIMESTAMPTZ   -- point-in-time the value is correct-for (anti-leakage)
  producer_id    TEXT          -- which producer wrote it (lineage)
  -- logical key (user_id, feature_name, as_of_ts) enforced by MERGE ON(...);
  -- append-only by as_of_ts — a newer as_of_ts supersedes, never overwrites.

feature_vec_<name>            -- ONE table PER vector feature (e.g. feature_vec_user2vec).
                               -- Separate table because HNSW needs a fixed FLOAT[dim] column
                               -- (spike-duckdb-vss.md finding 1); the EAV store cannot hold it.
  user_id        TEXT          -- pseudonymous
  emb            FLOAT[<dim>]  -- FIXED-size ARRAY; <dim> recorded in semantic_catalog.
  as_of_ts       TIMESTAMPTZ
  producer_id    TEXT
  -- HNSW index created offline by the producer (build ≈ 2s/50k×128, NOT at query time).
  -- CREATE INDEX hnsw_<name> ON feature_vec_<name> USING HNSW (emb) WITH (metric='cosine');

audience_snapshot              -- always materialised (D10); versioned/time-travel via DuckLake
  snapshot_id    UUID          -- UUIDv7 (time-ordered)
  campaign_id    TEXT          -- caller-supplied opaque
  as_of_ts       TIMESTAMPTZ   -- data cut-off reflected (NOT write wall-clock)
  user_id        TEXT
  features       JSON          -- frozen feature values at selection time (D11)
  hit_reason     JSON          -- which DSL predicate selected this user (D11)
  -- logical key (snapshot_id, user_id) enforced by the materialise write path.

suppression                    -- closed loop (E1); written by delivery system, read by Exclude
  suppression_id UUID
  campaign_id    TEXT
  user_id        TEXT
  channel        TEXT          -- sms / email / push / ads
  action         TEXT          -- targeted / delivered / converted / opted_out / bounced
  occurred_ts    TIMESTAMPTZ
  received_ts    TIMESTAMPTZ   -- when engine ingested the writeback (lag audit)
  -- logical key suppression_id; delivery system supplies it for idempotency.

semantic_catalog               -- built by L0 Profiler at onboarding (D4); read by L1 RAG
  entity_type    TEXT          -- table | column
  system         TEXT
  table_name     TEXT
  column_name    TEXT          -- NULL for entity_type=table
  semantic_type  TEXT          -- identifier | dimension | measure | event_ts | pii | fk
  data_type      TEXT          -- DuckDB logical type
  description    TEXT          -- LLM-generated, human-editable
  pii_flag       BOOLEAN
  sample_values  JSON          -- bounded sample (≤20 values, each ≤64 bytes)
  embedding      FLOAT[<dim>]  -- FIXED-size ARRAY; HNSW requires FLOAT[N] (spike-duckdb-vss).
```

Wide feature views: the query layer never scans the long table for a feature
predicate; `IngestionActor` maintains `feature_wide_<family>` pivot views
(DuckDB `PIVOT`) so `Feature: {cadence.regularity > 0.7}` compiles to a cheap
scan over a narrow wide view, not an EAV self-join.

## 3. DSL AST (the agent composes this; never raw SQL on the happy path)

```text
SegmentQuery := { source: Dataset, ops: [Op] }
Op :=
  | Filter   { predicate: Predicate }            -- B
  | Temporal { kind: Lapsed|Recency, .. }        -- B
  | SetOp    { kind: Intersect|Union|Minus, other: SegmentRef }  -- B
  | Distinct { key: "user_id" }
  | Feature  { name, op, value }                 -- F  (precomputed)
  | Derive   { name, expr, over: SegmentRef }    -- J  (survivor-scoped, guarded)
  | Similar  { anchor, axis, top_k }             -- S  (phase 2)
  | Exclude  { suppression: SuppressionRef }     -- B over suppression table
  | Characterize { baseline: BaselineRef }       -- P  (terminal; emits profile not rows)
```

This AST is the load-bearing contract between agent and engine. Every node maps
to exactly one capability code ([80](./80-glossary.md)); the compiler rejects
mixes it cannot guard. See [12](./12-query-engine.md).

## 4. REST wire envelopes (see [21](./21-rest-api.md) for full surface)

All JSON is `camelCase`. Every result carries a `freshness` label (D5).

```jsonc
// Sync query response (small result)
{ "rows": [ {"userId": "u_7", /*...*/} ],
  "count": 48213,
  "freshness": { "worstSource": "batch", "lagSeconds": 6 },
  "queryId": "q_..." }

// Async job
POST /jobs { "dsl": {..}, "materialize": { "campaignId": "c1" } }
→ 202 { "jobId": "j_..", "snapshotId?" }
GET  /jobs/j_.. → { "status": "running|done|failed", "snapshotId", "error?" }

// Snapshot export (big result, never streamed inline — D10/D13)
GET /audience/snap_..           → metadata + presigned URL (Parquet)
GET /audience/snap_../export?format=parquet → binary

// Suppression writeback (external delivery system)
POST /suppression { "campaignId","userId","channel","action","occurredTs" }
→ 201 { "suppressionId" }
```

## 5. Invariants

- **I1** No engine table stores raw PII; `user_id` is pseudonymous everywhere
  (D12). Enforced by the Profiler's `pii_flag` + a storage-layer lint.
- **I2** Every `audience_snapshot` row has non-null `hit_reason` and `features`
  (D11). Compiler guarantees: a segment with no selectable predicate cannot
  materialise.
- **I3** `as_of_ts` on a snapshot ≤ the `as_of_ts` of every feature/raw row it
  read (no time-travel leakage across sources). Verified at materialisation.
- **I4** `feature_store` rows are append-only by `(as_of_ts)`; producers never
  overwrite — a new `as_of_ts` supersedes. Point-in-time queries select the
  max `as_of_ts ≤ requested`.
- **I5** DSL nodes carry their capability code; the compiler refuses a query
  whose J node's `over` survivor set is unbounded (no preceding B/F narrowing).

## 6. Cross-references

- ← Depends on: [00](./00-prd.md).
- → Consumed by: [11](./11-runtime-core.md) (writer owns these tables),
  [12](./12-query-engine.md) (compiles DSL → SQL over these),
  [13](./13-semantic-layer.md) (builds `semantic_catalog`),
  [20](./20-ingestion.md) (writes `feature_store`/`raw_*`),
  [21](./21-rest-api.md) (wire shapes).
