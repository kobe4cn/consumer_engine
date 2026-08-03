# T4 — Feature Store + Semantic Layer (detailed plan, closes M3)

Status: planning artifact (Phase 0). This is the detailed expansion of
`91-impl-plan.md` Phase 4 (tasks 4.1–4.4), authored in the `t3` file-by-file
style before any Rust is written. The parent spec set is
[`10`](./10-data-model.md), [`12`](./12-query-engine.md), [`13`](./13-semantic-layer.md),
[`20`](./20-ingestion.md), [`21`](./21-rest-api.md).

Origin: the planner read the codebase + specs and produced this plan; the M3
exit criteria + verification gates are in the "Exit criteria" section.

## Goal

Implement the **F** capability (`FeatureProducer` trait + `feature_store` write
path + wide pivot views + `Feature` DSL predicate) and the agent-facing
discovery layer (L0 Profiler + L1 Intent RAG via `/catalog`), so the "periodic
buyers" example resolves end-to-end, a newly onboarded table is queryable with
auto-generated descriptions, and graded per-source freshness is reported.

## Phase A — Foundations: freshness/feature/catalog types + storage DDL & writers

- `crates/core/src/freshness.rs`: extend the freshness model for D5 (graded per
  source). Add `SourceType` enum (`Batch`, `Cdc`, serde `lowercase`), `SourceMeta
  { source_type, last_epoch_secs }`, `FreshnessRegistry(DashMap)` with
  `set/get/worst(sources, now)`. `worst()` computes lag = now − last_epoch per
  touched source and returns the max-lag source's type as `worst_source`;
  empty/unknown default to `Batch`, lag 0. `Freshness::batch(lag)` stays for
  back-compat/tests.
- `crates/core/src/feature.rs` (new): `FeatureRow { user_id, feature_name,
  num_value, as_of_ts, producer_id }` (Debug, Clone, PartialEq; plain DTO;
  re-exported).
- `crates/core/src/semantic.rs` (new): `SemanticType` enum, `CatalogRow`,
  `CatalogHit` (re-exported).
- `crates/core/src/ident.rs`: `validate_feature_name(name)` — allowlist
  `^[a-zA-Z0-9_.-]{1,64}$` (note the `.` vs `validate_ident`; reused for feature
  names + producer ids).
- `crates/core/Cargo.toml`: add `dashmap.workspace = true`.
- `crates/storage/src/lib.rs`:
  - `ensure_feature_store_table()` (`dl.feature_store`, no PK — MERGE-limit
    spike).
  - `ensure_semantic_catalog_table(embedding_dim)` (`dl.semantic_catalog`,
    `FLOAT[]` variable list for M3; flag phase-2 fixed `FLOAT[dim]` + HNSW).
  - `write_feature_rows(rows)` (parameterised multi-row INSERT; append-only;
    validates `feature_name`/`producer_id`).
  - `write_catalog_rows(rows)` (parameterised INSERT; embedding bound as a list).
  - `refresh_feature_wide_view(family, short_names)` (`CREATE OR REPLACE VIEW
    dl.feature_wide_{family}`; one `arg_max(num_value, as_of_ts)` column per
    short name).
  - Tests: `test_should_write_and_read_feature_store`,
    `test_should_refresh_feature_wide_view`, `test_should_reader_resolves_writer_wide_view`
    (the cross-alias resolution test).

## Phase B — Producer trait + registry + ingestion write path (task 4.1)

- `crates/ingestion/Cargo.toml`: add `consumer_engine-execution`,
  `async-trait`, `dashmap`, `chrono` (promote from dev); keep `consumer_engine-core`.
- `crates/ingestion/src/producer.rs` (new):
  - `#[async_trait] trait FeatureProducer: Send + Sync { fn id(&self) -> &str;
    async fn run(&self, as_of: &str) -> core::Result<ProducerOutput>; }`
    (document the `async-trait` + `dyn` object-safety reason per AGENTS.md).
  - `ProducerOutput { rows }` + `families()` (splits each `feature_name` on the
    first `.` into `(family, short)`).
  - `ProducerRegistry(DashMap<String, Arc<dyn FeatureProducer>>)`: `register`,
    `get`, `ids`.
- `crates/ingestion/src/lib.rs`:
  - `IngestionHandle` gains `registry: Arc<ProducerRegistry>`. `start(writer,
    registry)` signature change.
  - `Cmd::WriteFeatures { rows, reply }`, `Cmd::WriteCatalog { rows, reply }`.
    `writer_loop` arms → `write_feature_rows` then `refresh_feature_wide_view`
    per distinct family; `write_catalog_rows`.
  - Handle methods: `write_features`, `write_catalog`, `run_producer(id, as_of)`
    (looks up the producer, **awaits `producer.run(as_of)` on the caller's
    async task** — the producer reads via its own `Reader`; the writer thread
    never blocks on async — then sends `Cmd::WriteFeatures`).
  - Test: `test_should_run_producer_writes_feature_store_and_view`.

## Phase C — Concrete `cadence_regularity` producer with point-in-time (task 4.2, I3)

- `crates/ingestion/src/producers/mod.rs` + `cadence.rs` (new):
  - `CadenceRegularityProducer { reader, orders: Dataset, producer_id }`. `id()
    = "cadence_sql"`.
  - `run(as_of)`: bounded read `SELECT user_id, ts FROM dro.raw_{system}_{entity}
    WHERE ts <= ?` (as_of bound as Text — **I3 enforced at SQL level**; VARCHAR
    ISO-8601 ts compare lexicographically = chronological). Defensive
    `LIMIT 1_000_000`. Compute per-user regularity **in Rust**: sort each
    user's timestamps ≤ as_of; if <2 events → 0.0; else intervals (epoch
    seconds), `cv = stddev_pop(intervals)/mean(intervals)`, `regularity =
    max(0.0, 1.0 − cv)` clamped to `[0,1]`. Emit one `FeatureRow` per user with
    ≥1 event.
  - Tests: `test_should_run_producer_point_in_time_bounded` (seed T1<T2<T3; run
    as_of=T2; assert a user whose only purchase is T3 is absent/zero — proves
    I3); `test_should_score_regular_buyers_high` (regular → >0.7; erratic →
    <0.3).

## Phase D — `Feature` DSL op: AST + parser + compiler (task 4.4 compiler half)

- `crates/query/src/ast.rs`: replace the unit stub `Feature,` with
  `#[serde(rename_all="camelCase")] Feature { name, op: Cmp, value:
  serde_json::Value }`.
- `crates/query/src/parse.rs`: `validate_op` gets a dedicated
  `Op::Feature { name, op, value }` arm — `validate_feature_name(name)`; require
  `op ∈ {Eq,Ne,Lt,Le,Gt,Ge}` (reject `In/NotIn/Like/NotLike`); require `value`
  is a JSON number; require `name` contains a `.`. Split the existing
  `test_should_reject_unsupported_capability`.
- `crates/query/src/compiler.rs`: `Feature` arm pushes a conjunct — split `name`
  on first `.` → `(family, short)`; `EXISTS (SELECT 1 FROM {alias}.feature_wide_{family}
  f WHERE f.user_id = base.{key} AND f.{short} {cmp} ?)` with `value` bound.
  Validate `family`/`short` via `validate_ident`. Do **not** add the feature
  view to `sources` (derived; freshness is over raw sources only). Fallback
  path (if Phase A's cross-alias view test failed): inline EAV pivot. Choose
  one path from the Phase A result.

## Phase E — `consumer_engine-semantic` crate: L0 Profiler + L1 IntentRag (4.3/4.4)

- `crates/semantic/Cargo.toml` (new crate): deps `core`, `execution`,
  `async-trait`, `serde`, `serde_json`, `thiserror`, `tracing`, `regex`, `tokio`,
  `chrono`. Crate attrs per AGENTS.md. Workspace dep
  `consumer_engine-semantic`.
- `crates/semantic/src/llm.rs` (new): `#[async_trait] trait LlmClient { async fn
  describe_column(..) -> Result<String>; }`, `trait EmbeddingModel { fn dim;
  fn embed(&str) -> Vec<f32>; }`. `StubLlm` (heuristic) + `StubEmbed`
  (deterministic hash → unit vector). M3 defaults (no network → deterministic).
  Real HTTP client gated behind a future `semantic-llm` feature.
- `crates/semantic/src/profiler.rs` (new): `Profiler { reader, llm, embed,
  sample_rows, sample_value_bytes }`. `async onboard(system, table) ->
  Result<Vec<CatalogRow>>`: bounded sample; classify `semantic_type`/
  `data_type`/`pii_flag` heuristically; build `sample_values` (≤20, each
  truncated); **redact PII samples** (I4); description via `llm`; embedding via
  `embed` (only the description is embedded, never PII values — I4); emit one
  `entity_type=table` row. Tests: `test_should_redact_pii_sample_values`,
  `test_should_bound_sample_size`, `test_should_classify_event_ts_and_identifier`.
- `crates/semantic/src/intent_rag.rs` (new): `IntentRag { reader, embed,
  default_k }`. `async retrieve(utterance, k) -> Result<Vec<CatalogHit>>`:
  embed utterance; read catalog; brute-force cosine in Rust; return top-`k`
  (bounded — I3). Empty catalog → `Ok(vec![])`. Test:
  `test_should_retrieve_bounded_candidates`.
- `crates/semantic/src/lib.rs`: re-export `Profiler`, `IntentRag`, `LlmClient`,
  `EmbeddingModel`, `StubLlm`, `StubEmbed`.

## Phase F — Graded freshness in the query engine (P2-FRESH)

- `crates/query/src/engine.rs`: replace `last_ingest_epoch: Arc<AtomicI64>`
  with `freshness: Arc<FreshnessRegistry>`. `QueryEngine::new(reader,
  ingestion, guardrails, freshness)`. `run_sync`:
  `self.freshness.worst(&compiled.sources, now_epoch())`. `materialize`/
  `snapshot_meta` unchanged (as_of stays `now()` for M3 — snapshot-level I3 is
  producer-level-tested in Phase C; documented in `93`).
- Integration test `test_should_report_worst_source_freshness` in the query
  engine.

## Phase G — REST surface: onboard profiling, `/catalog`, `/producers/run`

- `crates/ingress/Cargo.toml`: add `consumer_engine-semantic`, `chrono`.
- `crates/ingress/src/lib.rs`: `AppState` gains `profiler: Arc<Profiler>`,
  `intent_rag: Arc<IntentRag>`, `freshness: Arc<FreshnessRegistry>` (replaces
  `last_ingest_epoch`). `OnboardRequest`: add `#[serde(default)] source_type:
  Option<String>` (default `"batch"`; validate). `onboard`: after `ingest_raw`,
  `freshness.set`; then `profiler.onboard` (bounded + stub-LLM = fast);
  `write_catalog`; return extended `OnboardResponse { rows_inserted, profiled,
  columns }`. Wrap profile in `tokio::time::timeout` (spec 21 I5) — on timeout
  return 200 with `profiled:false`.
- `crates/ingress/src/catalog.rs`: `GET /catalog` (q byte cap 1024, k 1..=50
  default 20) → `Json<Vec<CatalogHit>>`.
- `crates/ingress/src/producers.rs`: `POST /producers/run` (`{ producerId,
  asOf? }`; `validate_feature_name`; `run_producer`; return `{ rowsWritten }`).
  Engineer-facing (elevated; auth stubbed in M3).

## Phase H — Server wiring

- `apps/server/src/lib.rs` (`Engine::build`): construct
  `Arc<FreshnessRegistry::new()>`; `ensure_feature_store_table` +
  `ensure_semantic_catalog_table(emb_dim)` at startup; build `StubLlm`/
  `StubEmbed`; construct `Profiler`/`IntentRag`; build `ProducerRegistry`,
  register `CadenceRegularityProducer`; pass into `IngestionHandle::start`;
  `QueryEngine::new(reader, ingestion, guardrails, freshness)`; `AppState`.
- `apps/server/Cargo.toml`: add `consumer_engine-semantic`.

## Phase I — M3 exit tests (the gate)

- `apps/server/tests/e2e.rs`:
  - `test_should_report_worst_source_freshness` (onboard `erp.orders` batch +
    `erp.events` cdc; POST `/query` touching both; assert `worstSource ==
    "batch"`).
  - `test_should_resolve_periodic_buyers_end_to_end` (onboard crafted users;
    `POST /producers/run {cadence_sql}`; `POST /query` the PRD DSL; assert
    periodic buyer returned, erratic not; then `POST /jobs` + assert snapshot
    count + non-null `hit_reason`).
  - `test_should_profile_new_table_on_onboard` (assert `profiled==true` +
    `columns` non-empty; `GET /catalog?q=user&k=5` returns ≤5 hits with
    descriptions).
  - `test_should_catalog_returns_bounded_candidates` (`GET /catalog?k=3` never
    >3).
- Fix signature-driven breakage in `spawn`/`spawn_guardrails` helpers.

## Phase J — Full verification gate + spec reconciliation

`cargo build --workspace`, `cargo test --workspace`, `cargo +nightly fmt`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo audit`,
`cargo deny check`, `make check-agent-sync`. Update `91`/`90`/`93`.

## Risks (carried)

- **Cross-alias wide-view resolution (highest risk):** a view created on the
  writer (`dl`) referencing `feature_store` may not resolve when the reader
  scans `dro.feature_wide_<family>`. Phase A includes a storage test; fallback
  is the inline EAV-pivot subquery in the compiler.
- **DuckLake `FLOAT[]` in `CREATE TABLE`:** unverified on DuckLake. M3 uses
  `FLOAT[]` + brute-force cosine; if rejected, fall back to JSON embedding.
- **Object-safety:** `FeatureProducer`/`LlmClient` need `dyn` dispatch →
  `async-trait` (AGENTS.md exception, documented).
- **Point-in-time (I3) scope:** producer-level in M3; snapshot-level stays
  `as_of=now()` (documented in `93`).
- **Signature cascade:** `QueryEngine::new`, `IngestionHandle::start`,
  `AppState`, `Engine::build`, the e2e `spawn*` helpers.
- **PII redaction (I4):** Profiler redacts `pii_flag` samples before any
  embedding/description.
- **No real LLM in M3:** Stub clients keep tests deterministic; G5 satisfied by
  heuristic stubs.

## M3 exit criteria

1. Periodic buyers resolves end-to-end (onboard → producer → `Feature` compile →
   query → materialise).
2. New table queryable + profiled (G5).
3. Graded freshness — a CDC+batch query reports `worstSource == "batch"`.
4. Feature predicate on a registered feature (covered by #1).
5. L0 profiles / L1 retrieves bounded.
6. Producer point-in-time (I3).
7. Full Rust gate green, no new audit/deny failures.
