# Improvements Review — Deferred Findings Backlog

Findings deferred out of their originating phase, with severity, citation, and a
one-line fix shape so a later phase can pick them up without re-deriving context.
Append-only.

## From Phase 1 / T1 (M0) — engine spine

### P1-1 — Read path must refresh the DuckLake snapshot per query

- **Citation**: `crates/execution/src/lib.rs` (`reader_loop`, `DETACH dro; <attach_sql>` before every query).
- **Finding**: a long-lived read-only DuckLake attach is **pinned to the snapshot
  at attach time** — it does not see tables/commits made afterward. T1 works
  around this by re-issuing `DETACH dro; ATTACH ... AS dro (READ_ONLY)` before
  every query.
- **Why deferred**: correct and fast enough for T1's read load; the cost is one
  detach+attach per query (~ms).
- **Fix shape (later phase)**: when read QPS matters (T2 perf calibration,
  `71-performance-budgets.md`), either use a small read-connection pool that
  re-attaches on a cadence rather than per query, or check DuckLake for a
  snapshot-refresh API that avoids full re-attach.

### P2-2 — `value_to_json` maps temporal/decimal/struct/map to null

- **Citation**: `crates/execution/src/lib.rs` (`value_to_json` catch-all).
- **Finding**: T1's query surface only produces `VARCHAR` and `BIGINT`, which are
  mapped precisely; other DuckDB types fall back to null.
- **Fix shape**: extend the match arms when the DSL (T2) or feature predicates
  (T4) start returning those types (timestamps → ISO-8601 string, decimals →
  number, struct/map → JSON object).

### P3-3 — Ingress validation is manual regex, not the `validator` crate

- **Citation**: `crates/ingress/src/lib.rs` (`validate_ident`).
- **Finding**: boundary validation is correct but hand-rolled; AGENTS.md § Input
  Validation recommends the `validator` crate.
- **Fix shape**: derive `Validate` on the request DTOs when they grow beyond
  T1's two endpoints; keep `validate_ident` semantics.

### P3-4 — Micro-batch is passthrough on T1

- **Citation**: `crates/ingestion/src/lib.rs` (`IngestRaw` flushes immediately).
- **Finding**: each onboard batch flushes via the writer's multi-row insert; the
  `micro_batch_flush_rows` config is not yet exercised. Real cross-call
  accumulation lands with the CDC adapter.
- **Fix shape**: in the CDC-adapter phase, accumulate rows per `(system, entity)`
  in the actor and flush on `micro_batch_flush_rows` / age.

### P3-5 — Object-storage per-file latency unbenched (carried from research)

- **Citation**: `docs/research/spike-microbatch-compaction.md` R1.
- **Finding**: T1 ran on local SSD; the 30 s / 50 k flush numbers are unvalidated
  on S3/OSS.
- **Fix shape**: bench on the target object storage before locking the flush
  interval; do this when a real object-storage target is chosen.

### P3-6 — Scoped `std::fs` allow for advisory locking

- **Citation**: `crates/storage/src/lib.rs` (`Writer::_lock` field + `OpenOptions`
  in `Writer::attach`).
- **Finding**: `fs2::FileExt` (advisory file lock enforcing the single writer,
  D3) is only impl'd for `std::fs::File`, not `tokio::fs::File`; this is a
  synchronous startup op outside the async runtime. `#[allow(clippy::
  disallowed_types)]` is scoped here with that justification.
- **Fix shape**: none unless the lock mechanism changes; keep the allow + comment.

## From the T1 code review (self-review pass)

### P3-7 — Newtype domain primitives

- **Citation**: `crates/ingress/src/lib.rs` (`system`/`entity`/`sql` as bare
  `String`); `crates/storage/src/lib.rs`.
- **Finding**: AGENTS.md § Type Design says "newtype every domain primitive";
  `system`/`entity`/`sql` are raw `String`.
- **Fix shape**: introduce `System`/`Entity` (private field + fallible
  constructor reusing `core::validate_ident`) and a `Sql` bound type when the
  DSL (T2) stabilises the query surface. Fold into T2.

### P3-8 — `CatalogPaths` data clump

- **Citation**: `(catalog_path, data_path)` repeated in `Writer::attach`,
  `open_reader`, `read_only_attach_sql`, `Engine::build`.
- **Finding**: the pair travels together — a `CatalogPaths` type wants to be
  born.
- **Fix shape**: introduce `CatalogPaths { catalog, data }` in `core`, pass it
  through; do when a 3rd caller appears or during T2.

### P3-9 — `AppState`/`EngineConfig` not `#[non_exhaustive]`

- **Citation**: `crates/ingress/src/lib.rs` (`AppState`),
  `crates/core/src/config.rs` (`EngineConfig`).
- **Finding**: `Error` and `QueryResult` were made non-exhaustive in the review
  pass, but `AppState` (constructed cross-crate by the server) and
  `EngineConfig` (constructed via struct literal in tests) were left exhaustive
  to avoid forcing constructors.
- **Fix shape**: add `#[non_exhaustive]` plus a constructor (`AppState::new`,
  `EngineConfig::new`/builder) when the surface is otherwise stable.

### P3-10 — Acceptance #5 not demonstrated at engine level

- **Citation**: `crates/storage/src/lib.rs` (`test_should_persist_across_restart`
  is storage-level).
- **Finding**: restart durability is proven at the storage layer (DuckLake
  persists), not via a full `Engine::build → drop → Engine::build` cycle.
- **Fix shape**: add an e2e restart test once the engine owns restorable state
  (it currently owns none beyond DuckLake); low priority.

## From the independent two-axis review (parallel sub-agents)

### Fixed in this pass (commit `phase 1 review (independent): …`)

- **`columns` count unbounded** → added `MAX_COLUMNS = 1024` cap in ingress
  `onboard` (both axes converged; DoS on `CREATE TABLE` width). Pinned by
  `test_should_reject_too_many_columns`.
- **`from_yaml_str` mis-categorised error** → was `Error::Ingestion`, now
  `Error::InvalidInput` (both axes converged; YAML parse ≠ ingestion failure).
- **`impl Trait` in public APIs** → `Reader::query` and `IngestionHandle::ingest_raw`
  now take `&str` (AGENTS.md § Code Style; explicit types in public APIs).
- **`compaction_loop` un-handled panic** → now runs under a `JoinSet` supervisor
  that logs + respawns on panic (AGENTS.md § Async).

### Deferred (smells / T2 scope)

- **P3-11 `map_err` duplication** — the `Error::Storage/Execution(BoxError::from(e))`
  shape repeats ~16×; extract `fn storage_err<E: Into<BoxError>>(e) -> Error`.
- **P3-12 `escape_for_sql_literal` doc inaccurate** — doc says backticks/quotes/
  backslashes, body only escapes `'`→`''`; align doc to body.
- **P3-13 `Freshness::worst_source` as `String`** — closed domain (`batch`/`cdc`);
  make it an enum (round-trips to the same camelCase JSON).
- **T2-scope (not T1 bugs)** — `/query` IO timeout (`spec 21 I5`), AuthN/AuthZ
  + `/readyz` (`spec 21 I1`), raw-SQL escape-hatch approval token + audit
  (`spec 21 §4`), startup read-only probe (`spec 11 I2`). These land with the
  DSL + guardrails phase (`91-impl-plan` Phase 2 / T2).
- **criterion 6 micro-batch** — already P3-4; the independent review confirms
  `micro_batch_flush_rows` is dead config until the CDC adapter.

## From the T2 independent two-axis review (parallel sub-agents)

### Fixed in this pass

- **SetOp recursion depth** (Standards HARD, DoS): `compile()` now caps SetOp
  nesting at `MAX_NESTING = 8` via `compile_at(q, depth)` (AGENTS.md § Resource
  Limits — explicit depth limits for nested parsing). Pinned by
  `test_should_reject_deeply_nested_setop`.

### Deferred (Phase-2, tracked here so the code's "see specs/93" citations hold)

- **P2-EXPLAIN — EXPLAIN-based cost pre-flight (AC#3 — implemented for rows).**
  Issue #3 AC#3 requires an over-budget query rejected *before it executes*.
  **Implemented** (post-review): `guardrail::explain_cost` runs
  `EXPLAIN (FORMAT JSON)` and parses the max `Estimated Cardinality` into
  `est_rows`; `QueryEngine::prepare` enforces row budgets (`max_output_rows`,
  and `sync_row_cap` → `Mode::Async` → `run_sync` rejects `TooLarge`) **before**
  the query runs. Pinned by `test_should_reject_over_budget_query_pre_execution`.
  **Remaining (DuckDB limitation, runtime-only):** EXPLAIN does not expose
  bytes-scanned or memory, so `max_bytes_scanned` / `memory_limit` are not
  pre-flighted — they are bounded at runtime by the `memory_limit` PRAGMA and
  the statement timeout. `enforce()` over an `Estimate` remains unit-tested for
  those fields for when a stable cost signal exists.
- **P2-FRESH — per-source freshness grading (AC#4/I4 partial).** `CompiledQuery.sources`
  is collected but unused; M1 emits `Freshness::batch(lag)` from one global
  clock because only batch sources exist. Per-source grading needs source-type
  metadata, which lands with the CDC adapter.
- **P2-DISTINCT — `Op::Distinct` AST node absent.** Spec 10 §3 lists `Distinct { key }`;
  M1 instead puts `key` top-level on `SegmentQuery` (implicit `SELECT DISTINCT`).
  Functionally equivalent for M1; reconcile the AST with the spec when a use
  case needs distinct over a non-key projection.
- **P3 smells (judgement)**: `now_epoch()`/`available_parallelism()` duplicated
  across crates (→ `core`); `memory_limit: String` parsed in two places (→
  `MemoryLimit` newtype); `within_days: u32` → `NonZeroU32`; `guardrails.threads`
  conflates DuckDB threads with the in-flight query `Semaphore` (→ separate
  `max_concurrent_queries`); reader `std::thread` has no restart-on-panic
  supervision (pre-existing T1 surface); escape-hatch returns `QueryError::InvalidDsl`
  (minor category drift); wire emits `lagSeconds` vs spec's `lagHours` (T1 drift).
