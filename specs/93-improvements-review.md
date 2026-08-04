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

## From Phase 3 / T3 (M2) — materialised audiences + delivery pull

### P3-SUPPLY — `paste` unmaintained advisory allowlisted

- **Citation**: `deny.toml` `[advisories].ignore` (RUSTSEC-2024-0436); pulled in
  transitively by `parquet` v58, a **test-only** dev-dep of `consumer_engine-server`
  (decodes snapshot exports in the e2e suite).
- **Finding**: `paste` is an unmaintained proc-macro helper — not a security
  vulnerability — with no safe upgrade while parquet 58 depends on it.
  `cargo audit` treats it as a warning (exit 0); `cargo deny check` fails on
  unmaintained advisories by default, so it is explicitly allowlisted.
- **Fix shape (later)**: revisit when parquet drops `paste`, or replace the
  parquet-decode test path (e.g. DuckDB-side row verification) so `parquet`/`arrow`
  leave the lockfile.

### P3-PEDANTIC — pedantic-as-error gate unachievable (pre-existing core debt)

- **Citation**: `crates/{core,storage,execution}/src/*.rs` (~33 `clippy::pedantic`
  warnings: `doc_markdown` missing backticks, `LazyLock` superseded
  `once_cell`-style type, `manual_map_or`, `manual_unwrap_or`, `let_and_return`,
  `redundant_closure`, `match_same_arms`, `cast_possible_wrap`).
- **Finding**: the AGENTS.md "stricter linting" gate
  (`-D warnings -W clippy::pedantic -W clippy::unwrap_used/expect_used/indexing_slicing/panic`)
  cannot pass today because `consumer_engine-core` alone emits 7 pedantic
  *errors* under `-D warnings`. These pre-date Phase 3 and are out of its scope
  (refactor-smear). The **binding** gate `cargo clippy --workspace --all-targets
  -- -D warnings` is green; Phase 3's *new* production code (`presign`/`jobs`/
  `audience`/`engine::materialize`/`ingestion` arms) introduces no
  `unwrap`/`expect`/`panic`/indexing in non-test paths.
- **Fix shape (later)**: a dedicated "pedantic sweep" pass across core/storage/
  execution (backticks, `LazyLock`, `map_or`, `let-else`, `NonZeroU32` for
  `within_days`, dedup `now_epoch`). Not coupled to any feature phase.

### T4-I3 — point-in-time bounding deferred (by design, M2 scope)

- **Citation**: `crates/query/src/engine.rs` (`materialize`: `as_of_ts = now()`).
- **Finding**: I3 (`audience_snapshot.as_of_ts` ≤ every feature/raw row's
  `as_of_ts`) is **not** enforceable in M2 — raw tables are `VARCHAR`-only and
  the Feature Store (the typed producer path) lands in T4. M2 sets
  `as_of_ts = materialisation time` (documented). The exit test asserts I2
  atomicity + non-null `hit_reason`/`features`, **not** the leak invariant.
- **Fix shape (T4)**: once Feature Store write paths + typed `TIMESTAMPTZ` raw
  columns exist, bound `as_of_ts` to the min source freshness and test the leak.

### T4-HITREASON / T4-FEATURES — snapshot payload placeholders (by design)

- **Citation**: `crates/query/src/engine.rs` (`features = "{}"`, `hit_reason =
  serde_json::to_string(q)`).
- **Finding**: `features` is the non-null placeholder (Feature Store is T4);
  `hit_reason` is the whole validated DSL JSON — a faithful per-row reason for
  B-only segments, to be refined to per-predicate when `Filter`-nesting/F/J ops
  land (T4/T5). Both satisfy I2 (non-null) today.
- **Fix shape (T4/T5)**: populate `features` from the frozen feature pivot;
  refine `hit_reason` to the selecting predicate chain.

### P3-DEPS — Phase-E dependency list deviation (justified, no action)

- **Citation**: `Cargo.toml` `[workspace.dependencies]`, `apps/server/Cargo.toml`.
- **Finding**: the plan listed workspace deps `rand, tokio-util, bytes`; the
  implementation used `getrandom` (OsRng-equivalent; lighter than `rand` and
  matching AGENTS.md § Crypto), omitted `tokio-util` (export streaming uses
  `axum::body::Body::from(Vec<u8>)` directly), and `bytes` is a server
  **test-only** dev-dep (in-memory `parquet` decode via `ChunkReader for Bytes`,
  avoiding disallowed blocking `std::fs`). All strictly cleaner than the plan.

## From Phase 4 / T4 (M3) — Feature Store + semantic layer

### T4-CATALOG-LIST — DuckDB `List` parameter binding unsupported (FIXED)

- **Citation**: `crates/storage/src/lib.rs` (`write_catalog_rows`).
- **Finding**: Phase A's catalog writer bound the `embedding` column as
  `Value::List(...)`; DuckDB's Rust binding rejects this (`"binding List
  parameters is not yet supported"`). The Phase A test `test_should_write_catalog_rows`
  was committed without ever running, so the defect lay dormant.
- **Fix applied**: the embedding is now written via a `list_value(?, ?, …)`
  constructor with one scalar placeholder per dimension (each float bound
  individually), with a dimension-consistency check. `semantic_catalog` stays
  `FLOAT[]` (variable-length) for M3's brute-force cosine; a phase-2 fixed-`FLOAT[dim]`
  + HNSW migration remains flagged.

### T4-RECENCY-NOW — Recency/Lapsed `now() - INTERVAL` binder error (FIXED)

- **Citation**: `crates/query/src/compiler.rs` (`compile_recency`,
  `compile_lapsed`).
- **Finding**: `now()` returns `TIMESTAMP WITH TIME ZONE`, and this DuckDB build
  has **no** `-(TIMESTAMPTZ, INTERVAL)` overload (binder error). The B-temporal
  ops were unit-tested only for SQL-string shape, never executed against real
  DuckDB, so the defect was latent (discovered during M3).
- **Fix applied (whole-project review)**: the compiler now renders
  `CAST(e.ts AS TIMESTAMP) >= CAST(now() AS TIMESTAMP) - INTERVAL '<n>' DAY` for
  both sides (raw `ts` is VARCHAR; both sides cast). End-to-end coverage added:
  `test_should_run_recency_and_lapsed_over_rest` executes `Recency` (recent
  buyer matches) and `Lapsed` (old buyer matches) over REST.

### T4-PRODUCER-CONFIG — producers hardcoded in `Engine::build` (DEFERRED)

- **Citation**: `apps/server/src/lib.rs` (`Engine::build`, the `CadenceRegularityProducer`
  over `erp.orders`).
- **Finding**: M3 wires one demo producer (the PRD's cadence over `erp.orders`)
  directly in server construction. A real deployment needs config-driven
  producer registration (dataset + schedule + as_of source).
- **Fix shape (later phase)**: a `producers` section in `EngineConfig` →
  `ProducerRegistry` construction; `run(as_of)` `as_of` sourced from the
  source's freshness epoch (D9), not a caller-supplied string.

### T4-SEMANTIC-TABLE-ROW — Profiler emits column rows only (design note)

- **Citation**: `crates/semantic/src/profiler.rs` (`onboard`).
- **Finding**: the detailed plan said "emit one `entity_type=table` row"; the
  implementation emits **column-level** rows only (`entity_type="column"`).
  `SemanticType` has no `Table` variant, and column rows are exactly what the
  IntentRag needs to hand the agent composable DSL predicates.
- **Action**: none for M3 (documented deviation; all exit tests pass). A table-level
  summary row can be added if a future IntentRag needs table-granular ranking.

### T4-I3 — point-in-time bounding: producer-level DONE, snapshot-level still deferred

- **Citation**: `crates/ingestion/src/producers/cadence.rs` (`WHERE ts <= ?`);
  `crates/query/src/engine.rs` (`materialize`, `as_of_ts = now()`).
- **Finding**: the earlier T4-I3 deferral is now **partly resolved** — producer
  point-in-time correctness is enforced at the SQL level (`ts <= ?`, bound as
  text; ISO-8601 compares lexicographically = chronologically) and tested
  (`test_should_run_producer_point_in_time_bounded`). Snapshot-level bounding
  **remains** `as_of_ts = materialisation time` by design for M3 (documented);
  the exit test asserts I2 atomicity + non-null `hit_reason`, not the leak
  invariant.
- **Fix shape (later)**: bound `materialize`'s `as_of_ts` to the min source
  freshness once typed `TIMESTAMPTZ` raw columns exist, and test the leak.

### T5-I5 — catalogue-freshness warning unimplemented (by design, M3 scope)

- **Citation**: `specs/13-semantic-layer.md` §3 I5 ("the query path warns if a
  referenced column's catalogue entry is older than the source's latest
  snapshot"); `crates/query/src/engine.rs` (`enforce_catalogue`).
- **Finding**: `enforce_catalogue` (issue #6 AC#3) checks catalogue **membership**
  only — it rejects columns absent from `semantic_catalog` but does not warn on
  stale catalogue entries (built before a later re-onboard). I5 is a stated spec
  invariant but is **not** an acceptance criterion of issue #6, whose AC#3
  (reject non-catalogued columns) is fully implemented + tested.
- **Fix shape (later)**: stamp each `semantic_catalog` row with the source
  snapshot it was built from (re-onboard versioning, spec 13 §4) and emit a
  `warn!` when a referenced column's entry predates the source's latest ingest
  (reusing the `FreshnessRegistry` epoch).

### T7c-TENANT — multi-tenant isolation deferred (by design, M5 scope)

- **Citation**: issue #10 AC6 ("Tenant isolation enforced — cross-tenant access
  is impossible by construction"); `specs/21-rest-api.md` §3 I2 (the compiler
  injects `tenant_id` into every SQL).
- **Finding**: the engine has **no tenant model** — there is no `tenant_id` to
  extract or inject.
- **What holds today (partial fix landed, whole-project review)**: bearer-token
  **authN** is implemented (`EngineConfig.auth_token`, constant-time middleware
  gating every route except healthz/readyz, e2e-covered) — the IDOR surface
  (any caller minting presigned exports) is closed on authenticated
  deployments. The engine remains **single-tenant** by construction (one
  catalog, one writer); there is no cross-tenant surface to leak.
- **Fix shape (later)**: `tenant_id` columns on every engine table + compiler
  injection + auth claims carrying the tenant; test that a tenant-B token
  cannot read tenant-A data.
  `tenant_id` from the verified token → `tenant_id` column on `raw_*`/
  `feature_store`/`suppression`/`audience_snapshot`/`semantic_catalog` → the
  compiler filters every query by the caller's tenant; test that a tenant-B
  token cannot read tenant-A data.

## From the GC tickets (#13/#15/#16, 2025-08)

### GC-HITREASON-REFINE — per-row matched hit_reason deferred (by design, #13 scope)

- **Citation**: `crates/query/src/engine.rs` (`materialize`: `hit_reason` = the
  validated `q.ops` array, bound per snapshot).
- **Finding**: `hit_reason` is the **per-predicate selection chain** (the op
  list), correct for every segment the DSL can express today — ops are
  AND-composed (every op matched by construction) and `SetOp` is terminal (its
  descriptor names the branch structure). The specs/92 refinement "compiler
  carries the per-row matched conjunct" (per-op matched booleans, `SetOp`
  UNION branch provenance) is **not** implemented.
- **Fix shape (later)**: a materialise compile mode that emits one `bool_or`
  column per op (reusing `compile_predicate`/`compile_recency`/… as boolean
  expressions) and tags `SetOp` branches with a branch id; then `hit_reason` =
  json of matched op descriptors per user.

### GC-DEDUP-BOUNDARY — dedup sits at the writer, not an adapter (by design, #16 scope)

- **Citation**: `crates/storage/src/lib.rs` (`upsert_raw` dedups by key before
  the MERGE); specs/20 §4 "the adapter MUST dedup a source batch by key".
- **Finding**: no `SourceAdapter` exists yet (CDC lands in #24), so the dedup
  lives at the writer boundary — defense-in-depth, and the only seam today.
- **Fix shape (#24)**: the CDC adapter dedups earlier (or relies on
  `upsert_raw`'s contract); re-verify at the adapter seam.

### GC-MAINT-BINDER — DuckLake maintenance procedures unbindable (blocked, issue #17)

- **Citation**: `crates/storage/src/lib.rs` (`Writer::probe_maintenance`);
  probe evidence: every timestamp-parameterized CALL
  (`ducklake_expire_snapshots`, `ducklake_delete_orphaned_files`,
  `ducklake_cleanup_old_files`) — literal, named-arg, and prepared-`?` forms —
  fails binder with "No function matches" even when the argument type renders
  identically to the declared `TIMESTAMP WITH TIME ZONE` parameter (duckdb
  crate 1.10505.0).
- **Finding**: the same TIMESTAMPTZ binder defect family as the documented
  `-(TIMESTAMPTZ, INTERVAL)` issue (T4-RECENCY-NOW). Maintenance procedures
  with a TSTZ parameter cannot be called on this build, so snapshot expiry and
  orphan cleanup cannot run; the maintenance pass degrades to merge-only with a
  one-time `warn!` (probed at first use).
- **Fix shape (upstream)**: re-enable `Writer::expire_snapshots` /
  `delete_orphaned_files` after a DuckLake upgrade that binds TSTZ procedure
  parameters; the capability probe flips automatically (issue #17 stays OPEN).

### GC-FRESHNESS-TENANT — freshness registry is tenant-agnostic (by design, #22 scope)

- **Citation**: `crates/core/src/freshness.rs` (`FreshnessRegistry` keyed by
  `{system}.{entity}` only); engine `enforce_catalogue` uses it for the I5
  staleness warn.
- **Finding**: two tenants ingesting the same source name share one epoch →
  a cross-tenant `lagSeconds`/stale-catalogue signal. Observability-only (no
  data leak; the compiler's tenant filter is the isolation boundary).
- **Fix shape (later)**: key the registry by `(tenant, system, entity)` and
  thread the tenant into the freshness label + staleness check.
