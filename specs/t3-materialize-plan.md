# T3 — Materialise Audience Snapshots + Delivery Pull (detailed plan)

Status: **Phases A–H done** — Phase 3 (T3) complete; M2 exit criteria met
(`test_should_materialise_snapshot_atomically_with_hit_reason` green; full gate
set green). Phase A landed in `09cb979`; Phases B–H landed in the Phase 3 commit.
This is the detailed expansion of `91-impl-plan.md` Phase 3 (issue #4).

Origin: an isolated scout→planner agent chain (see `docs/agents/`); the planner
read the codebase + `specs/{10,12,21}` and produced this file-by-file plan.

## Goal

Implement T3 (issue #4): the async audience-snapshot materialisation path —
compile a DSL segment, atomically write its distinct keys (non-null `hit_reason`
+ frozen `features` + `as_of_ts`) into `audience_snapshot` via the single-writer
actor, expose it through `POST /jobs` → poll `GET /jobs/:id`, and serve snapshot
metadata + a short-lived presigned Parquet export at `GET /audience/:id` and
`GET /audience/:id/export?format=parquet`. REST-seam tests including
`test_should_materialise_snapshot_atomically_with_hit_reason`.

## Phase A — Shared foundations ✅ DONE (`09cb979`)

- `core/src/catalog.rs`: `READ_ONLY_CATALOG_ALIAS="dro"`, `WRITE_CATALOG_ALIAS="dl"` (re-exported).
- `core/src/snapshot.rs`: `SnapshotSpec { snapshot_id, campaign_id, as_of_ts, features, hit_reason }` (all `String`; re-exported).
- `storage`: `Writer::ensure_audience_snapshot_table` (no PK), `materialize_snapshot` (atomic single `INSERT…SELECT`, bound params, validated `key_column`), `export_snapshot_parquet` (`COPY … TO parquet`); uses the core alias consts.

## Phase B — Compiler alias threading (query/compiler)

- `crates/query/src/compiler.rs`: thread a catalog alias so the writer runs the
  materialise subquery under `dl.*` while the reader EXPLAINs under `dro.*`.
  - `fn raw_table(d, alias)`, `fn base_select(.., alias)`, `fn compile_at(q, depth, alias)` (alias recurses into `SetOp`).
  - `pub fn compile(q)` = `compile_with_alias(q, READ_ONLY_CATALOG_ALIAS)` (read path unchanged; existing tests still see `dro.raw_*`).
  - `pub fn compile_with_alias(q, alias)`; re-export from `crates/query/src/lib.rs`.
  - Test `test_should_compile_with_write_alias` asserting `dl.raw_erp_orders`.
- `crates/execution/src/lib.rs`: use `READ_ONLY_CATALOG_ALIAS` in the reader
  `DETACH dro; <attach>` refresh string (DRY; no behaviour change).

## Phase C — Ingestion actor Q2 / Q-export commands

- `crates/ingestion/src/lib.rs`:
  - `Cmd::Materialize { subquery_sql, subquery_params: Vec<Value>, key_column, spec: SnapshotSpec, reply }`.
  - `Cmd::ExportParquet { snapshot_id, dest: PathBuf, reply }`.
  - `writer_loop` arms → `writer.materialize_snapshot(...)` / `writer.export_snapshot_parquet(...)`.
  - `IngestionHandle::materialize_snapshot(...) -> Result<u64>`, `export_parquet(snapshot_id, dest) -> Result<()>`.
  - Test `test_should_materialize_via_handle`.

## Phase D — QueryEngine.materialize (read→write bridge)

- `crates/query/Cargo.toml`: add `consumer_engine-ingestion`, `chrono`.
- `crates/query/src/engine.rs`:
  - Add field `ingestion: IngestionHandle`; change `QueryEngine::new(reader, ingestion, guardrails, last_ingest_epoch)`.
  - `pub async fn materialize(&self, q: &SegmentQuery, campaign_id: &str) -> Result<String>` returning `snap_<uuidv7>`:
    1. `compile(q)` then **best-effort** `explain_cost` (no `max_output_rows` rejection — large is the point; log est_rows). Reuses T2 EXPLAIN for early bad-DSL failure.
    2. Scalars: `snapshot_id = Uuid::now_v7()`; `as_of_ts = chrono::Utc::now().to_rfc3339()` (M2 = materialisation time; true I3 point-in-time bounding lands in T4); `features = "{}"` (non-null; Feature Store is T4); `hit_reason = serde_json::to_string(q)` (the validated DSL — faithful per-row reason for B-only segments; refine when F/J land).
    3. `compile_with_alias(q, WRITE_CATALOG_ALIAS)` → subquery SQL/params (references `dl.raw_*`).
    4. `self.ingestion.materialize_snapshot(&write_sql, &write_params, &q.key, &spec).await?`.
    5. Return `format!("snap_{snapshot_id}")`.
  - Split: "Q2 materialise work" (`materialize -> snap_id`, here) from "REST job lifecycle" (jobId/poll, ingress) — cleaner SoC; note in module docs.
  - `snapshot_meta(snap_uuid) -> Result<SnapshotMeta>` reading `dro.audience_snapshot` (cast `as_of_ts`/JSON cols to VARCHAR because `execution::value_to_json` maps temporal/JSON → null today).
  - Test `test_should_materialize_returns_snapshot_id`.

## Phase E — Ingress: jobs, audience, presigned export

- Workspace `Cargo.toml`: add `dashmap`, `hmac`, `sha2`, `subtle`, `rand`, `tokio-util` (io), `bytes`.
- `crates/ingress/src/presign.rs`: HMAC-SHA256 short-lived token (AGENTS.md § Crypto: `subtle::ConstantTimeEq`, OsRng secret). `sign(key, snapshot_id, ttl) -> "{expiry}.{hex_hmac}"`; `verify(key, snapshot_id, token) -> bool` (reject expired, constant-time).
- `crates/ingress/src/jobs.rs`: `JobRegistry(Arc<DashMap<String, JobStatus>>)`; `JobStatus { Running, Done(snap), Failed(err) }`; `POST /jobs` (parse dsl, validate campaign_id, mint `j_<uuid>`, spawn materialise task with panic handling, `202 {jobId}`); `GET /jobs/:id` (poll / 404).
- `crates/ingress/src/audience.rs`: `GET /audience/:id` → metadata + `downloadUrl` (presigned, 15-min TTL); `GET /audience/:id/export?token=…` → verify token (401 on fail), `COPY` to temp parquet via ingestion, stream/serve with `content-disposition`.
- `crates/ingress/src/lib.rs`: `AppState { ingestion, query_engine, last_ingest_epoch, jobs: Arc<JobRegistry>, signing_key: Arc<[u8;32]> }`; routes `/jobs`, `/jobs/:id`, `/audience/:snapshot_id`, `/audience/:snapshot_id/export`.

## Phase F — Server wiring

- `apps/server/src/lib.rs`: `Engine::build` — `let signing_key: [u8;32] = rand::random()` (OsRng); construct `QueryEngine::new(reader.clone(), ingestion.clone(), guardrails, epoch)`; build `JobRegistry`; pass into `AppState`.
- `apps/server/Cargo.toml`: add `rand`.

## Phase G — REST-seam tests (the exit gate)

- `apps/server/tests/e2e.rs`: `test_should_materialise_snapshot_atomically_with_hit_reason` (onboard → `POST /jobs` 202 → poll done → `GET /audience/:id` → export → decode parquet → assert row count + every `hit_reason`/`features` non-null); `test_should_post_jobs_returns_202_with_jobid`; `test_should_poll_job_until_done_or_failed`; `test_should_reject_jobs_with_bad_campaign_id`; `test_should_stream_parquet_export` (+ expired token → 401).

## Phase H — Verification gate

`cargo build`, `cargo test` (esp. `-p storage -p ingestion -p query` + `--test e2e`), `cargo +nightly fmt`, `cargo clippy --workspace --all-targets -- -D warnings` (boundary `unwrap_used`/`expect_used` clean), and **`cargo audit` + `cargo deny check`** (lockfile changes — new crypto/streaming deps).

## Risks (carried from the planner)

- Export runs on the **writer** (`dl` attach) to sidestep `READ_ONLY` + `COPY` uncertainty; it serialises with ingests (single writer) — acceptable for M2 test-scale, flag for later.
- `as_of_ts`/JSON via the reader: `value_to_json` maps TIMESTAMPTZ/JSON → null; `snapshot_meta` casts to VARCHAR in SQL; Parquet COPY writes native types (decoded natively in tests).
- **I3 point-in-time bounding not testable in M2** (raw tables are VARCHAR-only, no Feature Store); `as_of_ts = now` is the documented M2 behaviour; full I3 lands in T4. The exit test asserts atomicity + non-null `hit_reason`/`features`, **not** the leak invariant.
- `hit_reason` = whole-DSL JSON (faithful for B-only segments); refine to per-predicate when F/J land.
- `QueryEngine::new` signature change: update `apps/server` (the only constructor) + any tests.
- Presigned deps (`hmac`/`sha2`/`subtle`/`rand`/`bytes`/`tokio-util`): pure-Rust, audited; `cargo audit`/`deny` must pass. `rand` uses OsRng.
- `downloadUrl` is a relative path (ingress doesn't know its external host); the test prepends the spawned base URL.

## Resume instructions (fresh session)

- This plan **is** the contract; `specs/91-impl-plan.md` Phase 3 is the parent.
- Start at Phase B. Apply the `impl` skill discipline: task-by-task, gates after each phase, REST-seam tests, then an independent two-axis code review (the `subagent` extension is fixed — restart pi and the native chain works, or use the bash-driven isolated-headless-`pi` equivalent as in T1/T2 reviews).
- Phase A is committed (`09cb979`); `materialize_snapshot`/`export_snapshot_parquet` are pub-but-unused until Phase C/D wire them.
