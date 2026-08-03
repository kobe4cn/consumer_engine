# 11-runtime-core: Engine Lifecycle & the Single Writer

Status: draft · Depends on: [10](./10-data-model.md)

## 1. Purpose

Owns process lifecycle, the actor topology, and — critically — the **single
`IngestionActor` writer** to DuckLake (D3). Everything else reads. This spec
encodes AGENTS.md § Async & Concurrency (actor model, channels over shared
state, no `Mutex<RefCell>`, `AtomicBool` shutdown) as concrete shapes.

## 2. Interface

The binary (`consumer_engine-server`) wires these actors; library crates expose
their handles.

```text
pub struct Engine { /* owns actor join set, shutdown signal */ }
impl Engine {
    pub async fn spawn(cfg: Config) -> Result<Self>;   // starts all actors
    pub fn query_handle(&self) -> QueryHandle;          // cheap cloneable sender
    pub fn ingestion_handle(&self) -> IngestionHandle;  // for /sources, /suppression
    pub async fn shutdown(&self) -> Result<()>;         // graceful, drains queues
}
```

## 2a. Actor topology & write boundary

```text
                       ┌─────────────────────────────────────────────┐
   REST (ingress) ───▶ │ QueryActor pool (read-only)                 │
                       │  - N workers, Semaphore-bounded             │
                       │  - each owns a read-only DuckDB attach      │
                       └──────────────┬──────────────────────────────┘
                                      │ materialise request (Q2)
                       ┌──────────────▼──────────────────────────────┐
                       │ IngestionActor  (SINGLE WRITER, D3)         │
                       │  owns the ONE write conn to Postgres catalog│
                       │  ┌──────────── Q1 source ingest (CDC/batch) │
                       │  ├──────────── Q2 snapshot materialisation  │
                       │  └──────────── Q3 suppression writeback     │
                       │  micro-batcher ──▶ DuckLake MERGE/INSERT    │
                       │  compaction scheduler (separate task)       │
                       └──────┬──────────────────────────────────────┘
                              │ exclusive write
                       ┌──────▼──────────────────────────────────────┐
                       │ DuckLake (Parquet@obj store + Postgres cat) │
                       └─────────────────────────────────────────────┘
   external delivery ──POST /suppression──▶ Q3
   source systems ─────CDC/batch──────────▶ Q1
```

Read path and write path never share a DuckDB connection: readers `ATTACH` the
catalogue read-only; only `IngestionActor` holds the writable attach.

## 3. Invariants

- **I1 Single writer.** At most one `IngestionActor` instance holds a writable
  catalogue attach process-wide (D3). Enforced at spawn: a second spawn returns
  `Error::WriterAlreadyHeld`.
- **I2 Read-only query path.** `QueryActor` connections attach with
  `READ_ONLY`; any DDL/DML attempt errors at DuckDB. Verified by a startup
  probe that asserts a probe INSERT fails.
- **I3 Graceful shutdown drains.** `shutdown()` stops accepting new work, drains
  Q1–Q3 to a clean checkpoint, flushes the micro-batcher, then detaches.
  Restart resumes from the last committed snapshot (DuckLake snapshots make
  this idempotent).
- **I4 Backpressure.** Each queue is bounded (`tokio::sync::mpsc`); a full Q2
  returns `Error::MaterialiseBackpressure` to the caller (sync query may fall
  back to a row-capped direct return per [12 §4](./12-query-engine.md)).

## 4. Behaviour

- **Restart/recovery (D6).** On start, `IngestionActor` reconciles: reads the
  latest DuckLake snapshot, resumes CDC offsets from the catalogue's last
  committed marker, and runs one compaction pass if small-file count exceeds
  threshold. No data loss; at-least-once from CDC source deduped by PK MERGE.
- **Panic policy (AGENTS.md).** Actors never panic on external input. A worker
  panic is caught by the supervisor (`JoinSet` + `spawn`), logged with the
  offending request's correlation id (redacted), and the worker is restarted;
  the offending request returns a typed error, never an `unwrap`.
- **Micro-batcher (D6).** Flushes on `max_rows` OR `max_age` (config; default
  50k rows / 5 s). On shutdown it force-flushes. Compaction runs on a cron task
  (default hourly) calling DuckLake maintenance: merge small files, expire
  snapshots older than retention ([10](./10-data-model.md); default 2y for
  snapshots, configurable per table).
- **Concurrency on the read path.** `QueryActor` pool size = physical cores;
  in-flight heavy queries bounded by a `Semaphore` (guardrail, [71](./71-performance-budgets.md)).
- **Read snapshot refresh (T1 realisation).** A long-lived read-only DuckLake
  attach is pinned to the snapshot at attach time and does **not** see later
  commits. T1's reader re-issues `DETACH dro; ATTACH ... AS dro (READ_ONLY)`
  before every query to refresh. See
  [93-improvements-review.md §P1-1](./93-improvements-review.md).

## 5. Cross-references

- ← Depends on: [10](./10-data-model.md).
- → Consumed by: [20](./20-ingestion.md) (fills Q1 + producer registry),
  [12](./12-query-engine.md) (feeds Q2), [21](./21-rest-api.md) (Q3 endpoint).
- Norms: AGENTS.md § Async & Concurrency, § Error Handling (thiserror enums,
  `Result<T>`, no `unwrap`), § Safety (`#![forbid(unsafe_code)]`).
