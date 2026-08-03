# 12-query-engine: DSL → SQL, Capabilities B/F/J/S/P, Guardrails

Status: draft · Depends on: [10](./10-data-model.md), [11](./11-runtime-core.md)

## 1. Purpose

Turns a validated DSL AST ([10 §3](./10-data-model.md)) into **guarded,
parameterised** DuckDB SQL; implements the five-layer capability model (D7); and
enforces the guardrail budget that makes the read path safe to expose to an AI
agent (D2). Owns the sync/async decision (D14).

## 2. Interface

```text
pub struct QueryEngine { /* read-only DuckDB pool, guardrail config */ }
impl QueryEngine {
    pub async fn plan(&self, q: &SegmentQuery) -> Result<Plan>;        // EXPLAIN, cost, mode
    pub async fn run_sync(&self, q: &SegmentQuery) -> Result<SyncResult>;   // rows or row-cap error
    pub async fn materialize(&self, q: &SegmentQuery, camp: &str) -> Result<JobId>; // → Q2
}
```

`Plan` carries: chosen mode (sync/async), estimated rows, estimated bytes
scanned, the set of sources touched (for the `freshness` label, D5), and the
guardrail verdict (allow / reject-with-reason).

## 2a. Compile & guard flow

```text
  SegmentQuery
      │
      ▼  validate AST (I5: J must follow B/F narrowing)
  ┌────────────────────────────────────────┐
  │ Compiler (per capability)               │
  │  B → parameterised SQL over raw_*       │
  │  F → SQL over feature_wide_<family>     │
  │  J → wrapped CTE over survivor subquery │
  │  S → (phase 2) VSS top-k                │
  │  P → aggregate SELECT + baseline diff   │
  │  Exclude → anti-join suppression        │
  └──────────────┬─────────────────────────┘
                 │  SQL + params
                 ▼
  ┌────────────────────────────────────────┐
  │ Guardrail pre-flight (EXPLAIN)         │
  │  reject full-scan over big table       │
  │  reject J over unbounded survivor set  │
  │  enforce memory/threads/timeout/rows   │
  └──────────────┬─────────────────────────┘
                 │ mode = sync? ──no──▶ /jobs (materialize, Q2)
                 │ yes
                 ▼
  DuckDB execute (read-only) → SyncResult {rows capped} | Error::TooLarge
```

## 3. Invariants

- **I1 Parameterised only.** Every compiled SQL uses bound parameters; no
  string-interpolated user values (AGENTS.md § Injection Prevention). Lint:
  `cargo clippy -W clippy::format` on the compiler module.
- **I2 Guardrails are non-bypassable on the DSL path.** A query that fails
  pre-flight is rejected with a typed `Error::Guardrail { rule, limit }`,
  never run.
- **I3 SQL escape hatch is separately authorised.** Raw SQL (escape hatch, D2)
  goes through `run_sql_approved`, which requires an approval token + logs to
  audit; it still runs under the same guardrails but is never reachable from
  the DSL path.
- **I4 Freshness label always present.** Every result includes `freshness`
  derived from the sources touched (D5); a query spanning a CDC and a batch
  source reports the batch source's lag as the worst case.

## 4. Behaviour

- **Mode selection (D14).** `plan()` picks sync iff estimated rows ≤
  `sync_row_cap` (default 100k) **and** estimated cost ≤ `sync_cost_cap`
  (default 1 s). Otherwise it returns `Plan { mode: Async }` and the caller
  submits to `/jobs`. Materialisation is always async (writes DuckLake via Q2).
- **J (JIT) bounding (I5/D7).** A `Derive` node compiles to a CTE whose input
  is the survivor subquery; the compiler injects an inner `LIMIT` equal to the
  survivor count reported by the prior B/F stages' `plan`. If that survivor
  count exceeds `j_survivor_cap` (default 200k), the J node is rejected — the
  agent must narrow first or precompute the feature (F).
- **Guardrail defaults** (calibrate in [71](./71-performance-budgets.md)):
  `memory_limit` 8 GB, `threads` = cores, `statement_timeout` 30 s,
  `max_output_rows` 1 M, `max_bytes_scanned` per-query budget, in-flight
  `Semaphore` = cores.
- **Exclude semantics.** `Exclude { suppression.of(campaign) }` compiles to an
  anti-join against `suppression` for that campaign, considering frequency-cap
  rules ([20 §5](./20-ingestion.md)).
- **Errors.** Per AGENTS.md: a `thiserror` enum `QueryError` with variants
  `Guardrail`, `TooLarge`, `InvalidDsl`, `SurvivorUnbounded`, `Execution`; the
  `Execution` variant carries a `#[source]` DuckDB error. No `unwrap`/`expect`.

## 5. Cross-references

- ← Depends on: [10](./10-data-model.md), [11](./11-runtime-core.md).
- → Consumed by: [21](./21-rest-api.md) (`/query`, `/jobs`).
- ↔ S capability + VSS deferred: [90](./90-roadmap.md) phase 2.
- Norms: AGENTS.md § Safety (no panic on external input), § Performance,
  § Error Handling.
