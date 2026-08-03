# Implementation Plan — Dependency-Ordered Build (engineer-facing)

## 0. Readiness assessment

**Ready**: PRD, data model, capability model (B/F/J/S/P), single-writer
topology, REST surface, ML-ready Feature Store seam, 16 key decisions — all
locked from the grilling session.

**Phase 0 spikes — RESOLVED** (research skill, 2025-08-03; memos in
[../docs/research/](../docs/research/)):

- ✅ DuckLake MERGE limits + no-PK + throughput — [spike-ducklake-merge.md](../docs/research/spike-ducklake-merge.md). [10]/[20] amended.
- ✅ micro-batch + compaction file-count — [spike-microbatch-compaction.md](../docs/research/spike-microbatch-compaction.md). [71] flush raised to 30 s/50 k.
- ✅ VSS vector type (`FLOAT[N]`) + HNSW engagement — [spike-duckdb-vss.md](../docs/research/spike-duckdb-vss.md). [10] vec tables split out.
- ✅ CDC adapter — [survey-cdc-adapter.md](../docs/research/survey-cdc-adapter.md). `ingestion-cdc` default off.
- ✅ `rust-toolchain.toml` pinned to 1.97.0.

**Residual open risks** (not Phase-0 blockers; tracked as in-phase follow-ups):
- Object-storage per-file latency (R1, spike-microbatch-compaction) — bench on
  target storage before locking flush interval; do during Phase 1 ingestion.
- Postgres-catalog multi-writer contention (R3, spike-ducklake-merge) —
  irrelevant while single-writer (D3); revisit only if scaling writers.
- Production-scale HNSW build (R2, spike-duckdb-vss) — phase-2 concern.

**Phase 1 is unblocked.**

## 1. Why dependency order ≠ feature order

- The **single writer + read-only pool** (11) must precede any DSL consumer
  (12), even though the first *user-visible* feature is a filter (M1). Land
  the contract before the consumer.
- The **Feature Store seam** (10/20) lands before the first feature predicate
  (M3), so F is never retrofitted — and so phase-2 ML plugs in without runtime
  change (D8/D9). Pay the design cost once.
- **Guardrails** (12/71) land *with* the first query path, not after — they are
  the DoS defense (70), and bolting them on later means a window of unsafe
  exposure.

## 2. Estimated total effort

≈ 10–17 weeks, one developer; collapses with parallelism after M0. See
[90 §3](./90-roadmap.md). Re-baseline after Phase 0.

## 3. Phase 0 — risk retirement (1–2 wk)

| #  | Deliverable | Status |
| -- | ----------- | ------ |
| 0.1 | DuckLake MERGE limits + throughput | ✅ [spike-ducklake-merge.md](../docs/research/spike-ducklake-merge.md); [20 §4](./20-ingestion.md) updated |
| 0.2 | micro-batch + compaction file-count | ✅ [spike-microbatch-compaction.md](../docs/research/spike-microbatch-compaction.md); [71 §4](./71-performance-budgets.md) updated |
| 0.3 | VSS vector type + HNSW engagement | ✅ [spike-duckdb-vss.md](../docs/research/spike-duckdb-vss.md); [10](./10-data-model.md) vec tables split out |
| 0.4 | CDC adapter feasibility | ✅ [survey-cdc-adapter.md](../docs/research/survey-cdc-adapter.md); `ingestion-cdc` default off |
| 0.5 | Pin toolchain | ✅ `rust-toolchain.toml` @ 1.97.0 |

**Exit gate**: ✅ met — all memos committed, specs amended.
**Residual**: object-storage latency bench slots into Phase 1 ingestion (R1).

**Verification**: `make check-agent-sync`; memos proofread; no Rust gate (no
engine code yet — only `rust-toolchain.toml` added).

## 4. Phase 1 — foundation spine (closes M0)

| #   | Task | Spec | Effort |
| --- | ---- | ---- | ------ |
| 1.1 | `consumer_engine-core`: error enums (thiserror), config (yaml, config crate), domain primitives | [00](./00-prd.md), [10](./10-data-model.md) | 2d |
| 1.2 | `consumer_engine-storage`: DuckLake attach (read + write), table DDL for `raw_*`/`feature_store`/`audience_snapshot`/`suppression`/`semantic_catalog` | [10](./10-data-model.md) | 3d |
| 1.3 | `consumer_engine-execution`: DuckDB read-only pool + EXPLAIN/cost helper | [11 I2](./11-runtime-core.md), [12](./12-query-engine.md) | 2d |
| 1.4 | `consumer_engine-ingestion`: `IngestionActor` skeleton + single-writer enforcement + micro-batcher + batch adapter + compaction task | [11](./11-runtime-core.md), [20](./20-ingestion.md) | 4d |
| 1.5 | `consumer_engine-ingress`: axum health + `/sources/onboard` + trivial SQL-over-REST; auth/tenancy stubs | [21](./21-rest-api.md) | 2d |
| 1.6 | `consumer_engine-server`: wire actors, load config, serve | [11](./11-runtime-core.md) | 1d |

**Exit criteria (M0)**: onboard a sample table → ingest → read over REST with
`freshness` label; `test_should_never_allow_second_writer` +
`test_should_assert_query_path_is_read_only` pass.
**Verification**: `cargo build`, `cargo nextest run -p consumer_engine-storage
-p consumer_engine-ingestion`, `cargo clippy -- -D warnings` on changed crates.

## 5. Phase 2 — Boolean/temporal DSL + guardrails (closes M1)

| #   | Task | Spec | Effort |
| --- | ---- | ---- | ------ |
| 2.1 | DSL AST + parser + validator (I5: J follows B/F) | [10 §3](./10-data-model.md) | 2d |
| 2.2 | Compiler: B (Filter/Temporal/SetOp/Distinct/Exclude) → parameterised SQL | [12](./12-query-engine.md) | 3d |
| 2.3 | Guardrails: EXPLAIN pre-flight, memory/threads/timeout/rows, Semaphore | [12 §4](./12-query-engine.md), [71](./71-performance-budgets.md) | 2d |
| 2.4 | Sync `/query` + `freshness` label + typed errors | [21](./21-rest-api.md), [12 I4](./12-query-engine.md) | 2d |

**Exit criteria (M1)**: "bought SKU A in 30d, lapsed" composes + runs sync,
P50 < 1 s; over-budget query rejected; `test_should_parameterise_all_user_values`
+ `test_should_reject_query_over_memory_limit` pass.
**Verification**: `cargo nextest run -p consumer_engine-query`; full gate on
touched crates.

## 6. Phase 3 — materialised audiences + delivery pull (closes M2)

> **Detailed plan:** [`t3-materialize-plan.md`](./t3-materialize-plan.md) (file-by-
> file, 8 phases A–H). **Phase A done** (`09cb979`: catalog aliases +
> `SnapshotSpec` + storage snapshot methods); resume from Phase B.

| #   | Task | Spec | Effort |
| --- | ---- | ---- | ------ |
| 3.1 | Async job model: `/jobs`, `/jobs/:id`, Q2 materialise path | [12 §4](./12-query-engine.md), [21](./21-rest-api.md) | 2d |
| 3.2 | Snapshot write (atomic, hit_reason, frozen features) + I3 time-bound check | [10 I2/I3](./10-data-model.md), [20 I4](./20-ingestion.md) | 2d |
| 3.3 | `/audience/:id` presigned URL + `/export?format=parquet` | [21 §4](./21-rest-api.md) | 2d |

**Exit (M2)**: large segment → snapshot (I2) → presigned Parquet → decoded by a
delivery test client. `test_should_materialise_snapshot_atomically_with_hit_reason`
passes.

## 7. Phase 4 — Feature Store + semantic layer (closes M3)

> **Status: DONE — M3 closed.** Detailed plan: [`t4-feature-store-semantic-plan.md`](./t4-feature-store-semantic-plan.md)
> (phases A–J). Phase A (core types + storage DDL/writers) landed first; phases
> B–J (producer trait/registry + cadence producer, `Feature` DSL op, semantic
> crate L0/L1, graded freshness, REST `/catalog` + `/producers/run`, server
> wiring, M3 exit tests) completed in this pass. All 7 M3 exit criteria are
> covered by tests; the full Rust gate (`build`, `test`, `+nightly fmt`,
> `clippy -- -D warnings`, `audit`, `deny check`) is green. Findings/deferrals
> are recorded in [93 §From Phase 4](./93-improvements-review.md).

| #   | Task | Spec | Effort |
| --- | ---- | ---- | ------ |
| 4.1 | Producer trait + registry + `feature_store` write path + wide pivot views | [20 §2](./20-ingestion.md), [D9](./99-key-decisions.md) | 3d |
| 4.2 | SQL producer example (`cadence_regularity`) with point-in-time correctness (I3) | [20 I3](./20-ingestion.md) | 2d |
| 4.3 | L0 Profiler (bounded sample, PII redaction, description+embed) + onboarding endpoint | [13](./13-semantic-layer.md) | 3d |
| 4.4 | L1 Intent RAG retrieval (`/catalog`) + `Feature` DSL predicate | [13](./13-semantic-layer.md), [12](./12-query-engine.md) | 2d |

**Exit (M3)**: "periodic buyers" example resolves end-to-end; a new table is
queryable < 30 min (G5); `test_should_report_worst_source_freshness` passes.

## 8. Phase 5 — closed suppression loop (closes M4)

| #   | Task | Spec | Effort |
| --- | ---- | ---- | ------ |
| 5.1 | `/suppression` writeback (idempotent) → Q3 | [21](./21-rest-api.md), [20](./20-ingestion.md) | 2d |
| 5.2 | `Exclude` compile (anti-join) + rule engine (per-campaign + frequency cap) | [12 §4](./12-query-engine.md), [20 §5](./20-ingestion.md) | 2d |

**Exit (M4)**: suppressed users absent from re-run; frequency cap enforced
(`test_should_exclude_suppressed_users_from_rerun`,
`test_should_enforce_frequency_cap`).

## 9. Phase 6 — hardening: J + P + budgets + security (closes M5)

| #   | Task | Spec | Effort |
| --- | ---- | ---- | ------ |
| 6.1 | `Derive` (J) over survivor subquery + `j_survivor_cap` enforcement | [12 I5](./12-query-engine.md), [71 §5](./71-performance-budgets.md) | 2d |
| 6.2 | `Characterize` (P) comparative profile | [12](./12-query-engine.md) | 2d |
| 6.3 | Perf calibration: lock guardrail numbers from bench harness | [71](./71-performance-budgets.md), [72](./72-testing-strategy.md) | 3d |
| 6.4 | Security checklist: boundary lints, redacting Debug, constant-time tokens, presigned expiry | [70](./70-security.md) | 2d |

**Exit (M5)**: G2/G3 hold; `cargo audit`/`cargo deny check` clean; full gate
green.

## 10. What makes this order *correct*, not just plausible

- **Writer before reader** — DuckLake's single-coordination-point model
  dictates the writer spine is foundational; everything reads from it.
- **Guardrails with the first query** — they are security, not polish; a later
  add means an unsafe window.
- **Feature Store seam before any feature** — D8/D9 hinge on this; retrofitting
  it later is a rewrite of the compiler and the ingestion producer path.
- **Suppression after snapshots** — Exclude keys off materialised snapshots
  (D10), so the loop can only close once M2 exists.

## 11. Cross-references

- Roadmap (user-feature order): [90-roadmap.md](./90-roadmap.md).
- Phase 0 hands off to the **research** skill; Phase 1 onward to the **impl**
  skill.
