# Roadmap — Incremental Delivery (stakeholder-facing)

## 0. Principles

- **Always shippable.** Every milestone leaves touched surfaces green on the
  Rust gate set (AGENTS.md § Toolchain).
- **Closure requires human confirmation.** A milestone closes only when (a)
  every exit criterion is met with evidence, (b) the closure note lists that
  evidence and any deferrals, and (c) a **human signs off**. "Closed with
  caveat" is not a valid state — a caveat means the milestone stays OPEN
  (roadmap §4).
- **Contract before consumer.** The data model + single-writer land before any
  DSL consumer; the Feature Store seam lands before any feature predicate.
- **Honest calibration.** One-developer estimates; pad for review/on-call. The
  Phase 0 spikes ([91](./91-impl-plan.md)) may move these numbers.

## 1. Build-order graph

```text
┌──────────────┐   ┌──────────────┐   ┌────────────────────┐
│ 00 PRD       │──▶│ 10 DataModel │──▶│ 11 RuntimeCore     │
│ 99 decisions │   │ 80 glossary  │   │ (single writer)    │
└──────────────┘   └──────┬───────┘   └─────────┬──────────┘
                          │                      │
                          ▼                      ▼
                   ┌──────────────┐   ┌────────────────────┐
                   │ 20 Ingestion │──▶│ 12 QueryEngine     │
                   │ (Q1+producers│   │ (B → F → J)        │
                   └──────┬───────┘   └─────────┬──────────┘
                          │                      │
                          ▼                      ▼
                   ┌──────────────┐   ┌────────────────────┐
                   │ 13 Semantic  │   │ 21 REST (sync/async│
                   │ (L0+L1)      │   │  + suppression)    │
                   └──────────────┘   └─────────┬──────────┘
                                                 │
                          ┌──────────────────────┴──────────────┐
                          ▼                                       ▼
                   ┌──────────────┐                      ┌──────────────┐
                   │ 70/71/72     │                      │ (phase 2)    │
                   │ sec/perf/test│                      │ S + ML prod  │
                   └──────────────┘                      └──────────────┘
```

## 2. Milestones (user-visible; exit criteria observable)

### M0 — Ingest one table, read it back over REST — ✅ CLOSED

**Specs touched**: 00, 10, 11, 20, 21. **Exit**: `POST /sources/onboard` ingests
a sample `raw_*` table into DuckLake (batch adapter); a trivial SQL-over-REST
returns rows with a `freshness` label. No DSL yet. Shipped in T1 (`e50e918`);
covered by `tests/e2e.rs` + storage unit tests (single-writer refusal,
read-only probe rejection, restart durability).

### M1 — Boolean/temporal DSL (B) for operators — ⚠️ OPEN — exit criterion met, awaiting human sign-off

> **Status**: exit criterion now MET with evidence — the read pool (#20) fixed
> the re-attach root cause and the bench gate (#25) enforces P50 < 1 s /
> P99 < 5 s, measuring P50 13–65 ms @ 50k rows. Closure requires ONLY the human
> sign-off (roadmap §4). Evidence table:
> [docs/research/perf-calibration.md](../docs/research/perf-calibration.md) §M1.

**Specs touched**: 12, 21. **Exit**: an agent composes "bought SKU A in 30d,
lapsed" via `/query` (sync) and gets guarded, parameterised results; P50 < 1 s
on the seeded corpus. Guardrails reject an over-budget query. Shipped in T2
(`90427f3`): DSL B + EXPLAIN pre-flight + guardrails (memory/threads/timeout/
rows/scan/semaphore) + freshness. **Perf (fixed, closing)**: the per-query DuckLake re-attach root cause (P1-1)
was removed by the read pool (#20); the bench gate (#25) now measures
P50 13–65 ms @ 50k rows and enforces P50 < 1 s / P99 < 5 s. The ≤50M-user
corpus re-run on a file-backed attach is still the final scale validation.

### M2 — Materialised audiences + delivery pull — ✅ CLOSED

**Specs touched**: 10 (snapshot), 12 (materialise), 21 (export). **Exit**: a
large segment materialises to `audience_snapshot` (async job) with frozen
features + hit_reason; `/audience/:id` returns a presigned Parquet URL; a
delivery client pulls and decodes it. Shipped in T3 (`7b7c916`) + hardened in
T7c (`8e01ed3`): atomic single-INSERT snapshot, /jobs + /audience + presigned
HMAC export, e2e parquet decode; compaction tuned (merge_adjacent_files,
file-count test).

### M3 — Feature Store (F) + semantic layer (L0/L1) — ✅ CLOSED

**Specs touched**: 13, 20 (producers). **Exit**: a SQL producer writes
`cadence_regularity`; `Feature` predicate filters on it ("periodic buyers"
example resolves end-to-end: onboard → producer → `Feature` compile → query →
materialise); L0 profiles a new table and L1 retrieval returns bounded
candidate columns to the agent. G5 (new table queryable < 30 min) holds. Graded
per-source freshness (`worstSource`) is reported on every query. All covered by
`tests/e2e.rs` M3 exit tests + per-crate unit tests; full gate green.

### M4 — Closed suppression loop — ✅ CLOSED

**Specs touched**: 20 §5, 21 (`/suppression`). **Exit**: a delivery system
POSTs suppression; the next snapshot for that campaign excludes those users;
frequency-cap rule enforced. G4 holds (zero re-targeting in regression).
Shipped in #7 (`5bf7c05`): idempotent `/suppression` (Q3), `Exclude` anti-join,
config-driven rules (per-campaign no-repeat + global frequency cap); e2e
asserts suppressed users absent from rerun + snapshot, cap enforced.

### M5 — Hardening: J + P + budgets + security — ⚠️ OPEN — exit criteria NOT met

> **Status**: reverted from CLOSED (2025-08) — exit criteria "perf budgets met"
> and "G2/G3 hold" NOT met (measured 2.5–15 s P50 @ 50k rows; AC6 tenant
> isolation deferred). See
> [docs/research/spec-gap-analysis.md](../docs/research/spec-gap-analysis.md).
> Closure requires: the perf fix (P1-1), tenant isolation (issue #10 AC6), and
> human confirmation (roadmap §4).

**Specs touched**: 12 (J/P), 70, 71, 72. **Exit**: JIT `Derive` over survivors
works and is bounded; `Characterize` emits comparative profiles; perf budgets
met; security checklist green. G2/G3 hold. Shipped in #8/#9 (`5bf7c05`) +
#10 (`8e01ed3`/`f089593`/`44fcccb`): Derive with measured (non-bypassable)
survivor cap, Characterize segment-vs-baseline profiles, boundary lint gate
(`make lint-boundary`), redacting Debug + log test, constant-time presign +
expiry + access log. **Perf (fixed, closing)**: the re-attach root cause was removed by the read
pool (#20); the bench gate (#25) enforces P50 < 1 s / P99 < 5 s and measures
P50 13–65 ms @ 50k rows. **Security (fixed, closing)**: AC6 tenant isolation
is enforced by construction — the compiler injects the caller's tenant into
every SQL; `test_should_isolate_tenants_by_construction` proves cross-tenant
reads/snapshots/suppression are impossible (#22). 71 §4 snapshot expiry is
tracked separately as #17 (upstream DuckLake binder blocker) — accepted as a
**non-blocking** deferral for M5 closure (its own ticket stays open).

### (Phase 2 — not v1) S similarity + ML producers

**Exit**: first VSS top-k lookalike; first ML propensity score registered as a
producer, queryable as a `Feature` predicate with **no** runtime layer change
(D8/D9).

## 3. Calendar shape (indicative, one developer)

| Milestone | Indicative effort | Notes |
| --------- | ----------------- | ----- |
| Phase 0 spikes | 1–2 wk | MERGE/VSS/CDC/bench |
| M0 | 1–2 wk | spine + DuckLake attach |
| M1 | 2–3 wk | DSL B + compiler + guardrails |
| M2 | 1–2 wk | snapshot + jobs + export |
| M3 | 2–3 wk | feature store + profiler/RAG |
| M4 | 1–2 wk | suppression + rules |
| M5 | 2–3 wk | J/P + hardening |

Phases 0–M5 ≈ 10–17 weeks for one developer; parallelism collapses M2/M4 once
the writer spine (M0) is stable. **Re-calibrate after Phase 0.**

## 4. Milestone closure process (binding)

1. **Evidence before status.** A milestone closes only by human sign-off; the
   closing note must map every exit criterion to concrete evidence (test name,
   bench number, spec §).
2. **No "closed with caveat".** If any exit criterion is unmet, the milestone
   stays OPEN with the gap named. The gap is either fixed, or the exit
   criterion is formally re-scoped in the spec — both require human sign-off.
3. **Deferrals need sign-off.** Anything deferred out of a milestone
   (`specs/93`) is listed in the closure note and explicitly accepted by the
   human; deferred items are re-verified at every subsequent milestone review.
4. **Roadmap status is the single source of truth.** README/docs claims mirror
   roadmap status; a stale "closed" claim anywhere is a defect.

## 5. Cross-references

- Pair 1:1 with [91-impl-plan.md](./91-impl-plan.md) (different ordering —
  roadmap is user-feature, impl-plan is dependency).
- Decisions shaping scope: D8/D15 ([99](./99-key-decisions.md)).
