# Key Decisions

Permanent record of load-bearing design choices. Supersede with a new D-id;
never edit in place. Each entry has alternatives, the *why*, and reverse
pointers to the specs that depend on it. These decisions originated in the
grilling session that produced this spec set (2025-08-03).

## D1 — DuckLake as the lakehouse format

- **Context**: how raw/feature/snapshot data is stored on object storage.
- **Alternatives**: plain Parquet (no upsert/delete, painful); Iceberg/Delta
  (mature but heavier, separate stack); "DuckDB Lake" (not a real format).
- **Decision**: DuckLake — Parquet data on object storage + a relational
  catalogue (Postgres for multi-process coordination). Native DuckDB read/write,
  ACID via catalogue transactions + MVCC, time-travel, logical deletes.
- **Why**: single-writer-friendly catalogue model matches our `IngestionActor`;
  DuckDB reads it natively with no extra engine; upsert/delete supported via
  MERGE (with documented limits, see [20](./20-ingestion.md) §4).
- **Pinned by**: [10](./10-data-model.md), [11](./11-runtime-core.md), [20](./20-ingestion.md).
- **Date**: 2025-08-03.

## D2 — DSL-primary contract, SQL is an approved escape hatch

- **Context**: how the AI agent expresses filtering intent.
- **Alternatives**: (a) free LLM→SQL (max flexibility, max injection/DoS/
  hallucination, operators can't review); (b) pure structured primitives only
  (safe but can't express the long tail).
- **Decision**: a structured **DSL** is the happy path; raw SQL is allowed only
  behind explicit human approval + guardrails + audit logging.
- **Why**: operator is non-technical (cannot vet SQL); safety and auditability
  outrank long-tail flexibility on the common path; the escape hatch preserves
  escape valve without making it the default.
- **Pinned by**: [12](./12-query-engine.md), [21](./21-rest-api.md).
- **Date**: 2025-08-03.

## D3 — Single `IngestionActor` writer, three queues

- **Context**: who may write DuckLake; concurrency model.
- **Alternatives**: multi-writer direct-to-catalogue (DDL/compaction still need
  a coordinator → more complex); Quack remote server (beta until ~v2.0/2026).
- **Decision**: one Rust actor owns the sole write connection to the Postgres
  catalogue. Three prioritised queues feed it: Q1 source ingest, Q2 snapshot
  materialisation, Q3 suppression writeback.
- **Why**: DuckLake is single-coordination-point by design; appends/disjoint
  writes don't conflict so a single serialised writer has high throughput; one
  writer = one place to enforce schema/onboarding/versioning (D4).
- **Pinned by**: [11](./11-runtime-core.md), [20](./20-ingestion.md) (§5 suppression rules).
- **Date**: 2025-08-03.

## D4 — Agents never DDL at runtime; schema onboarding is controlled/versioned

- **Context**: the grilling initially floated "agent autonomously maintains
  tables". This is forbidden at runtime.
- **Alternatives**: runtime agent-driven DDL (unbounded, unauditable, races the
  writer).
- **Decision**: schema changes (new source table, add column) happen only via an
  explicit onboarding flow run by the L0 Profiler, versioned and approved. The
  query path is strictly read-only.
- **Why**: keeps the writer boundary intact (D3); makes schema changes
  reviewable and reversible; matches DuckLake's single-writer model.
- **Pinned by**: [13](./13-semantic-layer.md), [11](./11-runtime-core.md).
- **Date**: 2025-08-03.

## D5 — Freshness is graded per source; engine is CDC-capable with batch fallback

- **Context**: how fresh is "recent" in "bought X recently".
- **Alternatives**: uniform real-time (impossible for batch-only sources);
  batch-only (loses CDC sources' freshness).
- **Decision**: engine accepts CDC (Debezium/Kafka) and batch. Freshness is
  **per source** and **surfaced to the operator** on every result.
- **Why**: physical reality — a batch source's freshness is capped by its batch
  interval regardless of engine capability; hiding this misleads operators.
- **Pinned by**: [20](./20-ingestion.md), [71](./71-performance-budgets.md).
- **Date**: 2025-08-03.

## D6 — Micro-batch + compaction are mandatory in `IngestionActor`

- **Context**: minute-level CDC into Parquet.
- **Alternatives**: per-row writes (small-file explosion).
- **Decision**: writes are攒-batched by N seconds/N rows; a scheduled
  compaction job merges small files and expires old snapshots.
- **Why**: Parquet immutability + high CDC rate = unbounded small files without
  this; DuckLake's maintenance primitives exist precisely for this.
- **Pinned by**: [11](./11-runtime-core.md), [20](./20-ingestion.md).
- **Date**: 2025-08-03.

## D7 — Five-layer capability model B / F / J / S / P

- **Context**: what kinds of filtering the engine offers beyond raw Boolean.
- **Alternatives**: "everything non-Boolean is similarity" (the grilling's
  initial mis-aim; conflated lapse/cadence with lookalike).
- **Decision**: B (Boolean/temporal, unbounded), F (precomputed feature
  predicates, bounded catalogue), J (JIT metric over survivors, guard-railed),
  S (similarity/lookalike, phase 2), P (comparative characterisation).
- **Why**: cleanly separates free relational composition (B) from everything
  that costs precompute or guarded runtime (F/J/S); the operator's "periodic
  buyers" example is B+F(+J), **not** similarity — this model prevents
  misclassification.
- **Pinned by**: [12](./12-query-engine.md), [80](./80-glossary.md).
- **Date**: 2025-08-03.

## D8 — ML training pipeline deferred; predictions enter via F (Feature Store)

- **Context**: "find users likely to buy A" / "next likely product".
- **Alternatives**: build full ML platform in v1 (doubles scope/team).
- **Decision**: v1 ships **no** ML training. The Feature Store's producer
  interface is designed ML-ready; phase-2 model scorers register as producers.
- **Why**: the runtime/query layer is prediction-agnostic — a propensity score
  and a count are both Feature Store columns; the only thing ML adds is an
  offline training subsystem, which is a separable project.
- **Pinned by**: [10](./10-data-model.md), [20](./20-ingestion.md), [90](./90-roadmap.md).
- **Date**: 2025-08-03.

## D9 — Feature Store is the single seam; uniform producer interface

- **Context**: how "smarter-than-SQL" signals enter.
- **Decision**: `feature_store` is written only by registered producers emitting
  `(user_id, feature_name, value, as_of_ts)`. SQL producers in v1, ML producers
  in phase 2 — identical contract.
- **Why**: one seam = one place to enforce point-in-time correctness, lineage,
  and versioning; adding prediction later changes zero runtime code.
- **Pinned by**: [10](./10-data-model.md), [20](./20-ingestion.md).
- **Date**: 2025-08-03.

## D10 — Audiences are always materialised to `audience_snapshot`

- **Context**: lazy virtual segment vs materialised snapshot.
- **Alternatives**: lazy re-evaluation (un-auditable; "who was targeted" is
  non-reproducible; breaks suppression feedback).
- **Decision**: every audience consumed downstream is materialised.
- **Why**: marketing/compliance require reproducible "who was selected, when,
  why"; suppression exclusion (D-loop) keys off snapshots.
- **Pinned by**: [10](./10-data-model.md), [12](./12-query-engine.md).
- **Date**: 2025-08-03.

## D11 — Snapshot rows carry frozen features + hit reason

- **Context**: what a snapshot row stores beyond the user ID.
- **Alternatives**: ID-only (cheaper, useless for review).
- **Decision**: each row stores the user ID **plus** the feature values at
  selection time **plus** the `hit_reason` (which DSL predicate selected them).
- **Why**: post-campaign analysis and future model backtesting both need "why
  this user, at that moment"; storing it once is cheaper than reconstructing.
- **Pinned by**: [10](./10-data-model.md).
- **Date**: 2025-08-03.

## D12 — PII-free; pseudonymous user IDs only

- **Context**: does the engine hold personal data.
- **Decision**: engine stores/returns only pseudonymous `user_id`; no
  email/phone/name. PII resolution is a source/delivery-system concern.
- **Why**: minimises compliance surface; shrinks blast radius of any leak;
  simplifies the trust model.
- **Pinned by**: [70](./70-security.md), [10](./10-data-model.md).
- **Date**: 2025-08-03.

## D13 — REST (JSON / Parquet / presigned URL) over gRPC + Arrow Flight

- **Context**: transport between agent and engine.
- **Alternatives**: gRPC + Arrow Flight (zero-copy streaming of large results).
- **Decision**: plain REST. Small results → JSON; large/materialised results →
  Parquet bytes or a presigned object-storage URL (D10 guarantees big results
  are materialised, never streamed inline).
- **Why**: because big results are always materialised to the lake, the
  "frequent large live stream" case that justifies Flight does not exist here;
  REST wins on simplicity, tooling, debuggability. Revisit if that assumption
  breaks.
- **Pinned by**: [21](./21-rest-api.md).
- **Date**: 2025-08-03.

## D14 — Query dual-mode: sync under threshold, else async job

- **Context**: synchronous reply vs job submission.
- **Decision**: estimated-fast + small → synchronous; otherwise async job
  (`POST /jobs` → poll/callback), which is also how materialisation runs.
- **Why**: keeps the common path interactive while preventing long/heavy
  queries from blocking; materialisation is inherently async (writes DuckLake).
- **Pinned by**: [12](./12-query-engine.md), [21](./21-rest-api.md).
- **Date**: 2025-08-03.

## D15 — Scheduling and ML training are out of engine scope (v1)

- **Context**: scope boundary.
- **Decision**: send-time scheduling lives in the external delivery system
  (NG1); ML model training is phase 2 (NG2/D8).
- **Why**: the engine's job is audience selection + suppression loop, not
  orchestration or ML; keeping these out preserves a buildable v1.
- **Pinned by**: [00](./00-prd.md), [90](./90-roadmap.md).
- **Date**: 2025-08-03.

## D16 — Rust core + Python/TS agent orchestration split

- **Context**: language boundary.
- **Decision**: engine core in Rust (all crates in [61](./61-crates-and-features.md));
  agent/LLM orchestration in Python or TS, calling via REST. PyO3 reserved for
  local dev/notebook embedding only.
- **Why**: LLM/agent ecosystem is overwhelmingly Python; a pure-Rust agent
  pays continual tax for no gain. The engine benefits from Rust (perf, safety,
  DuckDB in-process); the split lets each side use its best tools.
- **Pinned by**: [61](./61-crates-and-features.md), [21](./21-rest-api.md).
- **Date**: 2025-08-03.
