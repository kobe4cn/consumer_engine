# Specs Index — Consumer Engine

AI-agent-facing audience filtering engine (Rust + DuckDB/DuckLake). This index
is the entry point: read top-to-bottom for the design, or follow the
build-order graph for the implementation path.

The 16 decisions from the design grilling (2025-08-03) live in
[99-key-decisions.md](./99-key-decisions.md); every spec cites the decisions it
depends on. File *types* (per AGENTS.md) are noted in the Type column.

## File table

| # | File | Type | Purpose |
| - | ---- | ---- | ------- |
| 00 | [00-prd.md](./00-prd.md) | prd | Vision, users, goals/non-goals, success metrics, binding naming |
| 10 | [10-data-model.md](./10-data-model.md) | design | DuckLake tables, DSL AST, REST wire shapes, invariants |
| 11 | [11-runtime-core.md](./11-runtime-core.md) | design | Engine lifecycle, actor topology, **single writer** (D3) |
| 12 | [12-query-engine.md](./12-query-engine.md) | design | DSL→SQL compiler, B/F/J/S/P capabilities, guardrails, sync/async |
| 13 | [13-semantic-layer.md](./13-semantic-layer.md) | design | L0 Profiler (onboarding) + L1 Intent RAG (query-time) |
| 20 | [20-ingestion.md](./20-ingestion.md) | design | Source adapters, producer registry, materialisation, MERGE limits |
| 21 | [21-rest-api.md](./21-rest-api.md) | design | REST surface, auth/tenancy, payload modes, external delivery contract |
| 61 | [61-crates-and-features.md](./61-crates-and-features.md) | design | Workspace layout, crate map, dependency direction, feature flags |
| 70 | [70-security.md](./70-security.md) | design | Threat model, PII boundary, guardrails-as-DoS-defense |
| 71 | [71-performance-budgets.md](./71-performance-budgets.md) | design | Freshness SLA, latency/memory guardrail numbers |
| 72 | [72-testing-strategy.md](./72-testing-strategy.md) | design | Test pyramid, fixtures, load-bearing tests, CI gates |
| 80 | [80-glossary.md](./80-glossary.md) | doc | Segment vs snapshot; B/F/J/S/P; producer; freshness; trust boundaries |
| 90 | [90-roadmap.md](./90-roadmap.md) | roadmap | Milestones M0–M5 (+ phase 2), exit criteria, calendar |
| 91 | [91-impl-plan.md](./91-impl-plan.md) | impl-plan | Phase 0 (spikes) → Phase 6, dependency-ordered, effort |
| 92 | [92-gap-closure-plan.md](./92-gap-closure-plan.md) | impl-plan | 差距补缺：P0 spike → P6 关闭验证；P1-1 读池先修（closes M1/M5, 人确认制） |
| t3 | [t3-materialize-plan.md](./t3-materialize-plan.md) | impl-plan (detail) | T3 materialise — Phase A done (`09cb979`); Phases B–H pending (expands 91 §6) |
| t4 | [t4-feature-store-semantic-plan.md](./t4-feature-store-semantic-plan.md) | impl-plan (detail) | T4 Feature Store + semantic layer — Phase 4 detail (expands 91 §7; closes M3) |
| 93 | [93-improvements-review.md](./93-improvements-review.md) | review | Deferred-findings backlog (impl skill appends here; 92 picks items up per phase) |
| 99 | [99-key-decisions.md](./99-key-decisions.md) | decisions | D1–D16, alternatives + why + reverse pointers |

## Reading order

1. **[00](./00-prd.md)** — what & why (problem, vision, goals, non-goals).
2. **[80](./80-glossary.md)** + **[99](./99-key-decisions.md)** — the vocabulary
   and the 16 load-bearing choices everything else inherits.
3. **[10](./10-data-model.md)** — the shapes (tables, DSL, wire).
4. **[11](./11-runtime-core.md)** → **[20](./20-ingestion.md)** → **[12](./12-query-engine.md)**
   → **[13](./13-semantic-layer.md)** → **[21](./21-rest-api.md)** — the build
   order of components.
5. **[70](./70-security.md)** / **[71](./71-performance-budgets.md)** / **[72](./72-testing-strategy.md)**
   — cross-cuts read alongside the components.
6. **[90](./90-roadmap.md)** (stakeholder) + **[91](./91-impl-plan.md)**
   (engineer) — what lands when.

## Build-order graph

```text
┌──────────────┐   ┌──────────────┐   ┌────────────────────┐
│ 00 PRD       │──▶│ 10 DataModel │──▶│ 11 RuntimeCore     │
│ 99 decisions │   │ 80 glossary  │   │ (single writer)    │
└──────────────┘   └──────┬───────┘   └─────────┬──────────┘
                          │                      │
                          ▼                      ▼
                   ┌──────────────┐   ┌────────────────────┐
                   │ 20 Ingestion │──▶│ 12 QueryEngine     │
                   │ Q1+producers │   │ B → F → J (+P)     │
                   └──────┬───────┘   └─────────┬──────────┘
                          │                      │
                          ▼                      ▼
                   ┌──────────────┐   ┌────────────────────┐
                   │ 13 Semantic  │──▶│ 21 REST            │
                   │ L0+L1        │   │ sync/async+suppr.  │
                   └──────────────┘   └─────────┬──────────┘
                                                 │
                          ┌──────────────────────┴──────────────┐
                          ▼                                       ▼
                   ┌──────────────┐                      ┌──────────────┐
                   │ 70/71/72     │                      │ phase 2:     │
                   │ sec/perf/test│                      │ S + ML prod  │
                   └──────────────┘                      └──────────────┘
```

## Decision spine (summary)

Storage=DuckLake · Writer=single `IngestionActor` (3 queues) · Reader=DuckDB
read-only · Contract=DSL-primary + SQL escape hatch · Capabilities=B/F/J/S/P ·
Predictions enter via Feature Store seam (ML deferred) · Audiences=always
materialised · PII-free pseudonymous IDs · Transport=REST · Scheduling & ML
training out of v1 scope · Rust core + Python/TS agent. Full rationale:
[99](./99-key-decisions.md).
