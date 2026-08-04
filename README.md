# Consumer Engine

An **AI-agent-facing audience filtering engine** written in Rust: a marketing
operator describes a target audience in plain language, an agent composes a
structured DSL, and the engine compiles it to **guarded** DuckDB SQL over a
**DuckLake** house, materialising versioned audience snapshots with audit +
suppression.

**Milestones M0–M5 are CLOSED** (see [specs/90-roadmap.md](specs/90-roadmap.md));
the PRD and every child issue are closed. The v1 capability set:

- **B** — Boolean/temporal-relational predicates over raw events.
- **F** — predicates over a precomputed per-user Feature Store.
- **J** — just-in-time derived metrics over the survivor set, under a measured
  non-bypassable cap.
- **P** — comparative characterisation of a segment vs the whole population.
- **S** (phase 2) — similarity/lookalike, out of v1 scope.

## Quick start

```sh
# build + test the whole workspace (all features incl. the optional HTTP LLM)
cargo build --workspace
cargo test --workspace --all-features

# run the server (default config; no auth — dev only)
cargo run -p consumer_engine-server
# or with a config file:  cargo run -p consumer_engine-server -- --config config.yaml

# boundary lint (no unwrap/indexing/panic/expect on the lib surfaces)
make lint-boundary

# query-latency calibration harness (scale via CE_SCALE_ROWS)
make bench-queries
```

Then talk to `http://127.0.0.1:8080`:

```sh
# health
curl localhost:8080/healthz

# onboard a source table (auto-profiled into the semantic catalogue)
curl -X POST localhost:8080/sources/onboard -H 'content-type: application/json' -d '{
  "system":"erp","entity":"orders","columns":["user_id","sku"],
  "rows":[["u1","A"],["u2","B"]]}'

# a DSL query — "users who bought SKU A"
curl -X POST localhost:8080/query -H 'content-type: application/json' -d '{
  "dsl": {"source":{"system":"erp","entity":"orders"},"key":"user_id",
          "ops":[{"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}]}}'
```

> **Production MUST set `auth_token`** — a tokenless engine lets any caller
> mint presigned exports (IDOR). See [docs/deployment.md](docs/deployment.md).

## Documentation

| What | Where |
| ---- | ----- |
| The design contract (PRD, data model, DSL AST, REST, security, budgets) | [specs/](specs/) — start at [specs/index.md](specs/index.md) |
| Component guidance — develop / use / test / deploy | [docs/](docs/) — [docs/index.md](docs/index.md) |
| Research memos (DuckLake spikes, CDC survey, perf calibration) | [docs/research/](docs/research/) |
| Issue tracker (all v1 issues closed) | GitHub `kobe4cn/consumer_engine` |
| Rust gate / lint / bench automation | [Makefile](Makefile) |

## Workspace layout

```text
crates/core         types, error model, config, domain primitives (dep root)
crates/storage      the single writable DuckLake handle + table DDL/writers
crates/execution    read-only DuckDB reader (single-threaded, channel-driven)
crates/ingestion    the writer actor (Q1/Q2/Q3) + FeatureProducer + cadence
crates/query        DSL AST/parser/compiler + B/F/J/P + guardrails + engine
crates/semantic     L0 Profiler + L1 Intent RAG + LLM/embedding clients
crates/ingress      axum REST: the single trust boundary (authN, validation)
apps/server         binary wiring everything from EngineConfig
```

Dependency direction is acyclic with `core` at the root (specs/11 §2).

## Status & known limitations

- **Performance targets are NOT met at scale**: measured B/F/J/P P50
  2.5–15 s at 50k rows, dominated by the per-query DuckLake re-attach (P1-1).
  Guardrail budgets stay as locked targets; the fix path (read-connection pool,
  file-backed DuckLake) is tracked in
  [docs/research/perf-calibration.md](docs/research/perf-calibration.md).
- Deferred items are logged in
  [specs/93-improvements-review.md](specs/93-improvements-review.md): snapshot
  point-in-time bounding (T4-I3), catalogue freshness warning (T5-I5),
  multi-tenant schema (T7c-TENANT — authN itself is implemented), CDC adapter
  (P3-4), DuckDB server-side statement timeout (unavailable in this build).
