# 61-crates-and-features: Workspace Layout & Crate Map

Status: draft · Depends on: [00](./00-prd.md)

## 1. Purpose

Pins the workspace shape so every other spec can reference crates by name.
Aligns with the existing scaffold (`crates/*`, `apps/*`, `consumer_engine-<short>`
naming, workspace deps for anyhow/serde/serde_json/thiserror/tokio) and
AGENTS.md (Rust 2024, `#![forbid(unsafe_code)]`, workspace deps, lint set).

## 2. Crate map

```text
consumer_engine-core        crates/core        shared types, domain primitives, error enums, config
consumer_engine-storage     crates/storage     DuckLake attach, table DDL, snapshot/suppression/catalog store
consumer_engine-ingestion   crates/ingestion   IngestionActor, source adapters, micro-batch, compaction, producer registry
consumer_engine-execution   crates/exec        DuckDB read-only wrapper, connection pool, EXPLAIN/cost
consumer_engine-query       crates/query       DSL AST+parser, SQL compiler, B/F/J/S/P, guardrails, job model
consumer_engine-semantic    crates/semantic    L0 Profiler, L1 Intent RAG, semantic catalogue
consumer_engine-ingress     crates/ingress     axum REST API, auth, tenancy, payload modes
consumer_engine-server      apps/server        binary: wires actors, loads config, serves
```

Dependency direction (no cycles; `core` is the root):

```text
                        core
              ┌──────┬──────┴───────┬──────────┐
          storage  execution  ingestion       semantic
              │       │            │              │
              └───┬───┴────────────┘              │
              query ◀─────────────────────────────┘
                │
              ingress
                │
              server (bin)
```

- `storage`, `execution`, `ingestion`, `semantic` depend only on `core`.
- `query` depends on `storage` + `execution` (+ reads `semantic` types).
- `ingress` depends on `query`, `semantic`, `ingestion`.
- `server` wires all; it is the only binary.

## 3. Workspace dependency policy (AGENTS.md)

- Reuse the existing `[workspace.dependencies]`: `anyhow`, `serde`,
  `serde_json`, `thiserror`, `tokio`. Add to workspace deps as needed
  (`axum`, `tower`/`tower-http`, `validator`, `tracing`/`tracing-subscriber`,
  `uuid`, `chrono`, `duckdb`, the `ducklake` path via duckdb). Pin with `~`
  (patch) for sensitive crates per AGENTS.md § Dependencies.
- Every crate: `#![forbid(unsafe_code)]`, `#![warn(rust_2024_compatibility,
  missing_docs, missing_debug_implementations)]` (AGENTS.md § Toolchain).
- `thiserror` for library error enums (with `#[source]`); `anyhow` only in
  `server`/bin paths.
- **Missing infra (Phase 1)**: there is no `rust-toolchain.toml` yet;
  AGENTS.md requires pinning the stable toolchain. Land it in Phase 1
  ([91](./91-impl-plan.md)).

## 4. Feature flags

Keep minimal in v1:

- `ingestion-cdc` (default off until the CDC adapter lands; batch is default).
- `semantic-llm` (default on; off → Profiler/RAG return errors, useful for
  tests without an LLM endpoint).
- `escape-hatch-sql` (default on; off → `/query` rejects the raw-SQL form
  entirely, for locked-down deployments).

No feature should silently relax a guardrail.

## 5. Cross-references

- ← Depends on: [00](./00-prd.md).
- → Referenced by every component spec's crate identity.
- D16 (Rust/Python split) lives here; the Python/TS agent is **outside** this
  workspace and calls via REST ([21](./21-rest-api.md)).
