# Development Guide

How the engine is built, the crate boundaries, the gates, and the conventions a
contributor must follow. The design contract is the spec set
([specs/index.md](../specs/index.md)); this guide is about the code.

## Toolchain

- **Rust 2024 edition**, pinned in [rust-toolchain.toml](../rust-toolchain.toml)
  (currently 1.97.0). `cargo +nightly fmt` is the formatter
  ([rustfmt.toml](../rustfmt.toml)).
- Workspace members: `crates/*` + `apps/*`
  ([Cargo.toml](../Cargo.toml)); shared deps live in `[workspace.dependencies]`.

## Architecture (how the pieces fit)

The engine is a set of actors communicating over channels
([specs/11-runtime-core.md](../specs/11-runtime-core.md)):

```
           ingress (axum, trust boundary)
                 │  DSL / onboard / suppression / jobs / catalog
                 ▼
          query engine (compile → guard → run)
                 │  read-only                          │ write via
                 ▼                                     ▼
       execution::Reader (single thread)      ingestion::IngestionActor
       (dro attach, re-attach per query)      (dl attach; Q1 raw / Q2 snapshots /
                                              Q3 suppression; producers; compaction)
```

- **Single writer**: `storage::Writer` is move-only, holds an exclusive file
  lock; a second attach is refused (`Error::WriterAlreadyHeld`). All writes go
  through the ingestion actor's flume channel (never from async handlers
  directly).
- **Read-only reader**: `execution::Reader` re-issues `DETACH dro; ATTACH …`
  before every query so it sees DuckLake commits (the P1-1 workaround — see
  [perf-calibration.md](research/perf-calibration.md)).
- **Trust boundary**: `ingress` validates every value (`validate_ident`,
  byte caps, closed enums, `deny_unknown_fields`), gates authN, and maps typed
  errors to HTTP codes. Everything below assumes validated input.

Key crate roles (detail in each crate's docs and the spec it implements):

| Crate | Implements | Spec |
| ----- | ---------- | ---- |
| `core` | `Error`, `EngineConfig`, domain DTOs, `validate_ident`/`validate_feature_name`, `FreshnessRegistry` | 00, 10 |
| `storage` | `Writer` (attach, DDL, writes), `open_reader` | 10, 20 §4 |
| `execution` | `Reader`, `QueryResult`, `value_to_json` | 11 |
| `ingestion` | `IngestionHandle` actor, `FeatureProducer` + registry, cadence producer, micro-batch/compaction | 20 |
| `query` | DSL AST/parse/compile (B/F/J/P/Exclude), guardrails, `QueryEngine`, `run_sql_approved` | 12, 21 §4 |
| `semantic` | `Profiler` (L0), `IntentRag` (L1), stub + HTTP LLM/embedding clients | 13 |
| `ingress` | axum router, authN middleware, handlers, presign | 21, 70 |
| `server` | `Engine::build` wiring + binary | 11, 21 |

## Rust gates (run before finishing a change)

Per [AGENTS.md](../AGENTS.md) § Toolchain & Build:

```sh
cargo build --workspace
cargo test --workspace --all-features      # 120 tests incl. the optional-feature suite
cargo +nightly fmt --check
cargo clippy --workspace --all-targets -- -D warnings
make lint-boundary                          # strict boundary lint (see below)
cargo doc --workspace --no-deps --all-features
```

- **Boundary lint** (`make lint-boundary`): `-W clippy::unwrap_used
  -W clippy::indexing_slicing -W clippy::panic -W clippy::expect_used` on the
  lib surfaces of the five boundary crates (`ingress`, `query`, `storage`,
  `semantic`, `ingestion`). Provably-safe indexing is rewritten defensively
  (`get`/`get_mut`/destructuring) — never `#[allow]` without a justification
  comment.
- **No `unsafe`**: `#![forbid(unsafe_code)]` crate-wide.
- **Docs**: `#![warn(missing_docs, missing_debug_implementations)]`; public
  items carry `///` docs with `# Errors` sections.
- **No `unwrap`/`expect`/`panic` on external input** — `?`, `match`,
  `ok_or_else`, `Result`-returning parsers.
- `cargo audit` / `cargo deny check` when dependencies change.

## Conventions (from AGENTS.md, abridged)

- Errors: `thiserror` enums (library) / `anyhow` (apps); `Result<T>` not
  `Option<T>` for fallible paths; `#[source]` chaining.
- Async: Tokio; **message passing over shared state** (channels, `DashMap`,
  `ArcSwap`); never `Mutex<HashMap>`; handle task panics (`JoinSet`);
  `async-trait` for object-safe `dyn` traits (documented at each trait).
- Type design: newtypes for domain primitives, `NonZeroU32` where zero is
  invalid, `#[non_exhaustive]` on library structs, `FromStr`/`TryFrom` for
  parsing, `typed-builder` for >5-field builders.
- Safety/security: validate at the boundary (byte caps, allowlists — identifiers
  are `^[a-zA-Z0-9_]{1,64}$`, **no `-`**), parameterised SQL only, constant-time
  comparisons (`subtle`), redacting `Debug` for anything carrying tokens/secrets
  (with tests), structured `tracing` logging (never `println!`).
- Serialization: `serde` + `rename_all = "camelCase"` + `deny_unknown_fields`;
  strongly-typed DTOs (not `serde_json::Value`) unless the schema is truly
  dynamic.

## Testing

See [docs/testing.md](testing.md) for the strategy and the full scenario map.
Short version:

- **REST-seam e2e** in `apps/server/tests/e2e.rs` (30 tests) — the spec-level
  behavior seam, against a real DuckLake tmp catalogue.
- **In-file unit tests** (`#[cfg(test)]`, `test_should_*`) for pure logic:
  parser, compiler SQL shape (assert parameterised), guardrail verdicts,
  freshness grading, presign, redaction, producer math.
- Feature-gated tests (HTTP LLM via `wiremock`) run under
  `--all-features`.

## Optional features

- `semantic-llm` (forwarded from `server`): real HTTP LLM/embedding clients
  instead of the deterministic stubs. Build/test with
  `--features semantic-llm`; the server warns and falls back to stubs if
  `EngineConfig.llm` is set but the feature is off.

## Perf calibration

`crates/query/examples/query_latency.rs` seeds a synthetic corpus
(`CE_SCALE_ROWS`, default 50k) and reports B/F/J/P P50/P99 through the real
engine: `make bench-queries`. Measured reality and the unblocking path are in
[docs/research/perf-calibration.md](research/perf-calibration.md) — targets are
**not met** at scale today (re-attach dominated).
