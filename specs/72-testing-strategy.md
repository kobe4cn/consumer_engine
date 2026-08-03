# 72-testing-strategy: Test Pyramid, Fixtures, Guardrail & CDC Tests

Status: draft · Depends on: all component specs

## 1. Purpose

Make the invariants in every spec mechanically checkable. The riskiest
behaviours are: guardrail non-bypass ([12](./12-query-engine.md)), single-writer
exclusivity ([11](./11-runtime-core.md)), snapshot atomicity + hit_reason
([10](./10-data-model.md)), suppression exclusion correctness (closed loop), and
DuckLake MERGE limits ([20](./20-ingestion.md)).

## 2. Test pyramid

```text
               ┌─────────────────────┐
               │ e2e (few)           │  full stack: ingest→DSL→snapshot→exclude
               ├─────────────────────┤
               │ integration (some)  │  DuckLake on tmp Parquet+SQLite cat;
               │                     │  REST via axum test server; CDC sim
               ├─────────────────────┤
               │ unit (many)         │  DSL parser, SQL compiler, guardrail
               │                     │  verdicts, micro-batcher, rule engine
               └─────────────────────┘
```

- **Unit** in-file `#[cfg(test)] mod tests`, names `test_should_…`
  (AGENTS.md). `rstest` for parameterised (e.g. guardrail threshold matrix),
  `proptest` for DSL-AST invariants (I5: J always follows B/F).
- **Integration** in `tests/`. A `DuckLakeTestFixture` brings up a tmp
  catalogue (SQLite for tests; Postgres only in CI) + tmp Parquet dir and seeds
  `raw_*`/`feature_store`/`suppression`.
- **e2e** under `tests/e2e/`, `#[ignore]` (slow), run in CI with
  `--include-ignored`.

## 3. Load-bearing tests (must exist)

| Test | Pins invariant | Shape |
| ---- | -------------- | ----- |
| `test_should_reject_query_over_memory_limit` | [12 I2](./12-query-engine.md) | a query that EXPLAINs over budget returns `Error::Guardrail`, never runs |
| `test_should_reject_j_over_unbounded_survivors` | [10 I5](./10-data-model.md) | a `Derive` without preceding narrowing is rejected |
| `test_should_never_allow_second_writer` | [11 I1](./11-runtime-core.md) | a second `IngestionActor::spawn` returns `WriterAlreadyHeld` |
| `test_should_assert_query_path_is_read_only` | [11 I2](./11-runtime-core.md) | a probe INSERT on a query conn errors |
| `test_should_materialise_snapshot_atomically_with_hit_reason` | [10 I2](./10-data-model.md) | partial snapshot is never observable; every row has hit_reason |
| `test_should_exclude_suppressed_users_from_rerun` | closed loop (E1) | a suppressed user is absent from the next snapshot for that campaign |
| `test_should_enforce_frequency_cap` | [20 §5](./20-ingestion.md) | a user over the N-in-D-days cap is excluded |
| `test_should_parameterise_all_user_values` | [12 I1](./12-query-engine.md) | compiled SQL has no interpolated literals (AST assert) |
| `test_should_report_worst_source_freshness` | [D5](./99-key-decisions.md) | a CDC+batch query reports the batch lag |
| `test_should_redact_secrets_in_logs` | [70 I5](./70-security.md) | a request DTO's `Debug` omits auth tokens |

## 4. CDC / compaction simulation

- A fake `SourceAdapter` emits batches with `cdc_offset`s; tests assert
  offset-advance is atomic with the data write (restart replay yields the same
  table). `test_should_resume_from_last_committed_offset`.
- A compaction test seeds many small Parquet files and asserts post-compaction
  file count ≤ threshold and time-travel still reads old snapshots.

## 5. Mocking & fixtures

- LLM/embedding: behind a trait, mocked in tests (`mockall`) — no network in
  unit/integration. The `semantic-llm` feature off → deterministic errors.
- External delivery: the `/suppression` writeback is tested via the real axum
  test client (no mock of our own surface).
- Prefer real DuckLake/DuckDB over mocks (fast enough on tmp dirs); mock only
  the LLM and the source system.

## 6. CI gates (AGENTS.md)

- `cargo build`, `cargo nextest run --all-features`, `cargo +nightly fmt
  --check`, `cargo clippy -- -D warnings -W clippy::pedantic` on every Rust
  change.
- `cargo audit` + `cargo deny check` on dependency/lockfile changes.
- Doctests via `cargo test --doc`.

## 7. Cross-references

- ← Depends on: every component spec's Invariants section.
- Norms: AGENTS.md § Testing (`test_should_` prefix, rstest/proptest,
  mockall/wiremock, `#[ignore]` slow).
