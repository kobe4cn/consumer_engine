# Testing Guide

The strategy, the commands, and the scenario map. The testing decisions are
normative in [specs/72-testing-strategy.md](../specs/72-testing-strategy.md);
this guide is the runnable reality.

## The one behavioral seam: REST

Spec-level behavior is exercised through `apps/server/tests/e2e.rs` — 30 tests
against a real DuckLake (tmp catalogue + tmp data), covering the full external
contract: onboard → DSL compile → guardrails → execute → materialise →
export → suppression → semantic retrieval. External collaborators (LLM,
embedding) are mocked at their trait boundaries (`wiremock` for the HTTP
clients under `--all-features`); the delivery writeback goes through its own
real endpoint.

## Commands

```sh
cargo test --workspace                 # 118 tests (default features)
cargo test --workspace --all-features  # 120 tests (incl. wiremock HTTP LLM)
cargo test --test e2e                  # the REST-seam suite only
cargo test -p consumer_engine-query --lib
cargo test -- --ignored                # slow tests (none currently)
```

Load-bearing invariants are pinned by tests (from specs/72):

- a second writer is refused; a probe INSERT on a read-only connection errors;
- a partial snapshot is never observable (atomic single INSERT);
- an over-budget query is rejected and never runs (EXPLAIN pre-flight);
- a compiled query contains no interpolated user values (only `?`);
- suppressed users are absent from a re-run; the frequency cap is enforced;
- the freshness label reports the worst source;
- a request's `Debug` redacts the token (Debug format **and** captured log
  output); presigned export access is logged;
- a `Derive` survivor set is measured (not estimated) and capped;
- temporal `Recency`/`Lapsed` execute end-to-end (the headline B capability);
- the compaction file count drops while rows + snapshot history survive.

## Scenario map (e2e)

| Scenario | Test |
| -------- | ---- |
| DSL filter + freshness over REST | `test_should_run_dsl_filter_query_over_rest` |
| Escape hatch closed / approved | `test_should_reject_raw_sql_escape_hatch`, `test_should_run_approved_raw_sql_escape_hatch` |
| Error → HTTP mapping | `test_should_map_query_errors_to_http_codes`, `test_should_map_survivor_unbounded_to_422`, `test_should_reject_invalid_dsl` |
| Boundary validation (onboard) | `test_should_reject_invalid_onboard_input`, `test_should_reject_too_many_columns`, `test_should_reject_oversized_cell` |
| Over-budget pre-execution | `test_should_reject_over_budget_query_pre_execution` |
| Temporal B | `test_should_run_recency_and_lapsed_over_rest` |
| Jobs + atomic snapshot + export | `test_should_post_jobs_returns_202_with_jobid`, `test_should_materialise_snapshot_atomically_with_hit_reason`, `test_should_stream_parquet_export`, `test_should_poll_job_until_done_or_failed`, `test_should_report_job_status_field`, `test_should_complete_concurrent_jobs_under_slot_cap` |
| 404 / 400 for unknown resources | `test_should_reject_unknown_producer_and_404s` |
| Feature Store + periodic buyers | `test_should_resolve_periodic_buyers_end_to_end` |
| Semantic: profiling + catalog | `test_should_profile_new_table_on_onboard`, `test_should_catalog_returns_bounded_candidates` |
| Freshness grading | `test_should_report_worst_source_freshness` |
| Suppression | `test_should_exclude_suppressed_users_from_rerun`, `test_should_enforce_frequency_cap`, `test_should_exclude_nothing_when_all_rules_off`, `test_should_reject_invalid_suppression_inputs` |
| JIT derive + profile over REST | `test_should_run_jit_derive_and_profile_over_rest` |
| AuthN | `test_should_require_bearer_auth_when_configured` |

## Unit-test coverage by crate

| Crate | Covers |
| ----- | ------ |
| `core` | ident allowlists, config parsing, freshness grading (worst-source, dedupe), DTO serde |
| `storage` | attach/lock, persistence across restart, idempotent suppression write, feature wide-view union + rollback, compaction file-count + snapshot retention |
| `execution` | value→JSON mapping |
| `ingestion` | producer registry, cadence point-in-time (I3) + regularity scores, materialise via handle |
| `query` | parser validation (incl. Derive position invariants), compiler SQL shape (parameterised, Exclude anti-join, frequency cap, Feature EXISTS, Derive CTE + LIMIT), guardrail verdicts, catalogue enforcement (allow/reject/disabled/feature), JIT derive run + cap rejection, comparative profile values, snapshot_meta, escape hatch |
| `semantic` | stub embedding (unit-length, deterministic), Profiler (PII redaction, bounded sample, classification), IntentRag (bounded retrieval, empty catalogue), HTTP clients (wiremock) under `--all-features` |
| `ingress` | token redaction (Debug + log output), presign (roundtrip/tamper/expiry/malformed), JobRegistry TTL expiry |

## Coverage gaps worth adding next

Tracked during the whole-project review (see [specs/93](../specs/93-improvements-review.md)):

- access-log assertion for presigned export (log capture is racy across
  parallel tests; currently verified manually);
- the Profiler's degrade path (embedding/LLM failure → warn + stub/zero-vector)
  has no forcing test (stub clients never fail);
- CDC adapter tests (deferred with the adapter, P3-4).

## Property checks

The DSL-AST invariant "J always follows B/F narrowing and is terminal" is
enforced in `query::parse::validate_positions` with explicit unit tests; a
`proptest` over the AST (as suggested by specs/72) is a future addition.
