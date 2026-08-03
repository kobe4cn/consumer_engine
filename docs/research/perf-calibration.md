# Perf calibration — B/F/J/P sync latencies (issue #10 AC1)

Status: harness shipped, numbers measured · Owner: platform · Date: 2026-08

## Harness

`crates/query/examples/query_latency.rs` — seeds a synthetic `erp.orders` corpus
(scale via `CE_SCALE_ROWS`, default 50 000), writes catalogue + a
`cadence.regularity` feature + wide view, then runs 100 sync queries of each
capability through the real `QueryEngine` (guardrails ON, catalogue enforced)
and reports P50/P99:

```sh
cargo run --release -p consumer_engine-query --example query_latency
```

## Measured (in-memory DuckLake attach, 50k rows, single-batch seed)

| type | p50 (ms) | p99 (ms) |
| ---- | -------- | -------- |
| B (filter)      | 2524 | 2590 |
| F (feature)     | 5685 | 5904 |
| J (derive)      | 7461 | 7736 |
| P (characterize)| 15308| 15695 |

## Findings

- **The ≤50M-user P50<1s / P99<5s targets are NOT met — not even at 50k rows.**
  The dominant cost is the **per-query DuckLake re-attach** (`DETACH dro;
  ATTACH ... READ_ONLY` before every reader query, the P1-1 workaround): each
  `run()` issues 4–5 reader queries (catalogue guardrail probes, EXPLAIN
  pre-flight, execution), each re-attaching the catalog. P (characterize) runs
  3 profile queries + guardrail probes → ~15 s.
- J adds a measured survivor `count(*)` scan (the non-bypassable cap) — correct,
  but another full pass over the narrowing set.
- Guardrail defaults in `GuardrailConfig` (specs/71) remain the **locked
  budgets**, not achieved latency — the harness is the calibration tool that
  shows the gap.

## What unlocks the targets (later phase, tracked)

1. **Fix P1-1** (the re-attach workaround): a small read-connection pool that
   re-attaches on a cadence (or a DuckLake snapshot-refresh API) instead of per
   query — removes 4–5 attach costs per query.
2. **File-backed DuckLake attach** with real parallel Parquet scan (the dev
   in-memory attach inlines the catalog; at 50M users seeding itself is the
   bottleneck — the earlier 50k-row ingest probe took ~135 s in the pre-tuning
   config).
3. Re-run the harness at `CE_SCALE_ROWS=50000000` on the file-backed config and
   re-lock the guardrail numbers from the P50/P99 it reports.

## Cross-references

- P1-1 deferral: `specs/93-improvements-review.md`.
- Budgets: `specs/71-performance-budgets.md`.
