# Perf calibration — B/F/J/P sync latencies (issue #10 AC1)

Status: harness shipped, numbers measured · Owner: platform · Date: 2026-08
Updated: read-path spike (issue #12 / GC-P0) found and fixed the seed-path
defect that produced the original numbers; new decomposition below.

## Harness

- `crates/query/examples/query_latency.rs` — full-path P50/P99 per capability
  through the real `QueryEngine` (guardrails ON, catalogue enforced).
- `crates/query/examples/read_path_spike.rs` — read-path decomposition: attach
  cost, freshness pinning, dirty-check viability, and per-capability
  full-vs-exec-vs-EXPLAIN (issue #12).

```sh
cargo run --release -p consumer_engine-query --example query_latency
CE_SCALE_ROWS=50000 cargo run --release -p consumer_engine-query --example read_path_spike
```

## Measured BEFORE the write-path fix (in-memory DuckLake attach, 50k rows)

| type | p50 (ms) | p99 (ms) |
| ---- | -------- | -------- |
| B (filter)      | 2524 | 2590 |
| F (feature)     | 5685 | 5904 |
| J (derive)      | 7461 | 7736 |
| P (characterize)| 15308| 15695 |

> **These numbers were dominated by a seed-path defect, not the engine.**
> `Writer::ingest_raw`/`write_feature_rows` committed **one DuckLake snapshot
> per row** (50k rows + 5k features → 55 011 snapshots). Every read attach then
> read 55k snapshot manifests → ~0.5–1.1 s per attach, and every query paid
> 3–5 attach costs (catalogue probes + EXPLAIN + execute). The same defect
> inflates any bench that seeds through the per-row path.

## Read-path spike findings (issue #12, GC-P0)

After batching the write path (multi-row `VALUES` chunks, 500 rows/commit) the
catalog holds **120 snapshots** and the picture is:

| measurement (50k rows, p50) | before fix | after fix |
| --------------------------- | ---------- | --------- |
| raw `DETACH+ATTACH` cost    | ~500–1100 ms | **26 ms** |
| B full path (`engine.run`)  | 2551–3423 ms | **115.7 ms** |
| F full path                 | 4684–5685 ms | **158.9 ms** |
| J full path                 | 7461 ms      | **160.9 ms** |
| P full path                 | 15308 ms     | **183.2 ms** |
| B execute (long-lived attach, pool floor) | — | **6.9 ms** |
| F execute (pool floor)      | —           | **5.4 ms** |
| J execute (pool floor)      | —           | **9.4 ms** |
| `EXPLAIN (FORMAT JSON)` cost (B/F/J) | —  | **0.6–0.8 ms** |

Findings:

1. **P1-1 confirmed**: a long-lived read-only DuckLake attach is pinned at
   attach time — it does **not** see post-attach commits; `DETACH+ATTACH` is
   the only refresh. (Spike phase A: 50 000 → 50 000 rows on the same attach,
   50 002 after re-attach.)
2. **No cheap dirty-check exists — two candidates measured and rejected**:
   `ducklake_snapshots('dro')` reads the attach's own pinned view (count
   unchanged 120 → 120 after commit, ~0.6 ms/check), and the catalog file's
   `mtime` does not advance on commit either (identical timestamps, ~0 µs/check
   — ducklake defers/flushes catalog writes elsewhere). Neither can signal
   "needs refresh"; refresh must be **wall-clock cadence driven**.
3. **Attach cost collapses with snapshot count** (55k snapshots → ~0.5–1.1 s
   attach; 120 snapshots → 26 ms). The relationship is not linear per-snapshot
   (a large fixed component dominates), but the direction is decisive: keep the
   catalog small — batched writes (this spike's fix), then micro-batcher #15
   and snapshot expiry #17 in steady state.
4. **EXPLAIN is cheap relative to execute** for B/F/J (0.08–0.15×, 0.6–0.8 ms
   vs 5–10 ms exec): it does not double the query cost at this scale, so the
   pre-flight is kept as-is (strategy ② 保留; ③ shape-prediction adds
   complexity for a non-problem). P has **no** EXPLAIN pre-flight in the
   production path by design (three profile queries), so the decision does not
   apply to it.

## Locked decisions (feed specs/92 Phase 1 / issue #20)

1. **Read pool = K connections, refresh on the writer's generation, cadence
   backstop.** K = physical cores (validated in #20), each attached read-only
   once at startup. The original spike concluded "refresh must be wall-clock
   cadence driven" because no **reader-side** dirty signal exists; the shipped
   design refines this by making the **single writer** (D3) the signal source —
   an `AtomicU64` generation bumped after every committed write, so a worker
   re-attaches exactly when the catalog changed (zero staleness, zero
   steady-state attach cost), with the 5 s cadence kept as a connection-warmth
   backstop. See the shipped-numbers table below.
2. **EXPLAIN pre-flight unchanged (保留).** B/F/J measured at 0.6–0.8 ms
   (0.08–0.15× of exec) — noise; it keeps the row estimate driving sync/async
   mode selection and the row-budget guardrail. Revisit only at 1M+ rows.
3. **Write path must stay batched.** The chunked multi-row `VALUES` writes in
   `Writer::ingest_raw`/`write_feature_rows` (500 rows/commit, parameter-capped)
   are now an invariant — 55k snapshots → 120. The micro-batcher (#15) and
   snapshot expiry (#17) keep the catalog small in steady state so attach stays
   ~ms. (This change landed in the spike as a measurement-correcting fix; its
   behavior is pinned by the multi-chunk storage tests.)

## Read pool shipped (issue #20, 2025-08) — numbers at 50k rows

The pool landed as a **refinement of decision 1 above**: refresh is driven by
an `AtomicU64` **write generation** the single writer bumps after every
committed write (the reliable commit signal the spike found no reader-side
proxy for), with the 5 s cadence demoted to a connection-warmth backstop. This
preserves write→query immediacy (pure cadence would serve stale rows right
after a commit) while keeping the hot path attach-free in steady state.

`bench` (`query_latency`, production path: pooled reader + generation):

| type | before pool (ms) | after pool p50 (ms) | p99 (ms) |
| ---- | ---------------- | ------------------- | -------- |
| B (filter)      | 115.7 | **10.65** | 138.6 |
| F (feature)     | 158.9 | **11.89** | 24.1 |
| J (derive)      | 160.9 | **21.10** | 184.5 |
| P (characterize)| 183.2 | **36.53** | 41.6 |

5–13× faster; **P50 < 1 s / P99 < 5 s met at 50k rows with 2–3 orders of
magnitude of headroom** (the exec floor is 5–10 ms; the residual is the
EXPLAIN pre-flight + catalogue probes, now attach-free). Re-run at
`CE_SCALE_ROWS=50000000` on the file-backed attach to re-lock the guardrail
numbers (still open — bench-gate #25 will CI-enforce the ≤ 1 s budget).

Pool details: `consumer_engine-execution::Reader::start_pooled(conns, …,
write_gen, interval)`; workers re-attach only when the generation advances or
`interval` elapses. `Reader::start` (no generation) keeps the per-query refresh
for standalone/test readers. K = physical cores (specs/11 §2a).

## What unlocks the targets (later phase, tracked)

1. ~~Read pool (issue #20)~~ — **done**: 50k-row full-path P50 is now 11–37 ms
   (see the table above); the residual is EXPLAIN + probes, not attach.
2. **File-backed DuckLake attach** with real parallel Parquet scan (the dev
   in-memory attach inlines the catalog; at 50M users seeding itself is the
   bottleneck).
3. Re-run the harness at `CE_SCALE_ROWS=50000000` on the file-backed config and
   re-lock the guardrail numbers from the P50/P99 it reports.
4. Bench gate with P50/P99 assertions (issue #25) lands once the read pool is
   in, so the ≤1 s budget becomes CI-enforced.

## Cross-references

- P1-1 deferral + write-path batching fix: `specs/93-improvements-review.md`,
  `specs/92-gap-closure-plan.md` (Phase 1).
- Budgets: `specs/71-performance-budgets.md`.
- Spike ticket: issue #12; read-pool implementation ticket: issue #20.
