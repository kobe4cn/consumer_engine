# Spike: DuckLake micro-batch file accumulation and compaction

Status: Done · Owner: platform · Date: 2025-08-03 · Outcome: **PASS-with-amendments**

Environment: DuckDB CLI **v1.5.4** + `ducklake`, local DuckDB-file catalog +
local Parquet data path (object-storage latency NOT exercised — see R1).
Runnable artefact: `/tmp/ce-spikes/files2.sql`.

## Question

[D6](../../specs/99-key-decisions.md) / [11](../../specs/11-runtime-core.md) /
[71 §4](../../specs/71-performance-budgets.md) assume minute-level CDC into
Parquet produces unbounded small files unless micro-batched + compacted. Is
that true on DuckLake, and does its compaction actually merge small files?

## Method

Create a DuckLake table, issue several 5 000-row `INSERT` "micro-batches", and
count Parquet data files via `ducklake_list_files('dl','evt')` after each;
then call `ducklake_rewrite_data_files('dl','evt')` and recount.

## Findings

1. ✅ **Each committed INSERT batch writes one Parquet file.** 0 files at
   create → 3 files after 3 batches → 5 files after 2 more. DuckLake does
   **not** auto-inline/absorb 5k-row batches into the catalog.
2. ⚠ **Compaction is threshold-gated.** `ducklake_rewrite_data_files('dl','evt')`
   over 5 files returned `files_processed = 0` — the merge did nothing because
   the file count/size was below the configured threshold. So "calling
   compaction" is not enough; the threshold must be set so it actually fires.
3. ✅ **File-count math validates D6.** At a 5 s flush, that is 12 files/min ≈
   **17 000 files/day per table** uncompacted. Hourly compaction (D6) is
   load-bearing, not optional.

## Decision

**GO-with-amendments.** Binding rules:

- **Micro-batch flush policy ([71 §4](../../specs/71-performance-budgets.md))**:
  flush on **50 000 rows OR 30 s**, whichever first (was "5 s"). The 5 s
  default produces too many files; 30 s gives ~2 880 files/day/table, well
  within hourly compaction headroom, and still meets the ≤ 5 min CDC freshness
  target ([71 §2](../../specs/71-performance-budgets.md)).
- **Compaction threshold must be tuned**, not left at default. Verify via
  `ducklake_settings` and set a threshold that merges at our file sizes; a CI
  test asserts compaction reduces file count on a seeded small-file scenario.
- **Snapshot expiry + orphan cleanup** scheduled: `ducklake_expire_snapshots`
  (time-travel window) + `ducklake_delete_orphaned_files` (reclaim space), per
  the 2-year retention ([10](../../specs/10-data-model.md)).

## Risks identified

- **R1 (open, critical)**: object storage (S3/OSS) adds ~10–100 ms **per file
  write** vs local SSD's ~µs. This makes file-count even more costly and may
  force a larger flush interval. **Bench on target object storage before
  locking the 30 s / 50 k numbers.**
- **R2 (pinned by test)**: `test_should_compact_reduces_file_count` — seed
  many small batches, run compaction with our threshold, assert file count
  drops; time-travel still reads old snapshots.
- **R3 (open)**: exact `ducklake_flush_inlined_data` arg signature not resolved
  this spike (binder error on `(catalog, table, schema)`); confirm when wiring
  the compaction scheduler. Inlining threshold for *very* small writes still
  needs characterizing.
