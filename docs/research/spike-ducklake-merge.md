# Spike: DuckLake MERGE limits, PK enforcement, and insert throughput

Status: Done · Owner: platform · Date: 2025-08-03 · Outcome: **PASS-with-amendments**

Environment: DuckDB CLI **v1.5.4** (Variegata) on macOS arm64; DuckLake via a
**local DuckDB-file catalog** (`ATTACH 'ducklake:cat.db'`) + local Parquet
data path. The Postgres-catalog multi-writer path is NOT exercised here (open
risk R3). Runnable artefacts: `/tmp/ce-spikes/spike-merge*.sql`,
`spike-thr.sql`.

## Question

Does [20-ingestion.md §4](../../specs/20-ingestion.md)'s assumption hold — that
DuckLake MERGE supports a single UPDATE/DELETE action, does not enforce primary
keys, and that ingest throughput is adequate at our batch sizes? If the MERGE
shape or PK story is different, the adapter design and [10-data-model.md]'s
table DDL both change.

## Method

`duckdb :memory: < spike-merge2.sql` (MERGE action combinations over a 3-row
table) and `spike-thr.sql` (inserts of 50k/200k/1M rows into a DuckLake
table), `.timer on`. Pasted outputs below are abbreviated to the load-bearing
lines.

## Findings

1. ✅ **DuckLake rejects `PRIMARY KEY`/`UNIQUE` constraints entirely** — not
   merely "unenforced". `CREATE TABLE dl.users_dim (user_id VARCHAR, ...,
   PRIMARY KEY(user_id))` → `Not implemented Error: PRIMARY KEY/UNIQUE
   constraints are not supported in DuckLake`. So every `PRIMARY KEY (...)`
   clause in [10-data-model.md](../../specs/10-data-model.md) is **invalid** and
   must be removed.
2. ✅ **MERGE allows exactly one `WHEN MATCHED` clause.**
   - UPDATE + INSERT (a true upsert) **succeeds in a single statement**:
     `WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` applied
     both (matched row updated, new row inserted).
   - UPDATE + DELETE is **rejected** at parse: `Parser Error: Unconditional
     WHEN MATCHED clause was already defined - only one unconditional WHEN
     MATCHED clause is supported`.
   - DELETE + INSERT (one MATCHED + one NOT MATCHED) is allowed.
3. ✅ **No dedup whatsoever.** Two `INSERT` of the same key yield two rows
   (`count = 3` for one `user_id` after 1 seed + 2 dup inserts). DuckLake will
   not collapse duplicates; the **adapter must** treat the MERGE `ON(key)` as
   the identity and carry deletes explicitly.
4. ✅ **INSERT throughput is not the bottleneck.** 1M single-column-ish event
   rows into DuckLake (local): **0.027 s real** ≈ 37 M rows/s. The cost lives
   in **file count on storage**, not in the write path (see
   [spike-microbatch-compaction.md](./spike-microbatch-compaction.md)).

## Decision

**GO-with-amendments.** The adapter and data model adopt these binding rules:

- **[10]**: drop **all** `PRIMARY KEY`/`UNIQUE` clauses from DuckLake DDL.
  Identity is enforced by the adapter via `MERGE ON(key)`, never by DuckLake.
- **[20 §4]**: a batch with **mixed update + delete** for matched keys is split
  into **two MERGEs in one catalog transaction** (one `UPDATE`, one `DELETE`).
  A pure **upsert** (update-or-insert) is **one** MERGE. Event appends stay
  plain `INSERT`.
- Adapter must dedup source batches by key before MERGE (DuckLake won't).
- Compaction is **mandatory**, not optional (R2 / D6).

## Risks identified

- **R1 (pinned by test)**: `test_should_dedup_source_batch_before_merge` —
  assert a batch with duplicate keys produces one row, not N.
- **R2**: compaction threshold gating (5 files merged 0) — see
  [spike-microbatch-compaction.md](./spike-microbatch-compaction.md).
- **R3 (open)**: Postgres-catalog multi-writer optimistic-concurrency
  contention untested (only DuckDB-file catalog here). Follow-up spike before
  scaling out writers; until then single-writer (D3) is safe by construction.
- **R4 (open)**: MERGE/INSERT throughput under **object-storage** latency
  (S3 ~10–100 ms/file) untested; local SSD hides per-file cost. Bench on
  target storage before locking the flush interval ([71 §4](../../specs/71-performance-budgets.md)).
