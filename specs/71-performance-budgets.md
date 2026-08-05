# 71-performance-budgets: Guardrail Numbers, Freshness SLA, Latency Targets

Status: draft · Depends on: [12](./12-query-engine.md), [20](./20-ingestion.md)

## 1. Purpose

Pins the concrete numbers behind D5 (graded freshness), D6 (micro-batch/
compaction), D14 (sync/async threshold), and the guardrails that make the
read path safe (D2/DoS defense in [70](./70-security.md)). These are **starting
defaults**; calibrate against the bench harness in [72](./72-testing-strategy.md)
and lock via config, not code edits.

## 2. Freshness SLA (graded, D5)

| Source type | Target freshness | Mechanism |
| -- | ---------------- | --------- |
| CDC (Debezium/Kafka) | ≤ 5 min end-to-end | micro-batch flush 5 s + compaction hourly |
| Batch (file/pull) | = batch interval (operator-visible) | scheduled pull; `freshness.lagSeconds` reported |

Every query result carries `freshness = { worstSource, lagSeconds }` (the worst
source touched). A query mixing CDC + batch reports the batch lag.

## 3. Query latency budgets (sync path)

| Metric | Target | Hard cap (guardrail) |
| ------ | ------ | -------------------- |
| P50 sync query | < 1 s | — |
| P99 sync query | < 5 s | `statement_timeout` 30 s → `Error::Guardrail` |
| Sync row cap | — | 100k rows (`sync_row_cap`); else async |
| Sync cost cap | — | no estimated-cost signal (EXPLAIN exposes only rows); heavy queries are runtime-bounded by `statement_timeout` + the row cap — see 12 §4 |
| Output inline cap | — | 1 M rows; bigger → materialise+presigned |
| Per-query memory | — | `memory_limit` 8 GB |
| Concurrency | — | in-flight `Semaphore` = physical cores |

Corpus assumption for the budgets: ≤ 50 M users, ≤ 1 B event rows, DuckLake on
warm object storage with local metadata cache. **Phase 0 spike**: bench a
representative `Lapsed` + `Feature` query at this scale to validate the P99
budget before locking it.

## 4. Ingestion budgets (D6; calibrated by spike)

| Knob | Default | Note |
| -- | ------- | ---- |
| Micro-batch flush rows | 50 000 | per table |
| Micro-batch flush age | **30 s** | raised from 5 s — 5 s ⇒ 17k files/day/table; 30 s ⇒ ~2.9k, within hourly compaction headroom, still ≤ 5 min freshness |
| Compaction cadence | hourly | `ducklake_rewrite_data_files`; **threshold must be tuned** (default merged 0 of 5 files — [spike-microbatch-compaction.md](../docs/research/spike-microbatch-compaction.md)) |
| Snapshot expiry + orphan cleanup | hourly | `ducklake_expire_snapshots` (730 d) + `ducklake_delete_orphaned_files` |
| MERGE split | upsert ⇒ 1 MERGE; update+delete ⇒ 2 MERGEs/txn | confirmed [spike-ducklake-merge.md](../docs/research/spike-ducklake-merge.md) |
| Per-file write cost on object storage | **open — bench before lock** | local SSD hides ~10–100 ms/file on S3/OSS (R1) |

## 5. J (JIT) budgets

| Knob | Default |
| -- | ------- |
| `j_survivor_cap` | 200 000 rows (J rejected above this — narrow first or precompute) |
| J statement timeout | shares the 30 s query budget |

## 6. Phase-2 (S/ML) — not budgeted in v1

Similarity (VSS top-k) and ML producer scoring are phase 2; their budgets are
set when those land ([90](./90-roadmap.md)). The architecture must not regress
the v1 budgets above when they arrive.

## 7. Cross-references

- ← Depends on: [12](./12-query-engine.md), [20](./20-ingestion.md).
- → Consumed by: [70](./70-security.md) (guardrails = DoS defense),
  [72](./72-testing-strategy.md) (bench harness).
- Norms: AGENTS.md § Performance (profile before optimising, preallocate,
  Bytes over Vec for payloads).
