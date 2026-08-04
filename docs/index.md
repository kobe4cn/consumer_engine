# Docs Index — Consumer Engine

工程文档。设计契约（spec）在 [../specs/](../specs/)；研究备忘（先例 + spike 证据）在此目录。

## 指南

| 指南 | 内容 |
| ----- | ----- |
| [development.md](./development.md) | crate 边界、工具链、Rust 门禁、约定、可选 feature |
| [usage.md](./usage.md) | REST 面、DSL 语法、可运行走查、错误码 |
| [testing.md](./testing.md) | 测试策略、命令、场景映射、覆盖缺口 |
| [deployment.md](./deployment.md) | 配置参考、安全清单、运维注意、限制 |

## Research

Load-bearing memos that validated (or corrected) spec assumptions before code.
The spec set cites these by path; the impl plan relies on their decisions.

| Memo | Kind | Outcome | What it settled |
| ---- | ---- | ------- | --------------- |
| [research/spike-ducklake-merge.md](./research/spike-ducklake-merge.md) | spike | PASS-with-amendments | DuckLake rejects PK constraints; MERGE = one `WHEN MATCHED` (upsert=1 stmt, update+delete=2); no dedup; insert 37M rows/s local |
| [research/spike-duckdb-vss.md](./research/spike-duckdb-vss.md) | spike | PASS | HNSW requires fixed `FLOAT[N]` (not `FLOAT[]`); index engages only with constant query vector; build offline |
| [research/spike-microbatch-compaction.md](./research/spike-microbatch-compaction.md) | spike | PASS-with-amendments | 1 Parquet file per commit; compaction is threshold-gated; flush interval raised to 30s/50k |
| [research/survey-cdc-adapter.md](./research/survey-cdc-adapter.md) | survey | GO | Debezium+Kafka via `rdkafka`; batch fallback realistic; per-source freshness grading |
| [research/perf-calibration.md](./research/perf-calibration.md) | calibration | measured | B/F/J/P sync P50/P99 harness; targets unmet — per-query DuckLake re-attach (P1-1) dominates; guardrail budgets stay as locked targets |
| [research/spec-gap-analysis.md](./research/spec-gap-analysis.md) | gap analysis | actionable | spec 承诺 vs 实现的逐条差距（33 不变式逐项核对）；根因：关闭权无归属、延后=范围削减、预算未验证即锁定；M1/M5 已回退为 OPEN |

## Spec corrections driven by this research

These memos changed the spec set — read them alongside the specs they amend:

- **[10-data-model.md](../specs/10-data-model.md)**: removed all `PRIMARY KEY`
  constraints (DuckLake rejects them); vector features moved out of the EAV
  `feature_store` into dedicated `feature_vec_<name>(emb FLOAT[dim])` tables;
  `semantic_catalog.embedding` is fixed `FLOAT[dim]`.
- **[20-ingestion.md §4](../specs/20-ingestion.md)**: precise MERGE split rule;
  adapter-enforced identity (DuckLake does not dedup).
- **[71-performance-budgets.md §4](../specs/71-performance-budgets.md)**:
  micro-batch flush raised to 50k rows / 30 s; compaction threshold must be
  tuned, not default.

Environment pin for all spikes above: **DuckDB CLI v1.5.4** (Variegata),
macOS arm64, 2025-08-03. Open risks (object-storage latency, Postgres-catalog
multi-writer contention, production-scale HNSW build) are named per memo and
tracked as follow-up spikes in [91-impl-plan.md](../specs/91-impl-plan.md).
