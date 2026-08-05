# GC 程序回顾 —— v1 差距补缺交付与证据（issue #11 收口）

状态: 收口文档 · 日期: 2025-08 · 对应: specs/92-gap-closure-plan.md（P0→P6）

本文档是伞形 PRD（issue #11）的关闭证据：把 30 条 user story 与 6 个阶段映射到
**真实存在的测试/门禁/commit**，供人核验。除 #17（上游阻塞，见末尾）外全部达成。

## 阶段完成度

| 阶段 | 内容 | 完成 |
| ---- | ---- | ---- |
| P0 spike | 读路径刷新 + EXPLAIN 实测 | ✅ #12（发现逐行快照根因） |
| P1 性能 | 读池 + bench 门禁 | ✅ #20（P50 13–65ms @50k）+ #25（门禁） |
| P2 数据实质 | 冻结特征 + hit_reason + I3 + PIT | ✅ #13 + #21 |
| P3 租户 | AC6 构造上隔离 | ✅ #14 + #22 |
| P4 摄入 | CDC + MERGE + micro-batch + 过期 | ✅ #15/#16/#24；⏸ 过期=#17 上游 |
| P5 语义 | I5 + 可编辑 + 表级行 | ✅ #18 + #23 + #19 |
| P6 关闭 | 证据表 + 人签字 | ✅ #26（M1/M5 关闭、重启 e2e） |

## 30 条 user story → 证据

| # | Story | 证据 |
| - | ----- | ---- |
| 1 | sync <1s | `make bench-queries`（#25）：B/F/J/P P50 13–65ms @50k |
| 2 | per-source freshness | `test_should_report_worst_source_freshness` + CDC 分级（#24） |
| 3 | DSL 免手写 SQL | B/F/J/P DSL e2e（`test_should_run_dsl_filter_query_over_rest` 等） |
| 4 | 逐用户特征 + 原因 | `test_should_resolve_periodic_buyers_end_to_end`（Parquet 冻结特征 + 谓词链，#13） |
| 5 | lapsed/recent 正确 | `test_should_run_recency_and_lapsed_over_rest` |
| 6 | 特征按查询时点 | `test_should_feature_predicate_respect_as_of`（#21） |
| 7 | 快照冻结特征 | 同 #4 + `test_should_bound_snapshot_as_of_and_frozen_features`（#21） |
| 8 | 抑制排除 | `test_should_exclude_suppressed_users_from_rerun` |
| 9 | 守卫拒超预算 | `test_should_reject_over_budget_query_pre_execution` |
| 10 | 有界检索 | `test_should_catalog_returns_bounded_candidates` |
| 11 | 目录陈旧告警 | `test_should_warn_when_catalogue_stale`（#18） |
| 12 | 描述可编辑 | `test_should_edit_catalogue_description_and_supersede`（#23） |
| 13 | Parquet 拉取 | `test_should_stream_parquet_export` |
| 14 | 幂等写回 | `test_should_write_suppression_idempotently` |
| 15 | CDC 端到端 | `test_should_consume_from_kafka_mock_cluster`（#24，feature-gated） |
| 16 | offset 原子 + 恢复 | `cdc_tests`（回滚/去重/恢复/pump，#24） |
| 17 | MERGE upsert | `test_should_upsert_dedup_and_update_by_key`（#16） |
| 18 | 逻辑删除 | `test_should_logical_delete_by_key`（#16） |
| 19 | micro-batch 攒批 | `test_should_accumulate_rows_until_flush_threshold` 等（#15） |
| 20 | 快照过期/孤儿清理 | ⏸ 上游阻塞（#17）—— 文件数轴有界（`test_should_maintenance_pass_keep_rows_and_expire_when_supported`：20→≤5）+ 门禁兜底 |
| 21 | 分级新鲜度 | `test_should_pump_apply_batches_mark_cdc_freshness_and_resume`（lag<300s，#24） |
| 22 | 快照 PIT | `test_should_bound_snapshot_as_of_and_frozen_features`（#21） |
| 23 | 冻结特征 + 链 | #4 证据 |
| 24 | 租户构造隔离 | `test_should_isolate_tenants_by_construction`（#22） |
| 25 | presign 租户作用域 | 同上（跨租户 404）+ `test_should_redact_download_url_in_debug` |
| 26 | 可复现 time-travel 快照 | DuckLake 快照保留（`test_should_compact_reduce_file_count_and_preserve_rows_and_snapshots`） |
| 27 | 重启持久化 | `test_should_survive_engine_restart`（#26，P3-10） |
| 28 | 里程碑状态真实 | roadmap M0–M5 全部 CLOSED（人签字），#26 |
| 29 | 性能预算 CI 门禁 | `make bench-queries`（#25） |
| 30 | 人签字关闭 | M1/M5 签字（2025-08），#26 |

## 遗留与剩余风险

1. **#17 快照过期（上游阻塞）**：DuckDB v1.5.5 的 TSTZ 过程参数绑定缺陷（11+
   调用形式证据，specs/93 GC-MAINT-BINDER）。缓解：文件数轴有界（merge）、
   产生率有界（micro-batch 30s/50k）、存量退化由 bench 门禁可观测；能力探测
   在 DuckDB 修复后零代码自动翻转。长跑部署（>30 天高频 CDC）需留意门禁。
2. **raw 侧 I3**：raw 表无 ingest 列，快照 as_of 只约束特征侧（specs/93
   GC-I3-RAW）。
3. **scale 验证**：性能数字在 50k 行（目标的 1/1000）；≤50M 用户的文件后端
   attach 复测未做（roadmap 已注明）。

## 交叉引用

- 差距清单: [spec-gap-analysis.md](./spec-gap-analysis.md)
- 实现计划: [92-gap-closure-plan.md](../../specs/92-gap-closure-plan.md)
- 里程碑证据: [perf-calibration.md](./perf-calibration.md) §M1/M5
- 延后项: [93-improvements-review.md](../../specs/93-improvements-review.md)
