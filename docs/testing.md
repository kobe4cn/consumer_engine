# 测试指南

策略、命令、场景映射。测试决策以 [specs/72-testing-strategy.md](../specs/72-testing-strategy.md) 为准；本指南是实际可运行的现状。

## 唯一行为缝：REST

规格级行为通过 `apps/server/tests/e2e.rs` 验证——30 个测试，针对真实 DuckLake（临时目录 + 临时数据），覆盖完整外部契约：onboard → DSL 编译 → 守卫 → 执行 → 物化 → 导出 → 抑制 → 语义检索。外部协作者在 trait 边界处 mock（HTTP 客户端在 `--all-features` 下用 `wiremock`）；交付写回走其真实端点。

## 命令

```sh
cargo test --workspace                 # 118 个测试（默认 feature）
cargo test --workspace --all-features  # 120 个测试（含 wiremock HTTP LLM）
cargo test --test e2e                  # 仅 REST-seam 套件
cargo test -p consumer_engine-query --lib
cargo test -- --ignored                # 慢测试（当前无）
```

由测试钉住的承载性不变量（来自 specs/72）：

- 第二个写者被拒；对只读连接的探测 INSERT 报错；
- 部分快照永不可观察（原子单条 INSERT）；
- 超预算查询被拒且绝不执行（EXPLAIN 预检）；
- 编译后的查询不含插值的用户值（只有 `?`）；
- 被抑制用户从重跑中消失；频次上限被强制执行；
- 新鲜度标签报告最差源；
- 请求的 `Debug` 脱敏 token（Debug 格式**和**捕获的日志输出）；presigned 导出访问被记录；
- `Derive` 幸存集是**实测**（非估算）并有上限；
- 时间算子 `Recency`/`Lapsed` 端到端可执行（B 头条能力）；
- 压缩后文件数下降，行数与快照历史保留。

## 场景映射（e2e）

| 场景 | 测试 |
| ---- | ---- |
| DSL 过滤 + 新鲜度（REST） | `test_should_run_dsl_filter_query_over_rest` |
| 逃生舱关闭 / 批准 | `test_should_reject_raw_sql_escape_hatch`、`test_should_run_approved_raw_sql_escape_hatch` |
| 错误 → HTTP 映射 | `test_should_map_query_errors_to_http_codes`、`test_should_map_survivor_unbounded_to_422`、`test_should_reject_invalid_dsl` |
| 边界校验（onboard） | `test_should_reject_invalid_onboard_input`、`test_should_reject_too_many_columns`、`test_should_reject_oversized_cell` |
| 执行前拒绝超预算 | `test_should_reject_over_budget_query_pre_execution` |
| 时间算子 B | `test_should_run_recency_and_lapsed_over_rest` |
| 任务 + 原子快照 + 导出 | `test_should_post_jobs_returns_202_with_jobid`、`test_should_materialise_snapshot_atomically_with_hit_reason`、`test_should_stream_parquet_export`、`test_should_poll_job_until_done_or_failed`、`test_should_report_job_status_field`、`test_should_complete_concurrent_jobs_under_slot_cap` |
| 未知资源 404 / 400 | `test_should_reject_unknown_producer_and_404s` |
| Feature Store + 周期买家 | `test_should_resolve_periodic_buyers_end_to_end` |
| 语义：画像 + 目录 | `test_should_profile_new_table_on_onboard`、`test_should_catalog_returns_bounded_candidates` |
| 新鲜度分级 | `test_should_report_worst_source_freshness` |
| 抑制 | `test_should_exclude_suppressed_users_from_rerun`、`test_should_enforce_frequency_cap`、`test_should_exclude_nothing_when_all_rules_off`、`test_should_reject_invalid_suppression_inputs` |
| JIT derive + 画像（REST） | `test_should_run_jit_derive_and_profile_over_rest` |
| AuthN | `test_should_require_bearer_auth_when_configured` |

## 各 crate 单测覆盖

| Crate | 覆盖 |
| ----- | ---- |
| `core` | 标识符 allowlist、配置解析、新鲜度分级（最差源、去重）、DTO serde |
| `storage` | attach/锁、重启持久化、suppression 幂等写、feature 宽表 union + 回滚、压缩文件数 + 快照保留 |
| `execution` | value→JSON 映射 |
| `ingestion` | producer 注册表、cadence point-in-time（I3）+ 规律性得分、经 handle 物化 |
| `query` | 解析器校验（含 Derive 位置不变量）、编译器 SQL 形状（参数化、Exclude 反连接、频次上限、Feature EXISTS、Derive CTE + LIMIT）、守卫判定、目录强制（允许/拒绝/禁用/feature）、JIT derive 运行 + 上限拒绝、对比画像数值、snapshot_meta、逃生舱 |
| `semantic` | stub embedding（单位长度、确定性）、Profiler（PII 脱敏、有界采样、分类）、IntentRag（有界检索、空目录）、HTTP 客户端（wiremock，`--all-features`） |
| `ingress` | token 脱敏（Debug + 日志输出）、presign（往返/篡改/过期/畸形）、JobRegistry TTL 过期 |

## 值得补充的覆盖缺口

全项目评审期间跟踪（见 [specs/93](../specs/93-improvements-review.md)）：

- presigned 导出的访问日志断言（并行测试间捕获日志有竞态；当前人工核验）；
- Profiler 降级路径（embedding/LLM 失败 → warn + stub/零向量）缺少强制测试（stub 客户端从不失败）；
- CDC adapter 测试（随 adapter 一并延后，P3-4）。

## 属性检查

「J 必须跟随 B/F 收敛且为末位」的 DSL-AST 不变量由 `query::parse::validate_positions` 用显式单测强制；按 specs/72 建议的 AST 上 `proptest` 是未来补充。
