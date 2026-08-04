# 92 — 差距补缺实现计划（Gap Closure Plan）

Status: ready · Depends on: [docs/research/spec-gap-analysis.md](../docs/research/spec-gap-analysis.md)（权威差距清单）· 关闭流程: [90-roadmap.md §4](./90-roadmap.md)

> 背景：M1/M5 已回退为 OPEN（性能 + 租户隔离退出标准未达成）。本计划是**依赖序
> 的补缺路径**，每一阶段都映射到差距清单的某一节，并在退出时按 roadmap §4 的
> 人确认制关闭。Phase 1 是性价比最高的一刀（P1-1 读连接池，预计把同步查询延迟
> 砍掉 5–10 倍）。

## 0. 差距 → 阶段映射

| 阶段 | 关闭/推进 | 对应差距（spec-gap-analysis） | 捞起的 93 延后项 |
| ---- | --------- | ----------------------------- | ---------------- |
| P0 spike | — | 性能缺口（最大缺口 #1） | P1-1（读路径刷新） |
| P1 读池 | **M1**（性能） | §性能预算 G3 | P1-1 |
| P2 数据实质 | G2 + 10-I3/I4 | 缺口 #2（占位符） | T4-I3、T4-HITREASON、T4-FEATURES |
| P3 租户隔离 | **M5**（安全） | 21-I2 / 70-I3（AC6） | T7c-TENANT |
| P4 摄入实质 | 71 摄入预算、20-I2 | 缺口 #3（CDC/MERGE/micro-batch） | P3-4、P3-5、P2-FRESH、I2 |
| P5 语义完备 | 13-I5、13 §4 | 语义层 | T5-I5、T4-SEMANTIC-TABLE-ROW |
| P6 关闭验证 | M1/M5 正式关闭 | 根因 #4（测试盲区） | P3-10、bench 入门禁 |

S/ML/HNSW/类型化 raw 列继续延后（roadmap Phase 2，本计划不涉及）。

---

## Phase 0 — spike：P1-1 读路径刷新 + EXPLAIN 双执行实测（1–2 天）

**问题**：`crates/execution/src/lib.rs` 每次查询 `DETACH dro; ATTACH ... READ_ONLY`；
且 `guardrail::explain_cost` 注明 DuckDB 的 EXPLAIN **会真执行**查询 → 每次 sync
查询实际跑两遍。单 reader 线程串行化全部查询（spec 11 说 N worker，实际 1 线程）。

**任务**：
- 0.1 调研 DuckLake 读侧快照刷新选项：长活只读 attach 是否在后续提交后可刷新
  （`ducklake_snapshots` / ATTACH 参数 / catalog 版本号）；若无可行的 refresh API，
  验证"按固定 cadence 重 attach 的小连接池"的折衷（读可见性延迟 ≤ cadence，与
  71 的新鲜度 SLA 对齐）。
- 0.2 实测 EXPLAIN 双执行的成本占比：对 B/F/J/P 各跑
  `EXPLAIN (FORMAT JSON) <q>` vs 直接执行，量化预检开销；决定策略 ——
  ① 小查询（预估行数低于阈值）跳过 EXPLAIN、直接用运行时守卫；或
  ② 保留 EXPLAIN 但接受成本；或 ③ 用编译期形状（是否全表扫描/是否带
  narrowing）做廉价预判。
- 0.3 把结果写进 [perf-calibration.md](../docs/research/perf-calibration.md)，
  锁定 Phase 1 的选型。

**退出**：选型确定（读池策略 + EXPLAIN 策略），有实测数字。

---

## Phase 1 — 读连接池 + bench 门禁（3–5 天，**关闭 M1**）

**任务**：
- 1.1 按 P0 选型实现读侧刷新：小连接池（N = 物理核，对齐 spec 11 的 worker 池）
  或 cadence 重 attach；消除每次查询的 `DETACH/ATTACH` 热路径成本。
- 1.2 落实 EXPLAIN 策略（P0 结论）：小查询跳过预检或降级预检，保留
  不可绕过性（runtime 守卫仍在）。
- 1.3 把 [query_latency.rs](../crates/query/examples/query_latency.rs) 的断言
  上升为门禁：`make bench-queries` 带 P50/P99 断言（先锁 50k 行阈值，再按
  实际能力上调规模），失败即红 —— 这是"性能预算从未被验证"的根治。
- 1.4 复测：50k 行下 B/F/J/P P50 < 1 s、P99 < 5 s。

**退出（M1）**：bench 门禁绿；50k 行达标；按 roadmap §4 提交人确认关闭 M1。

**验证**：`cargo test --workspace` + `make bench-queries`；性能断言进 CI。

---

## Phase 2 — 数据实质：冻结特征 + hit_reason + PIT（4–5 天，G2 + 10-I3/I4）

**任务**：
- 2.1 **D11 冻结特征**（`crates/query/src/engine.rs::materialize`）：快照的
  `features` 不再写 `"{}"` —— 物化时按 `as_of` 从 `feature_wide_*` 冻结每个
  命中用户的特征值进 JSON（T4-FEATURES）。
- 2.2 **hit_reason 逐谓词**（T4-HITREASON）：`hit_reason` 从"整个 DSL JSON"
  细化为命中该用户的**谓词链**（编译器可携带每行命中的 conjunct）。
- 2.3 **I3 as_of 有界**（T4-I3）：`materialize` 的 `as_of_ts` 绑定为
  涉及源的新鲜度下限（`FreshnessRegistry` 已有每源 epoch）；断言
  `as_of_ts ≤ 每行特征/raw 行的 as_of_ts`，补 I3 泄漏测试。
- 2.4 **特征 PIT 查询**（10-I4）：`Feature` 谓词支持 `as_of ≤ 请求`
  （如把宽表视图参数化为 as-of 视图，或编译器注入 `as_of_ts <= ?`），
  消除"全局最新值赢"的语义偏差。
- 2.5 顺手项：P2-DISTINCT（AST 节点对齐，spec 10 §3）、P2-2
  （`value_to_json` 补 TIMESTAMPTZ → ISO-8601，随 2.3 的类型化时间戳一起）。

**退出**：快照行携带真实冻结特征 + 逐谓词 hit_reason；I3 泄漏测试绿；
PIT 查询测试绿。

**验证**：`cargo test -p consumer_engine-query -p consumer_engine-storage` +
e2e 扩展。

---

## Phase 3 — 租户隔离 AC6（3–5 天，推进 **M5** 安全退出）

**任务**：
- 3.1 租户模型：auth claims 携带 `tenant_id`；`EngineConfig`/ingress 从 token
  解析（21-I1/I2、70-I3）。
- 3.2 表结构：`raw_*`/`feature_store`/`suppression`/`audience_snapshot`/
  `semantic_catalog` 加 `tenant_id` 列（写路径默认注入调用者租户）。
- 3.3 编译器注入：每条 DSL 编译的 SQL 按调用者 `tenant_id` 过滤 ——
  **由编译器注入，绝不信调用者传的过滤**（spec 21 I2 原话）。
- 3.4 测试：租户 B 的 token 读不到租户 A 的快照/查询/抑制（e2e 交叉租户）。

**退出**：交叉租户隔离 e2e 绿；IDOR 面（`/audience/:id` 越权、presign 越权）
关闭。

**验证**：`cargo test -p consumer_engine-ingress -p consumer_engine-query` +
e2e 交叉租户。

---

## Phase 4 — 摄入实质：CDC + MERGE + micro-batch + 快照过期（7–10 天）

**任务**：
- 4.1 **CDC adapter**（20 §2、D5、I2；survey-cdc-adapter.md 已 GO）：Debezium/
  Kafka consumer（`rdkafka`），`SourceAdapter` trait 落地；**offset 与数据同事务
  提交**（20-I2）—— 重启精确一次（源侧 at-least-once，PK MERGE 去重）。
- 4.2 **MERGE/upsert/delete**（20 §4，spike 结论）：维度表 upsert（单
  `WHEN MATCHED` MERGE）、事件表 append、逻辑删除；adapter 边界按 key 去重。
  这同时修掉"重放同 key 重复行、维度无法更新、无删除"的实缺口。
- 4.3 **micro-batcher 激活**（P3-4）：`IngestionActor` 内按
  `micro_batch_flush_rows` / 30 s 攒批 flush；`micro_batch_flush_rows` 从死配置
  变活配置；关停时强 flush（11-I3 排空）。
- 4.4 **快照过期 + 孤儿清理**（71 §4）：压缩循环补
  `ducklake_expire_snapshots`（730 d）+ `ducklake_delete_orphaned_files`。
- 4.5 **P2-FRESH**：per-source 分级新鲜度随 CDC 真实生效（batch 源报 lag、
  CDC 源报 ≤5min），71 §2 的 CDC SLA 可测。
- 4.6 P3-5：object-storage 延迟在真实存储选型后补 bench（如无真实存储，标注
  仍开放）。

**退出**：CDC e2e（minute 新鲜度 + offset 重启恢复）；维度 upsert/逻辑删除
正确性测试；micro-batch 攒批行为测试；快照过期清理测试。

**验证**：`cargo test -p consumer_engine-ingestion -p consumer_engine-storage` +
CDC 集成（`ingestion-cdc` feature）。

---

## Phase 5 — 语义完备：I5 + 可编辑 + 表级行（3–4 天）

**任务**：
- 5.1 **目录新鲜度 I5**（T5-I5）：`semantic_catalog` 行打来源快照戳；查询路径
  在引用列目录项旧于源最新 ingest 时 `warn!`；re-onboard 版本化。
- 5.2 **描述可编辑**（13 §4）：`/catalog/:id` 编辑端点（写保护 + authz），
  编辑后重新 embed，版本化。
- 5.3 **表级目录行**（T4-SEMANTIC-TABLE-ROW）：`SemanticType` 加 `Table` 变体，
  profiler 发 table 级摘要行（供表粒度 ranking）。

**退出**：I5 warn 测试绿；编辑-重嵌入往返测试绿；表级行可见。

**验证**：`cargo test -p consumer_engine-semantic -p consumer_engine-ingress`。

---

## Phase 6 — 关闭验证：退出标准测试 + 人确认（2–3 天，**关闭 M1/M5**）

**任务**：
- 6.1 逐 milestone 退出标准对照测试清单（roadmap §4 第 1 条落地）：
  M1 性能断言（Phase 1 已入 CI）、M5 预算 + 租户（Phase 3）、G2 审计
  （Phase 2）、G4 抑制（已存在）、G5（已存在）。
- 6.2 P3-10：引擎级重启 e2e（`Engine::build → drop → build` 全链路持久化）。
- 6.3 生成关闭核对单（evidence table），由人签字后关闭 M1/M5、issue #3/#10/#1。

**退出**：两份人签字的关闭核对单；README/roadmap 状态一致。

---

## 估计总工作量

| 阶段 | 天数 |
| ---- | ---- |
| P0 spike | 1–2 |
| P1 读池 + bench | 3–5 |
| P2 数据实质 | 4–5 |
| P3 租户 | 3–5 |
| P4 摄入实质 | 7–10 |
| P5 语义 | 3–4 |
| P6 关闭验证 | 2–3 |
| **合计** | **23–34 天（一人）** |

## 仍延后（明确不在此计划）

- S 相似度 / ML producer（roadmap Phase 2）
- HNSW / `feature_vec_*` 固定维度向量表（随 S 落地）
- raw 表类型化列（VARCHAR 全面替换为类型化 —— 大重构，独立立项；
  当前先靠 2.3 的 cast 方案 + P2-2 输出映射兜底）
- P2-EXPLAIN 的 bytes/memory 预检（DuckDB EXPLAIN 不暴露，等上游）
- DuckDB 服务端语句超时（本 build 不可用，见 93）

## 交叉引用

- 差距权威清单： [spec-gap-analysis.md](../docs/research/spec-gap-analysis.md)
- 里程碑状态 + 关闭流程： [90-roadmap.md](./90-roadmap.md)（§4）
- 主实现计划： [91-impl-plan.md](./91-impl-plan.md)
- 延后项仓库： [93-improvements-review.md](./93-improvements-review.md)
- 性能实测： [perf-calibration.md](../docs/research/perf-calibration.md)
