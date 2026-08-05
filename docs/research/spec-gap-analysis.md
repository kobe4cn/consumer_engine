# Spec 差距分析 —— 承诺 vs 实际实现（2025-08）

状态: 分析结论 · 触发: grilling 会话（M1/M5 关闭状态复核） · 日期: 2025-08

本文档回答一个问题：**spec 承诺的能力，代码实际交付了多少？** 依据是全部 13 份
spec、9 个 crate 的源码、e2e 测试面、性能校准文档逐条对照的结论。

## 总论

架构骨架达成约 90%，实体内容达成约 60%，而 PRD 的核心性能承诺（G3）与数据实质
承诺（D11）**没有达成** —— 且差距被"CLOSED with caveat"的关闭机制掩盖了。

代码质量本身极高（安全、错误处理、测试纪律认真），问题不在代码烂，而在
**"实现的接缝正确，接缝里的实体是占位的"**，以及**关闭判定没有经过人确认**。

## 已确认的达成项（✅，约 20/33 不变式）

| 项 | 证据 |
| -- | ---- |
| 单写入者 D3 + 读路径只读 11-I2 | `storage::Writer` 文件锁跨进程、`READ_ONLY` attach、`test_should_refuse_second_writer` |
| 参数化 SQL 12-I1（零插值） | `compiler.rs` 全量 `?` 绑定 + `test_should_parameterise_all_user_values` |
| B/F/J/P 四能力 | 编译 + 守卫 + e2e 全覆盖 |
| 守卫不可绕过 12-I2 | EXPLAIN 预检 + 运行时限 + 内存 PRAGMA + Semaphore；J 用实测 survivor 数 |
| 物化原子性 20-I4 | 单条 `INSERT…SELECT` + `test_should_materialise_snapshot_atomically_with_hit_reason` |
| 抑制闭环 G4/E1 | `Exclude` 反连接 + 幂等 `/suppression` + 频率上限 |
| Feature Store 接缝 D9 | `FeatureProducer` trait + registry + PIT 正确的 cadence 例子 |
| L0/L1 有界采样、PII 脱敏、有界检索 | `profiler.rs` / `intent_rag.rs`（13-I2/I3/I4） |
| 逃逸舱门 D2 | approval token 常数时间比对 + audit log + 同守卫 |
| 安全 70-I5/I6、边界 lint、redacting Debug | 有断言日志无 token 的测试 |
| 错误处理、无 unsafe、actor 模型 | 全仓库贯彻 |

## 差距清单（⚠️=打折 ❌=未实现）

### PRD（specs/00）六大目标

| Goal | 状态 | 实情 |
| ---- | ---- | ---- |
| G1 代理无 SQL 组段 | ⚠️ | 引擎侧全通，但代理本体（D16 的 Python/TS）从未落地，"≥80%"无从测量 |
| G2 每行可审计 | ⚠️ | `hit_reason`/`features` 非空（10-I2 ✅），但内容是占位：`features="{}"`、`hit_reason=整个 DSL JSON`。**"冻结的特征值"没有冻结任何特征值**（D11 落空） |
| G3 P50<1s / P99<5s @≤50M | ❌ | 实测 50k 行（目标的 1/1000）：B 2.5s、F 5.7s、J 7.5s、P 15.3s —— **差 2–3 个数量级** |
| G4 抑制闭环 | ✅ | |
| G5 新表 <30min 可查 | ✅ | onboard 内联 profiling 恒满足（LLM 默认为 stub，见 13 节） |
| G6 预测就绪零改动 | ✅ | Feature Store 接缝成立 |

### 数据模型（specs/10）

| 不变式 | 状态 | 实情 |
| ------ | ---- | ---- |
| I1 无 PII | ⚠️ | `/sources/onboard` 接受任意列任意值，`email` 等原始值直接存入 `raw_*`；profiler 只在目录打 `pii_flag`，**没有存储层拦截/脱敏**（spec 说的 storage-layer lint 不存在） |
| I2 快照行非空 | ⚠️ | 非空成立；**内容**是占位（D11 落空） |
| I3 as_of 无泄漏 | ❌ | `materialize` 的 `as_of_ts = now()`（`crates/query/src/engine.rs`），从未绑定源新鲜度 |
| I4 特征 PIT | ⚠️ | append-only ✅；但无"按 `as_of ≤ 请求` 查询"能力，宽表是"全局最新值赢"的单一快照 |
| I5 J 有界 | ✅ | 实测计数 + `j_survivor_cap` |

结构偏差：全部 `raw_*` 表为 **VARCHAR**（spec 说 schema 由 profiler 发现/可类型化）；
`Distinct` 是顶层 `key` 而非 AST 节点（P2-DISTINCT）；`feature_vec_*`+HNSW 表不存在；
wire 发 `lagSeconds` 而 spec 写 `lagHours`（T1 drift）。

### 运行时核心（specs/11）

| 不变式 | 状态 | 实情 |
| ------ | ---- | ---- |
| I1 单写入者 | ✅ | |
| I2 读路径只读 | ⚠️ | 有单测，但 spec 要求的**启动探针**（`Engine::build` 中 probe INSERT 失败）未实现 |
| I3 优雅关停排空 | ⚠️ | `shutdown()` 只发 `Shutdown`；无 drain 到 checkpoint 的逻辑（micro-batcher 不存在） |
| I4 背压 | ⚠️ | 通道有界（64），但满 Q2 返回通道错误而非类型化 `MaterialiseBackpressure` |

结构偏差：spec 说 **N 个 QueryActor worker 各持一个只读 attach** —— 实际是
**单 reader 线程 + Semaphore**。既是偏离，也是性能瓶颈的直接来源。

### 查询引擎（specs/12）

| 不变式 | 状态 | 实情 |
| ------ | ---- | ---- |
| I1/I2/I3/I4 | ✅ | 全实现 |
| Plan 携带 bytes-scanned | ❌ | DuckDB EXPLAIN 不暴露（spec 已降级为运行时限制，可接受） |
| 71 的 "sync cost cap 估计 1s" | ❌ | `Estimate` 只有 `est_rows`；**估计会花 15s 的查询照样走 sync**（行数 ≤ cap 即放行） |
| EXPLAIN 预检 | ⚠️ | **DuckDB 的 EXPLAIN 会真的执行查询**（`guardrail.rs` 注释自认）→ 每次 sync 查询 = EXPLAIN(真执行) + 执行 = 双倍工作量，是 2.5–15s 的重要组成 |

### 语义层（specs/13）

| 不变式 | 状态 | 实情 |
| ------ | ---- | ---- |
| I1–I4 | ✅ | |
| **I5 目录新鲜度** | ❌ | 目录行无来源快照戳、无 re-onboard 版本化、查询路径不 warn（T5-I5） |
| 描述可编辑 | ❌ | spec 13 §4 的 "human-editable, versioned, re-embedded" **没有任何编辑端点** |
| LLM | ⚠️ | HTTP client 有 timeout+retry（✅），但默认是启发式 **stub**；真实 LLM 需 `semantic-llm` feature |
| 表级目录行 | ❌ | 只发 column 行，`SemanticType` 无 `Table` 变体（T4-SEMANTIC-TABLE-ROW） |

### 摄入（specs/20）

| 不变式 | 状态 | 实情 |
| ------ | ---- | ---- |
| I1/I3/I4 | ✅ | |
| **I2 offset 原子提交** | N/A | 无 CDC |
| **CDC adapter** | ❌ | 完全不存在；`sourceType:"cdc"` 只是标签；91 提到的 `ingestion-cdc` feature 也不存在 |
| **MERGE/upsert/delete** | ❌ | spec 20 §4 整节（去重、维度 upsert、逻辑删除、双 MERGE）无实现；`ingest_raw` 就是无脑 INSERT —— 重放同 key 重复行，维度无法更新，无删除 |
| **micro-batch** | ❌ | `micro_batch_flush_rows` 是**死配置**（全仓库仅 config 出现），P3-4 原样挂着 |
| 压缩 | ⚠️ | 只有 `merge_adjacent_files`；**快照过期 + 孤儿文件清理未实现**（71 §4 表格 4 项只做了 1 项） |

### REST（specs/21）

| 不变式 | 状态 | 实情 |
| ------ | ---- | ---- |
| I1 AuthN | ✅ | bearer 常数时间 |
| **I1 AuthZ 作用域** | ⚠️ | 所有已认证调用者权限相同；`/sources/onboard`、`/suppression` 无需 elevated scope |
| **I2 租户隔离** | ❌ | **零租户代码**（`rg tenant` 无命中）。这是 issue #10 的 **AC6**，被整体延后却关闭了 issue |
| I3/I4 | ✅ | body 限、presign 15min + 日志 |
| I5 全 IO 超时 | ⚠️ | profile/查询有；`/jobs` 的 semaphore 等待、`/suppression` 写回**无显式超时** |

### 性能预算（specs/71，差距最重）

| 预算 | 状态 |
| ---- | ---- |
| P50<1s / P99<5s | ❌ 差 2–3 个数量级 |
| CDC ≤5min 新鲜度 | ❌ 无 CDC |
| micro-batch 30s/50k | ❌ 死配置 |
| 压缩 + 快照过期 + 孤儿清理 | ⚠️ 只做压缩 |
| sync cost cap 1s | ❌ |
| J 预算 | ✅ |

## 三个最大的缺口（按影响排序）

1. **性能：差 2–3 个数量级，且根源（P1-1）从 T1 就知道，从未修。** 单 reader
   线程 + 每查询 DETACH/ATTACH + EXPLAIN 真执行（查询跑两遍）+ 全 VARCHAR 无法走
   索引 + P 类 3 条查询各带 4–5 次 reader 往返。
2. **数据实质是占位符。** 冻结特征 = `"{}"`；快照 as_of = 物化墙钟；特征宽表 =
   全局最新值而非 PIT。
3. **实质能力被整体延后且无回归通道。** CDC、MERGE/upsert、租户隔离（PRD AC）、
   目录新鲜度 I5、描述可编辑、快照过期清理。

## 根因诊断

不是代码质量问题，是五个机制性问题的叠加：

1. **关闭权没有归属，退出标准被"附带条件"架空。** M1/M5 的 roadmap 条目白纸黑字
   写着 "perf caveat: target NOT met"，却标 ✅ CLOSED。当 "CLOSED" 可以带着未达成
   的 exit criteria，里程碑状态就失去信息量。流程里没有"人"这个环节 —— 关闭是
   实现者自己盖的章。
2. **延后机制成了范围削减的合法通道。** `specs/93` 是 append-only 墓地（P1-1 从
   T1 挂到现在）；`features="{}"`、`as_of=now()`、单租户、micro-batch 死配置都被记
   成 "by design"。没有机制要求延后项回到 spec 重新确认范围、或由人签字。
3. **预算从未被验证就被锁定，验证失败后没有回滚机制。** 71 §3 自己写着
   "Phase 0 spike: … validate the P99 budget **before locking it**"，但 91 §3 的
   Phase 0 没有 query-latency spike。预算被锁进 spec；M1 实测 2.5s 后，正确动作是
   "预算回滚/重新校准"，实际动作是 "CLOSED with caveat"。
4. **测试矩阵验证的是"代码语义"，不是"spec 语义"。** 全部测试绿，但没有任何测试
   对着 exit criteria 打：性能无断言（bench 是手动 example，不进门禁）、I3 无测试
   （by design）、I5 无测试、租户无测试、D11 内容无测试。
5. **预期从未被契约化。** PRD 的 G3 是明确承诺；实现的"完成"定义是"结构达成 +
   延后登记"；README 如实披露了差距。**从来没有人把"spec 承诺 vs 实现现状"做成
   一份人签字确认的对照表。**

一句话：**实现者把"接缝做对了"定义为完成，把"实体做对了"（性能、冻结特征、
PIT、租户）定义为可延后；而这个定义只经过实现者自己确认。** 预期来自 PRD，
文档状态来自"实现者自评"，两者之间的校准环节不存在。

## 处置（已执行）

- M1 / M5 在 [90-roadmap.md](../../specs/90-roadmap.md) 中从 CLOSED 回退为
  **OPEN（exit criterion 未达成）**；关闭流程改为"人确认制"（见 roadmap §5）。
- **已闭环（2025-08）**：读池（#20）、冻结特征/谓词链（#13）、租户隔离 AC6（#22）、
  bench 门禁（#25）补齐证据后，经人签字 M1/M5 重新 **CLOSED**（见
  [perf-calibration.md](./perf-calibration.md) §M1/M5 证据表）。
- GitHub issue #3（M1/T2）、#10（T7c/M5）重新打开并附未达成证据。
- 本文档登记为差距的权威清单；补缺按"缺口排序"进行，P1-1 读连接池是性价比
  最高的一刀（单此项可把延迟砍掉 5–10 倍）。

## 交叉引用

- 性能实测： [perf-calibration.md](./perf-calibration.md)
- 延后清单： [93-improvements-review.md](../../specs/93-improvements-review.md)
- 里程碑状态与关闭流程： [90-roadmap.md](../../specs/90-roadmap.md)
- 关键决策（D11 冻结特征、D12 PII）： [99-key-decisions.md](../../specs/99-key-decisions.md)
