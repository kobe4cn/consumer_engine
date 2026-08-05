# Consumer Engine

一个**面向 AI 代理的受众筛选引擎**（Rust 编写）：营销运营用自然语言描述目标受众，代理据此组合结构化 DSL，引擎将其编译为 DuckLake 之上的**受守卫的** DuckDB SQL，并物化为带审计 + 抑制的**版本化受众快照**。

**里程碑 M0–M5 已全部关闭**（人签字确认，2025-08；证据表见
[specs/90-roadmap.md](specs/90-roadmap.md) 与
[docs/research/perf-calibration.md](docs/research/perf-calibration.md) §M1/M5
—— 性能门禁 `make bench-queries` 实测 P50 13–65 ms @50k 行，租户隔离 AC6 按构造强制）。
快照过期（#17）作为上游阻塞的独立票保持 OPEN。v1 能力集：

- **B** — 原始事件上的布尔/时间-关系谓词。
- **F** — 预计算逐用户 **Feature Store** 上的谓词。
- **J** — 幸存集上的即时派生指标，受**实测、不可绕过**的上限约束。
- **P** — 段 vs 全量人群的对比画像。
- **S**（阶段 2）— 相似度/相似人群，不在 v1 范围。

## 快速开始

```sh
# 构建 + 测试整个 workspace（含可选 HTTP LLM 的 all-features）
cargo build --workspace
cargo test --workspace --all-features

# 启动服务（默认配置；无认证 —— 仅开发）
cargo run -p consumer_engine-server
# 或指定配置文件：  cargo run -p consumer_engine-server -- --config config.yaml

# 边界 lint（lib 表面禁用 unwrap/indexing/panic/expect）
make lint-boundary

# 查询延迟校准基准（规模用 CE_SCALE_ROWS 控制）
make bench-queries
```

然后访问 `http://127.0.0.1:8080`：

```sh
# 健康检查
curl localhost:8080/healthz

# 注册数据源表（自动画像进语义目录）
curl -X POST localhost:8080/sources/onboard -H 'content-type: application/json' -d '{
  "system":"erp","entity":"orders","columns":["user_id","sku"],
  "rows":[["u1","A"],["u2","B"]]}'

# DSL 查询 —— "购买过 SKU A 的用户"
curl -X POST localhost:8080/query -H 'content-type: application/json' -d '{
  "dsl": {"source":{"system":"erp","entity":"orders"},"key":"user_id",
          "ops":[{"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}]}}'
```

> **生产环境必须配置 `auth_token`** —— 无认证的引擎允许任何调用者签发 presigned 导出（IDOR）。见 [docs/deployment.md](docs/deployment.md)。

## 文档

| 内容 | 位置 |
| ---- | ---- |
| 设计契约（PRD、数据模型、DSL AST、REST、安全、预算） | [specs/](specs/) —— 从 [specs/index.md](specs/index.md) 开始 |
| 组件指南 —— 开发 / 使用 / 测试 / 部署 | [docs/](docs/) —— [docs/index.md](docs/index.md) |
| 研究备忘（DuckLake spike、CDC 调研、性能校准） | [docs/research/](docs/research/) |
| Issue 追踪（v1 全部 issue 已关闭） | GitHub `kobe4cn/consumer_engine` |
| Rust 门禁 / lint / 基准自动化 | [Makefile](Makefile) |

## Workspace 结构

```text
crates/core        类型、错误模型、配置、领域原语（依赖根）
crates/storage     唯一可写 DuckLake 句柄 + 表 DDL/写入器
crates/execution   只读 DuckDB reader（单线程、通道驱动）
crates/ingestion   写入 actor（Q1/Q2/Q3）+ FeatureProducer + cadence
crates/query       DSL AST/解析/编译 + B/F/J/P + 守卫 + 引擎
crates/semantic    L0 Profiler + L1 Intent RAG + LLM/embedding 客户端
crates/ingress     axum REST：唯一信任边界（authN、校验）
apps/server        用 EngineConfig 装配一切的二进制
```

依赖方向无环，`core` 在根（specs/11 §2）。

## 现状与已知限制

- **性能目标在规模下未达成**：实测 B/F/J/P P50 2.5–15 s（50k 行），主因是每次查询的 DuckLake 重新 attach（P1-1）。守卫预算保持为锁定目标；修复路径（读连接池、文件后端 DuckLake）跟踪于
  [docs/research/perf-calibration.md](docs/research/perf-calibration.md)。
- 完整差距清单见 [docs/research/spec-gap-analysis.md](docs/research/spec-gap-analysis.md)（spec 承诺 vs 实现逐条对照）。
- 延后项记录在 [specs/93-improvements-review.md](specs/93-improvements-review.md)：快照级 point-in-time（T4-I3）、目录新鲜度告警（T5-I5）、多租户 schema（T7c-TENANT —— authN 本身已实现）、CDC adapter（P3-4）、DuckDB 服务端语句超时（本 build 不可用）。
