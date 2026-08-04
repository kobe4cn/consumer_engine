# 开发指南

引擎如何构建、crate 边界、门禁、以及贡献者必须遵守的约定。设计契约是 spec 集
（[specs/index.md](../specs/index.md)）；本指南讲代码本身。

## 工具链

- **Rust 2024 edition**，版本钉在 [rust-toolchain.toml](../rust-toolchain.toml)（当前 1.97.0）。格式化用 `cargo +nightly fmt`（[rustfmt.toml](../rustfmt.toml)）。
- Workspace 成员：`crates/*` + `apps/*`（[Cargo.toml](../Cargo.toml)）；共享依赖放 `[workspace.dependencies]`。

## 架构（各部分如何协作）

引擎由一组通过通道通信的 actor 组成（[specs/11-runtime-core.md](../specs/11-runtime-core.md)）：

```
           ingress (axum, 信任边界)
                 │  DSL / onboard / suppression / jobs / catalog
                 ▼
          query engine (编译 → 守卫 → 执行)
                 │  只读                                 │ 经写入
                 ▼                                       ▼
       execution::Reader (单线程)             ingestion::IngestionActor
       (dro attach, 每查询重 attach)          (dl attach; Q1 原始 / Q2 快照 /
                                              Q3 抑制; producers; 压缩)
```

- **单写者**：`storage::Writer` 只移不克隆，持有排它文件锁；第二个 attach 被拒（`Error::WriterAlreadyHeld`）。所有写入都经过 ingestion actor 的 flume 通道（异步 handler 从不直接写）。
- **只读 reader**：`execution::Reader` 每次查询前重新执行 `DETACH dro; ATTACH …` 以看到 DuckLake 提交（P1-1 的变通 —— 见 [perf-calibration.md](research/perf-calibration.md)）。
- **信任边界**：`ingress` 校验每个值（`validate_ident`、字节上限、闭集枚举、`deny_unknown_fields`）、做 authN 门禁、把类型化错误映射为 HTTP 码。其下所有代码都假设输入已校验。

各 crate 关键职责（详见各 crate 文档与对应 spec）：

| Crate | 实现 | Spec |
| ----- | ---- | ---- |
| `core` | `Error`、`EngineConfig`、领域 DTO、`validate_ident`/`validate_feature_name`、`FreshnessRegistry` | 00, 10 |
| `storage` | `Writer`（attach、DDL、写入）、`open_reader` | 10, 20 §4 |
| `execution` | `Reader`、`QueryResult`、`value_to_json` | 11 |
| `ingestion` | `IngestionHandle` actor、`FeatureProducer` + 注册表、cadence producer、微批/压缩 | 20 |
| `query` | DSL AST/解析/编译（B/F/J/P/Exclude）、守卫、`QueryEngine`、`run_sql_approved` | 12, 21 §4 |
| `semantic` | `Profiler`（L0）、`IntentRag`（L1）、stub + HTTP LLM/embedding 客户端 | 13 |
| `ingress` | axum 路由、authN 中间件、handlers、presign | 21, 70 |
| `server` | `Engine::build` 装配 + 二进制 | 11, 21 |

## Rust 门禁（改动完成前必须跑）

按 [AGENTS.md](../AGENTS.md) § Toolchain & Build：

```sh
cargo build --workspace
cargo test --workspace --all-features      # 146 个测试，含可选 feature 套件
cargo +nightly fmt --check
cargo clippy --workspace --all-targets -- -D warnings
make lint-boundary                          # 严格边界 lint（见下）
make bench-queries                          # 性能门禁（见下；性能相关改动必跑）
cargo doc --workspace --no-deps --all-features
```

- **边界 lint**（`make lint-boundary`）：对五个边界 crate（`ingress`、`query`、`storage`、`semantic`、`ingestion`）的 lib 表面启用 `-W clippy::unwrap_used -W clippy::indexing_slicing -W clippy::panic -W clippy::expect_used`。可证明安全的索引改写为防御式（`get`/`get_mut`/解构）——绝不无理由 `#[allow]`。
- **无 `unsafe`**：全 crate `#![forbid(unsafe_code)]`。
- **文档**：`#![warn(missing_docs, missing_debug_implementations)]`；公开项带 `///` 文档，含 `# Errors` 小节。
- **外部输入上禁止 `unwrap`/`expect`/`panic`** —— 用 `?`、`match`、`ok_or_else`、返回 `Result` 的解析器。
- 依赖变化时跑 `cargo audit` / `cargo deny check`。

## 约定（AGENTS.md 摘要）

- 错误：库用 `thiserror` 枚举 / app 用 `anyhow`；可失败路径返回 `Result<T>` 而非 `Option<T>`；`#[source]` 链式。
- 异步：Tokio；**优先消息传递而非共享状态**（通道、`DashMap`、`ArcSwap`）；绝不 `Mutex<HashMap>`；处理任务 panic（`JoinSet`）；对象安全的 `dyn` trait 用 `async-trait`（各 trait 处注明原因）。
- 类型设计：领域原语用 newtype，零值非法用 `NonZeroU32`，库结构体 `#[non_exhaustive]`，解析用 `FromStr`/`TryFrom`，超过 5 字段的 builder 用 `typed-builder`。
- 安全：边界处校验（字节上限、allowlist —— 标识符为 `^[a-zA-Z0-9_]{1,64}$`，**不含 `-`**）；SQL 只用参数化；机密比较用常数时间（`subtle`）；任何携带 token/机密的结构体用脱敏 `Debug`（并带测试）；结构化 `tracing` 日志（绝不 `println!`）。
- 序列化：`serde` + `rename_all = "camelCase"` + `deny_unknown_fields`；用强类型 DTO（除非 schema 真正动态，否则不用 `serde_json::Value`）。

## 测试

策略与完整场景映射见 [docs/testing.md](testing.md)。要点：

- **REST-seam e2e**：`apps/server/tests/e2e.rs`（30 个测试）——规格级行为缝，针对真实 DuckLake 临时目录。
- **文件内单测**（`#[cfg(test)]`、`test_should_*`）覆盖纯逻辑：解析器、编译器 SQL 形状（断言参数化）、守卫判定、新鲜度分级、presign、脱敏、producer 数学。
- feature 门控测试（HTTP LLM 用 `wiremock`）在 `--all-features` 下运行。

## 可选 feature

- `semantic-llm`（由 `server` 转发）：真实 HTTP LLM/embedding 客户端取代确定性 stub。用 `--features semantic-llm` 构建/测试；若设置了 `EngineConfig.llm` 但 feature 关闭，server 会告警并回退到 stub。

## 性能门禁（issue #25）

`make bench-queries` 是**性能退出标准门禁**：`crates/query/examples/query_latency.rs` 播种合成语料（`CE_SCALE_ROWS`，默认 50k），经真实引擎（读池 + 写代数，issue #20）跑 B/F/J/P，断言锁定预算 **P50 ≤ 1 s / P99 ≤ 5 s**（specs/71 §3），未达标 **exit non-zero**。阈值可覆盖以便自测：`CE_MAX_P50_MS` / `CE_MAX_P99_MS`。实测数字与 M1 证据表见 [docs/research/perf-calibration.md](research/perf-calibration.md) —— 50k 行 P50 11–64 ms，远低于预算。
