# 部署指南

如何在生产环境配置与运行引擎。引擎按设计是**单写者 / 单节点**（specs/11）；写者水平扩展不在范围（Postgres-catalog 多写者竞争是已跟踪的开放风险）。

## 配置

服务通过 `--config <path>` 读取 YAML（否则用默认值）。所有键为 `camelCase`；未知键被拒绝。示例：

```yaml
catalog_path: /var/lib/ce/catalog.db      # DuckLake 目录（DuckDB 文件）
data_path: /var/lib/ce/data               # Parquet 数据位置（对象存储亦可）
bind: "0.0.0.0:8080"
compaction_interval_secs: 3600            # 每小时压缩扫描
micro_batch_flush_rows: 50000             # 冲刷阈值（specs/71 §4）

guardrails:
  memoryLimit: "8GB"
  threads: 8
  statementTimeoutSecs: 30
  syncRowCap: 100000
  maxOutputRows: 1000000
  jSurvivorCap: 200000
  enforceCatalogue: true                  # 拒绝未编目列

suppression:
  perCampaignNoRepeat: true
  frequencyCap: { maxContacts: 3, windowDays: 30 }   # 可选；省略即禁用

compaction:
  inliningRowLimit: 0                     # 每个微批次 → 一个数据文件
  targetFileSize: "1MB"

# 安全 —— 生产必须设置：
authToken: "<长随机 token>"               # 门禁所有路由（除 healthz/readyz）
sqlApprovalToken: "<另一随机 token>"       # 可选：启用原始 SQL 逃生舱

# 可选：真实 HTTP LLM/embedding（spec 13 §4）；需要 `semantic-llm` feature 构建
# llm:
#   baseUrl: "http://llm-service:8080"
#   apiKey: "<key>"
#   embeddingDim: 1536
```

每个旋钮的默认值都在 `consumer_engine_core::EngineConfig`（[crates/core/src/config.rs](../crates/core/src/config.rs)）——只含 `catalog_path` + `data_path` 的配置文件即有效。

## 安全清单（specs/70）

- **设置 `authToken`** —— 无认证的引擎允许任何调用者签发 presigned 导出（IDOR）。token 经哈希、常数时间比较。
- **仅当需要原始 SQL 逃生舱时才设置 `sqlApprovalToken`**；它带审计日志并在相同守卫下运行。默认关闭。
- presigned 导出 URL 15 分钟过期（`EXPORT_TTL_SECS`）、HMAC 绑定到快照、访问被记录（仅快照 id）。
- 请求 `Debug` 绝不打印 token/机密（脱敏实现 + 测试）。
- 引擎只存伪匿名 `user_id`；PII 解析留在源/交付系统（specs/10 I1）。
- 任何表都**没有 `PRIMARY KEY`** —— 身份/去重由写入路径强制（suppression 写回经 `suppressionId` 幂等；feature 行按 `as_of_ts` 只追加）。

## 运维注意

- **单写者不变量**：同一目录只允许一个 server 进程 attach；第二个被拒（`WriterAlreadyHeld`）。重启安全——已提交数据可持久（写穿），崩溃时在途写仅缺失（幂等客户端重试覆盖 suppression）。
- **压缩**：微批次累积小文件（内联关闭）；每小时扫描（`compaction_interval_secs`）调用 `ducklake_merge_adjacent_files`。已验证：文件数下降、行完好、快照历史保留。按存储调整 `compaction.targetFileSize`（对象存储每文件延迟是开放风险——specs/71 §4）。
- **读路径**每查询重新 attach DuckLake 目录（P1-1）——低 QPS 尚可；读连接池修复跟踪于 [perf-calibration.md](research/perf-calibration.md)。
- **性能目标当前未在规模下达成**（实测 B/F/J/P P50 2.5–15 s @ 50k 行）。投入延迟 SLO 前先用 `make bench-queries` 校准并跟踪解锁路径。
- **日志**：结构化 `tracing`（生产用 JSON）。`pii_flag` 列不记录用户值；审批审计记录 SQL 文本。

## 构建变体

```sh
cargo build --release                       # 默认：stub LLM/embedding（无网络）
cargo build --release --features semantic-llm   # HTTP LLM/embedding 客户端
```

启用 `semantic-llm` **且**在配置中设置 `llm` 以使用真实模型服务；否则运行确定性 stub（确定性、无网络）。

## 健康 / 就绪

`GET /healthz`（存活）与 `GET /readyz`（就绪）开放，writer + reader 装配完成即返回 200（即构建时）。

## 已知限制（勿重复踩坑）

全部跟踪于 [specs/93-improvements-review.md](../specs/93-improvements-review.md)：

- DuckDB `EXPLAIN` 只暴露行数估算——扫描/内存预算为运行时约束（PRAGMA + 超时），非预检。
- 本 DuckDB build **没有**服务端语句超时 PRAGMA；tokio 超时是后盾（失控查询可能短暂占用单 reader 线程）。
- 按历史快照读取表（时间旅行）在本 DuckLake build 不可解析（`AS OF` / `ducklake_scan` API 拒绝）；快照历史被保留。
- 多租户隔离（tenant_id schema）未实现；authN 已实现。按构造为单租户。
