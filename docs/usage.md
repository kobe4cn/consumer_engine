# 使用指南

如何通过 REST API 驱动引擎——面向 AI 代理与交付系统。wire 形状以
[specs/10-data-model.md §4](../specs/10-data-model.md) 和
[specs/21-rest-api.md](../specs/21-rest-api.md) 为准；本指南是可运行的走查。

## 认证

配置了 `auth_token` 时（生产环境），除 `/healthz` 和 `/readyz` 外的每个请求都需要：

```
Authorization: Bearer <auth_token>
```

否则返回 `401`。onboarding 与 `/producers/run` 面向工程师；`/query` 与 `/catalog` 是代理的界面。

## 端点

| 方法 | 路径 | 用途 |
| ---- | ---- | ---- |
| GET  | `/healthz`、`/readyz` | 存活 / 就绪（开放） |
| POST | `/sources/onboard` | 注册 + 自动画像一个数据源表 |
| POST | `/query` | 同步 DSL 查询（小结果）；`sql` 逃生舱需审批 token |
| GET  | `/catalog?q=…&k=…` | 意图检索（有界候选） |
| POST | `/producers/run` | 运行一个已注册的 feature producer |
| POST | `/suppression` | 交付写回（幂等） |
| POST | `/jobs` | 异步物化 |
| GET  | `/jobs/{id}` | 轮询任务（`status: running\|done\|failed`） |
| GET  | `/audience/{snapshot_id}` | 快照元数据 + presigned 导出 URL |
| GET  | `/audience/{snapshot_id}/export?format=parquet&token=…` | 流式 Parquet 字节 |

## DSL（主路径）

一个段是 `{ source, key, ops[] }`；每个结果都携带分级 `freshness` 标签。算子（specs/10 §3、specs/12）：

| op | 能力 | 说明 |
| -- | ---- | ---- |
| `filter` | B | `{ column, op: eq\|ne\|lt\|le\|gt\|ge\|in\|notIn\|like\|notLike, value }` |
| `recency` | B | `{ event, userKey, tsColumn, withinDays }` —— 最近 N 天买过 |
| `lapsed` | B | `{ event, userKey, tsColumn, withinDays }` —— 之前买过、窗口内没买 |
| `setOp` | B | `{ op: intersect\|union\|minus, other }` —— 必须为末位算子 |
| `feature` | F | `{ name: "family.short", op, value }` —— 宽表上的数值比较 |
| `derive` | J | `{ name, metric: { kind: count\|sum\|avg\|min\|max, event, column? } }` —— 末位，必须跟随 B/F 收敛 |
| `characterize` | P | `{ event, tsColumn, monetaryColumn, categoryColumn }` —— 末位；返回一行画像 |
| `exclude` | B | `{ campaignId }` —— 对 suppression 的反连接 |

引擎强制约束：值一律绑定参数（绝不插值）；标识符满足 `^[a-zA-Z0-9_]{1,64}$`；`feature` 必须引用 producer 已写过的特征；引用的原始列必须存在于语义目录（onboard 自动画像）；超预算查询在执行前被拒（EXPLAIN）；`derive` 的幸存集是**实测**的并受 `j_survivor_cap` 限制。

## 走查

```sh
BASE=http://127.0.0.1:8080
# （若配置了认证，请加 -H 'authorization: Bearer …'）

# 1. 注册数据源表（自动画像进目录）
curl -X POST $BASE/sources/onboard -H 'content-type: application/json' -d '{
  "system":"erp","entity":"orders",
  "columns":["user_id","ts","amount","category"],
  "rows":[["u1","2025-01-01T00:00:00Z","100","A"],["u2","2025-01-02T00:00:00Z","10","B"]]}'
# → {"rowsInserted":2,"profiled":true,"columns":["user_id","ts","amount","category"]}

# 2. B：category 为 A 的买家
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}}]}}'

# 3. B 时间算子：流失买家（30 天前买过、窗口内没买）
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"lapsed","event":{"system":"erp","entity":"orders"},
                 "userKey":"user_id","tsColumn":"ts","withinDays":30}]}}'

# 4. F：运行 cadence producer，再按其特征过滤
curl -X POST $BASE/producers/run -H 'content-type: application/json' \
  -d '{"producerId":"cadence_sql","asOf":"2025-12-31T00:00:00Z"}'
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.7}]}}'

# 5. J：幸存集上的即时指标
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id","ops":[
    {"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}},
    {"kind":"derive","name":"revenue_a",
     "metric":{"kind":"sum","event":{"system":"erp","entity":"orders"},"column":"amount"}}]}}'

# 6. P：对比画像（段 vs 全量人群）
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id","ops":[
    {"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}},
    {"kind":"characterize","event":{"system":"erp","entity":"orders"},
     "tsColumn":"ts","monetaryColumn":"amount","categoryColumn":"category"}]}}'
# → 一行：{ profile: { segment:{…}, baseline:{…}, ratios:{…} } }

# 7. 排除已抑制用户（先写回，再反连接）
curl -X POST $BASE/suppression -H 'content-type: application/json' -d '{
  "suppressionId":"11111111-2222-3333-4444-555555555555","campaignId":"c1",
  "userId":"u1","channel":"email","action":"delivered","occurredTs":"2025-01-01T00:00:00Z"}'
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"exclude","campaignId":"c1"}]}}'

# 8. 物化受众（异步任务），再拉取 Parquet
curl -X POST $BASE/jobs -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}}]},
  "materialize":{"campaignId":"c1"}}'          # → 202 { jobId }
curl $BASE/jobs/j_…                            # → { status, done, snapshotId?, error? }
curl $BASE/audience/snap_…                     # → 元数据 + presigned downloadUrl
curl $BASE/audience/snap_…/export?format=parquet&token=…   # → Parquet 字节

# 9. 意图检索（代理组合 DSL 前先发现 schema）
curl "$BASE/catalog?q=buyers%20category&k=5"    # → 带描述的有界候选
```

## 错误

类型化错误 1:1 映射（specs/21 §4）：

| HTTP | 含义 |
| ---- | ---- |
| 400 | DSL 无效 / 边界校验失败（`InvalidDsl`、`InvalidInput`） |
| 401 | bearer 或审批 token 缺失/错误 |
| 404 | 未知任务 / 未物化快照 |
| 413 | 查询对同步过大（超过 `sync_row_cap`） |
| 415 | 不支持的导出格式 |
| 422 | 超过守卫（行数/上限）、幸存集超过 `j_survivor_cap` |
| 503 | 语义目录不可用（embedding/LLM 服务宕机） |

## 原始 SQL 逃生舱

`POST /query { "sql": "…", "approvalToken": "…" }` 在相同守卫下运行已批准的原始 SQL，带审计日志，**仅当**配置了 `sql_approval_token` 且 token 匹配时可用。默认关闭。
