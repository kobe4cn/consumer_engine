# Usage Guide

How to drive the engine over its REST API — for the AI agent and the delivery
system. The wire shapes are normative in
[specs/10-data-model.md §4](../specs/10-data-model.md) and
[specs/21-rest-api.md](../specs/21-rest-api.md); this guide is the runnable
walkthrough.

## Auth

When `auth_token` is configured (production), every request except
`/healthz` + `/readyz` needs:

```
Authorization: Bearer <auth_token>
```

Without it you get `401`. Onboarding and `/producers/run` are
engineer-facing; `/query` and `/catalog` are the agent's surface.

## Endpoints

| Method | Path | Purpose |
| ------ | ---- | ------- |
| GET  | `/healthz`, `/readyz` | liveness / readiness (open) |
| POST | `/sources/onboard` | register + auto-profile a source table |
| POST | `/query` | sync DSL query (small result); `sql` escape hatch w/ approval token |
| GET  | `/catalog?q=…&k=…` | intent retrieval (bounded candidates) |
| POST | `/producers/run` | run a registered feature producer |
| POST | `/suppression` | delivery writeback (idempotent) |
| POST | `/jobs` | async materialise |
| GET  | `/jobs/{id}` | poll a job (`status: running\|done\|failed`) |
| GET  | `/audience/{snapshot_id}` | snapshot metadata + presigned export URL |
| GET  | `/audience/{snapshot_id}/export?format=parquet&token=…` | stream Parquet bytes |

## DSL (the happy path)

A segment is `{ source, key, ops[] }`; every result carries a graded
`freshness` label. Ops (specs/10 §3, specs/12):

| op | kind | notes |
| -- | ---- | ----- |
| `filter` | B | `{ column, op: eq\|ne\|lt\|le\|gt\|ge\|in\|notIn\|like\|notLike, value }` |
| `recency` | B | `{ event, userKey, tsColumn, withinDays }` — bought in last N days |
| `lapsed` | B | `{ event, userKey, tsColumn, withinDays }` — bought before, not within |
| `setOp` | B | `{ op: intersect\|union\|minus, other }` — must be last op |
| `feature` | F | `{ name: "family.short", op, value }` — numeric compare on the wide pivot |
| `derive` | J | `{ name, metric: { kind: count\|sum\|avg\|min\|max, event, column? } }` — terminal, must follow B/F narrowing |
| `characterize` | P | `{ event, tsColumn, monetaryColumn, categoryColumn }` — terminal; returns a profile row |
| `exclude` | B | `{ campaignId }` — anti-join against suppression |

Constraints enforced by the engine: values are always bound parameters
(never interpolated); identifiers follow `^[a-zA-Z0-9_]{1,64}$`; a `feature`
must reference a feature a producer has written; any raw column referenced must
exist in the `semantic_catalogue` (onboard auto-profiles); over-budget queries
are rejected pre-execution (EXPLAIN), `derive` survivor sets are **measured**
and capped at `j_survivor_cap`.

## Walkthrough

```sh
BASE=http://127.0.0.1:8080
# (add -H 'authorization: Bearer …' if auth is configured)

# 1. onboard a source table (profiles it into the catalogue)
curl -X POST $BASE/sources/onboard -H 'content-type: application/json' -d '{
  "system":"erp","entity":"orders",
  "columns":["user_id","ts","amount","category"],
  "rows":[["u1","2025-01-01T00:00:00Z","100","A"],["u2","2025-01-02T00:00:00Z","10","B"]]}'
# → {"rowsInserted":2,"profiled":true,"columns":["user_id","ts","amount","category"]}

# 2. B: buyers of category A
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}}]}}'

# 3. B temporal: lapsed buyers (bought before 30d, not within)
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"lapsed","event":{"system":"erp","entity":"orders"},
                 "userKey":"user_id","tsColumn":"ts","withinDays":30}]}}'

# 4. F: run the cadence producer, then filter on its feature
curl -X POST $BASE/producers/run -H 'content-type: application/json' \
  -d '{"producerId":"cadence_sql","asOf":"2025-12-31T00:00:00Z"}'
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.7}]}}'

# 5. J: a JIT metric over the survivors
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id","ops":[
    {"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}},
    {"kind":"derive","name":"revenue_a",
     "metric":{"kind":"sum","event":{"system":"erp","entity":"orders"},"column":"amount"}}]}}'

# 6. P: comparative profile (segment vs whole population)
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id","ops":[
    {"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}},
    {"kind":"characterize","event":{"system":"erp","entity":"orders"},
     "tsColumn":"ts","monetaryColumn":"amount","categoryColumn":"category"}]}}'
# → one row: { profile: { segment:{…}, baseline:{…}, ratios:{…} } }

# 7. exclude suppressed users (writeback, then anti-join)
curl -X POST $BASE/suppression -H 'content-type: application/json' -d '{
  "suppressionId":"11111111-2222-3333-4444-555555555555","campaignId":"c1",
  "userId":"u1","channel":"email","action":"delivered","occurredTs":"2025-01-01T00:00:00Z"}'
curl -X POST $BASE/query -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"exclude","campaignId":"c1"}]}}'

# 8. materialise an audience (async job), then pull the Parquet
curl -X POST $BASE/jobs -H 'content-type: application/json' -d '{
  "dsl":{"source":{"system":"erp","entity":"orders"},"key":"user_id",
         "ops":[{"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}}]},
  "materialize":{"campaignId":"c1"}}'          # → 202 { jobId }
curl $BASE/jobs/j_…                            # → { status, done, snapshotId?, error? }
curl $BASE/audience/snap_…                     # → metadata + presigned downloadUrl
curl $BASE/audience/snap_…/export?format=parquet&token=…   # → Parquet bytes

# 9. intent retrieval (the agent discovers schema before composing DSL)
curl "$BASE/catalog?q=buyers%20category&k=5"    # → bounded candidates w/ descriptions
```

## Errors

Typed errors map 1:1 (specs/21 §4):

| HTTP | Meaning |
| ---- | ------- |
| 400 | invalid DSL / boundary validation (`InvalidDsl`, `InvalidInput`) |
| 401 | missing/wrong bearer or approval token |
| 404 | unknown job / unmaterialised snapshot |
| 413 | query too large for sync (over `sync_row_cap`) |
| 415 | unsupported export format |
| 422 | guardrail exceeded (rows/cap), survivor over `j_survivor_cap` |
| 503 | semantic catalogue unavailable (embedding/LLM service down) |

## Raw-SQL escape hatch

`POST /query { "sql": "…", "approvalToken": "…" }` runs approved raw SQL under
the same guardrails, audit-logged, **only** when `sql_approval_token` is
configured and the token matches. It is closed by default.
