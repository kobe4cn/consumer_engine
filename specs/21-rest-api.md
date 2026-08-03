# 21-rest-api: REST Surface, Auth/Tenancy, Payload Modes, External Contract

Status: draft · Depends on: [12](./12-query-engine.md), [13](./13-semantic-layer.md),
[20](./20-ingestion.md)

## 1. Purpose

The single trust boundary between the (Python/TS) agent / external delivery
systems and the Rust core (D16). All transport is **REST** (D13). Owns auth,
tenancy isolation, payload-size modes, and the bidirectional contract with
delivery systems (E1). Every value crossing this boundary is hostile until
validated (AGENTS.md § Safety & Security, § Input Validation).

## 2. Interface (full surface)

| Method | Path | Purpose | Caller |
| -- | ---- | ------- | ------ |
| POST | `/query` | sync DSL query (small result) | agent |
| POST | `/jobs` | async materialise / heavy query | agent |
| GET  | `/jobs/:id` | poll job | agent |
| GET  | `/audience/:snapshot_id` | snapshot metadata + presigned URL | agent / delivery |
| GET  | `/audience/:snapshot_id/export?format=parquet` | binary export | delivery |
| POST | `/suppression` | **writeback** from delivery (E1) | delivery |
| POST | `/sources/onboard` | register + profile a new source table | engineer/agent |
| GET  | `/catalog` | intent retrieval (L1) for agent | agent |
| GET  | `/healthz`, `/readyz` | liveness/readiness | ops |

Wire shapes: [10 §4](./10-data-model.md). JSON is `camelCase`; all instants
ISO-8601 UTC; all request bodies `#[serde(deny_unknown_fields)]` and validated
with `validator` (AGENTS.md § Input Validation: length/range caps, charset
allowlists on every external string, e.g. `campaign_id` `^[a-zA-Z0-9_-]{1,64}$`).

## 2a. Request lifecycle (sync query)

```text
 Agent                Ingress(axum)            QueryEngine          DuckDB(read-only)
   │  POST /query {dsl}  │                         │                      │
   │ ───────────────────▶│ validate + authz + tenant│                      │
   │                     │ ───────────────────────▶│ plan() EXPLAIN       │
   │                     │                         │ guardrail verdict    │
   │                     │                         │ mode? sync─▶ run_sync│
   │                     │                         │ ────────────────────▶│ execute (timeout)
   │                     │                         │ ◀────────────────────│ rows (capped)
   │                     │ ◀────────────────────── │ SyncResult+freshness │
   │ ◀───────────────────│ 200 {rows,count,freshness}                    │
   │                     │                         │                      │
   │           (if TooLarge/planned-async)         │                      │
   │ ◀───────────────────│ 202 {jobId} ──▶ /jobs/:id ──▶ /audience/:id    │
```

## 3. Invariants

- **I1 AuthN + AuthZ on every request.** No endpoint trusts network position
  (AGENTS.md § AuthN/AuthZ). `/sources/onboard` and `/suppression` require
  elevated scopes; `/query` requires tenant-scoped read.
- **I2 Tenant isolation.** Every query/snapshot/suppression is scoped by
  `tenant_id` from the auth token; cross-tenant access is impossible by
  construction (the compiler injects `tenant_id` into every SQL, never trusts
  caller-supplied filtering).
- **I3 Body limits.** Request bodies capped (`DefaultBodyLimit`, default 1 MB
  for JSON; `/export` and bulk suppression use streaming + counted-reader, not
  `read_to_end`). No decompression-bomb surface (AGENTS.md § Resource Limits).
- **I4 Presigned URLs expire.** Audience export URLs are short-lived (default
  15 min) and scoped to the snapshot; access is logged.
- **I5 Timeouts on every IO.** Ingress wraps each handler in
  `tokio::time::timeout`; downstream LLM/DuckDB calls have their own budgets
  ([71](./71-performance-budgets.md)).

## 4. Behaviour

- **External delivery contract (E1).** Delivery systems `GET` a snapshot to
  know whom to send, then `POST /suppression` per outcome. Both are REST; the
  writeback is idempotent (client supplies `suppression_id` or a dedupe key).
  This is the **only** external write path into the engine.
- **Payload modes (D13).** Small sync → JSON inline. Large/materialised → the
  snapshot is written by Q2, and `/audience/:id` returns a presigned Parquet
  URL (or `/export` streams Parquet bytes). The engine never returns > 1 M rows
  inline.
- **Escape hatch (D2).** `POST /query` with `{ "sql": "...", "approvalToken":
  "..." }` runs approved raw SQL under the same guardrails; without a valid
  token it is rejected. Always audit-logged.
- **Errors** map 1:1 to typed `QueryError`/`IngestionError` via an `axum`
  `IntoResponse`; no `unwrap`, no leaking internal messages (PII-free by D12
  but still no stack traces to clients).

## 5. Cross-references

- ← Depends on: [12](./12-query-engine.md), [13](./13-semantic-layer.md),
  [20](./20-ingestion.md).
- → Consumed by: the Python/TS agent (D16), the external delivery system.
- Norms: AGENTS.md § Safety (boundary validation), § AuthN/AuthZ, § Resource
  Limits (body/timeouts/concurrency).
