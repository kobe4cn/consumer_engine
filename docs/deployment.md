# Deployment Guide

How to configure and run the engine in production. The engine is **single
writer / single node** by design (specs/11); horizontal scaling of writers is
out of scope (Postgres-catalog multi-writer is a tracked open risk).

## Configuration

The server reads YAML via `--config <path>` (defaults otherwise). All keys are
`camelCase`; unknown keys are rejected. Example:

```yaml
catalog_path: /var/lib/ce/catalog.db      # DuckLake catalogue (DuckDB file)
data_path: /var/lib/ce/data               # Parquet data location (object storage OK)
bind: "0.0.0.0:8080"
compaction_interval_secs: 3600            # hourly compaction sweep
micro_batch_flush_rows: 50000             # flush threshold (specs/71 §4)

guardrails:
  memoryLimit: "8GB"
  threads: 8
  statementTimeoutSecs: 30
  syncRowCap: 100000
  maxOutputRows: 1000000
  jSurvivorCap: 200000
  enforceCatalogue: true                  # reject non-catalogued columns

suppression:
  perCampaignNoRepeat: true
  frequencyCap: { maxContacts: 3, windowDays: 30 }   # optional; omit to disable

compaction:
  inliningRowLimit: 0                     # every micro-batch → a data file
  targetFileSize: "1MB"

# SECURITY — set these in production:
authToken: "<long-random-token>"          # gates every route (except healthz/readyz)
sqlApprovalToken: "<another-random-token>" # optional: enables the raw-SQL escape hatch

# optional: real HTTP LLM/embedding (spec 13 §4); requires the `semantic-llm` feature build
# llm:
#   baseUrl: "http://llm-service:8080"
#   apiKey: "<key>"
#   embeddingDim: 1536
```

Defaults for every knob are in `consumer_engine_core::EngineConfig`
([crates/core/src/config.rs](../crates/core/src/config.rs)) — a config file
with just `catalog_path` + `data_path` is valid.

## Security checklist (specs/70)

- **Set `authToken`** — a tokenless engine lets any caller mint presigned
  exports (IDOR). The token is hashed and compared in constant time.
- **Set `sqlApprovalToken` only if you need the raw-SQL hatch**; it is
  audit-logged and runs under the same guardrails. Default: closed.
- Presigned export URLs expire in 15 minutes (`EXPORT_TTL_SECS`), are
  HMAC-bound to the snapshot, and access is logged (snapshot id only).
- Request `Debug` never prints tokens/secrets (redacting impls + tests).
- The engine stores pseudonymous `user_id` only; PII resolution stays in
  source/delivery systems (specs/10 I1).
- No `PRIMARY KEY` on any table — identity/dedup is enforced by the writer
  path (suppression writeback idempotency via `suppressionId`; feature rows
  append-only by `as_of_ts`).

## Operational notes

- **Single-writer invariant**: exactly one server process may attach a given
  catalogue; a second is refused (`WriterAlreadyHeld`). Restart is safe —
  committed data is durable (write-through), and a crashed in-flight write is
  simply absent (idempotent client retries cover suppression).
- **Compaction**: micro-batches accumulate small files (inlining disabled);
  the hourly sweep (`compaction_interval_secs`) calls
  `ducklake_merge_adjacent_files`. Verified: file count drops, rows intact,
  snapshot history retained. Tune `compaction.targetFileSize` against your
  storage (object-storage per-file latency is an open risk — specs/71 §4).
- **Reads** re-attach the DuckLake catalogue per query (P1-1) — fine at low
  QPS; the read-connection-pool fix is tracked in
  [perf-calibration.md](research/perf-calibration.md).
- **Performance targets are not met at scale yet** (measured B/F/J/P P50
  2.5–15 s at 50k rows). Calibrate with `make bench-queries` and track the
  unblocking path before committing to a latency SLO.
- **Logs**: structured `tracing` (JSON in production). No user values are
  logged for `pii_flag` columns; approval-token audits log the SQL text.

## Build variants

```sh
cargo build --release                       # default: stub LLM/embedding (no network)
cargo build --release --features semantic-llm   # HTTP LLM/embedding clients
```

Enable `semantic-llm` **and** set `llm` in config to use a real model service;
otherwise the deterministic stubs run (deterministic, no network).

## Health / readiness

`GET /healthz` (liveness) and `GET /readyz` (readiness) are open and return
200 once the writer + reader are wired (i.e., at build time).

## Known limitations (do not rediscover)

All tracked in [specs/93-improvements-review.md](../specs/93-improvements-review.md):

- DuckDB `EXPLAIN` exposes only row estimates — scan/memory budgets are
  runtime-bounded (PRAGMA + timeout), not pre-flight.
- This DuckDB build has **no** server-side statement-timeout PRAGMA; the
  tokio timeout is the backstop (a runaway query may briefly occupy the single
  reader thread).
- Reading a table at a historical snapshot (time-travel) is not resolvable in
  this DuckLake build (`AS OF` / `ducklake_scan` APIs reject); snapshot
  history is retained.
- Multi-tenant isolation (tenant_id schema) is not implemented; authN is.
  Single-tenant by construction.
