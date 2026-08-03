# Improvements Review — Deferred Findings Backlog

Findings deferred out of their originating phase, with severity, citation, and a
one-line fix shape so a later phase can pick them up without re-deriving context.
Append-only.

## From Phase 1 / T1 (M0) — engine spine

### P1-1 — Read path must refresh the DuckLake snapshot per query

- **Citation**: `crates/execution/src/lib.rs` (`reader_loop`, `DETACH dro; <attach_sql>` before every query).
- **Finding**: a long-lived read-only DuckLake attach is **pinned to the snapshot
  at attach time** — it does not see tables/commits made afterward. T1 works
  around this by re-issuing `DETACH dro; ATTACH ... AS dro (READ_ONLY)` before
  every query.
- **Why deferred**: correct and fast enough for T1's read load; the cost is one
  detach+attach per query (~ms).
- **Fix shape (later phase)**: when read QPS matters (T2 perf calibration,
  `71-performance-budgets.md`), either use a small read-connection pool that
  re-attaches on a cadence rather than per query, or check DuckLake for a
  snapshot-refresh API that avoids full re-attach.

### P2-2 — `value_to_json` maps temporal/decimal/struct/map to null

- **Citation**: `crates/execution/src/lib.rs` (`value_to_json` catch-all).
- **Finding**: T1's query surface only produces `VARCHAR` and `BIGINT`, which are
  mapped precisely; other DuckDB types fall back to null.
- **Fix shape**: extend the match arms when the DSL (T2) or feature predicates
  (T4) start returning those types (timestamps → ISO-8601 string, decimals →
  number, struct/map → JSON object).

### P3-3 — Ingress validation is manual regex, not the `validator` crate

- **Citation**: `crates/ingress/src/lib.rs` (`validate_ident`).
- **Finding**: boundary validation is correct but hand-rolled; AGENTS.md § Input
  Validation recommends the `validator` crate.
- **Fix shape**: derive `Validate` on the request DTOs when they grow beyond
  T1's two endpoints; keep `validate_ident` semantics.

### P3-4 — Micro-batch is passthrough on T1

- **Citation**: `crates/ingestion/src/lib.rs` (`IngestRaw` flushes immediately).
- **Finding**: each onboard batch flushes via the writer's multi-row insert; the
  `micro_batch_flush_rows` config is not yet exercised. Real cross-call
  accumulation lands with the CDC adapter.
- **Fix shape**: in the CDC-adapter phase, accumulate rows per `(system, entity)`
  in the actor and flush on `micro_batch_flush_rows` / age.

### P3-5 — Object-storage per-file latency unbenched (carried from research)

- **Citation**: `docs/research/spike-microbatch-compaction.md` R1.
- **Finding**: T1 ran on local SSD; the 30 s / 50 k flush numbers are unvalidated
  on S3/OSS.
- **Fix shape**: bench on the target object storage before locking the flush
  interval; do this when a real object-storage target is chosen.

### P3-6 — Scoped `std::fs` allow for advisory locking

- **Citation**: `crates/storage/src/lib.rs` (`Writer::_lock` field + `OpenOptions`
  in `Writer::attach`).
- **Finding**: `fs2::FileExt` (advisory file lock enforcing the single writer,
  D3) is only impl'd for `std::fs::File`, not `tokio::fs::File`; this is a
  synchronous startup op outside the async runtime. `#[allow(clippy::
  disallowed_types)]` is scoped here with that justification.
- **Fix shape**: none unless the lock mechanism changes; keep the allow + comment.
