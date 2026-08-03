# Spike: DuckDB VSS — vector column type and HNSW index engagement

Status: Done · Owner: platform · Date: 2025-08-03 · Outcome: **PASS**

Environment: DuckDB CLI **v1.5.4**, `vss` extension loaded via `INSTALL vss;
LOAD vss`. Runnable artefacts: `/tmp/ce-spikes/vss_official.sql`,
`vss_idx.sql`, `vss_final.sql`.

## Question

[10-data-model.md](../../specs/10-data-model.md) originally specified vector
features as `FLOAT[]` variable lists. Can the phase-2 **S** (similarity)
capability use HNSW acceleration on that type, or must vectors be fixed-size
`FLOAT[N]`? This gates the `feature_store` / `semantic_catalog` schema we lock
**now** even though S ships later (D8).

## Method

Build `FLOAT[128]` and `FLOAT[]` tables, attempt `CREATE INDEX ... USING HNSW`,
then `EXPLAIN` top-k cosine queries with both a non-constant (`random()`
subquery) and a constant (`[...]::FLOAT[4]`) query vector.

## Findings

1. ✅ **HNSW requires fixed-size `FLOAT[N]`.** `CREATE INDEX hnsw ON t(emb)
   USING HNSW` over a `FLOAT[]` (variable list) column → `Binder Error: HNSW
   index keys must be of type FLOAT[N]`. Variable lists cannot be indexed.
2. ✅ **The index engages only with a constant query vector.**
   `ORDER BY 1 - array_cosine_similarity(emb, <const>) LIMIT k` **and**
   `ORDER BY array_cosine_distance(emb, <const>) LIMIT k` both produce
   `HNSW_INDEX_SCAN ── HNSW Index: hnsw`. A non-constant query vector (e.g. a
   `random()` subquery) degrades to `SEQ_SCAN` + `TOP_N`.
3. ⚠ **Index build is not free.** Building HNSW over 50 000 × 128-dim ≈
   **2.0 s wall / 27.7 s CPU**. Must run **offline** as part of a producer,
   never at query time.
4. ✅ **top-k latency with index** over 50k vectors < 1 ms (timed run).

## Decision

**GO.** Binding rules adopted now so phase 2 needs no schema redesign:

- **[10]**: vector features do **not** live in the generic (scalar, EAV)
  `feature_store`. Each vector feature gets its **own** table
  `feature_vec_<name>(user_id TEXT, emb FLOAT[<dim>])` with its own HNSW index.
  `feature_store` keeps **scalar** features only. `semantic_catalog.embedding`
  is `FLOAT[<dim>]` (fixed).
- **Phase-2 S compiler**: the anchor/query vector must be bound as a
  **literal or parameter**, never a per-row subquery, or the index is bypassed.
- **Producer contract (D9)**: a vector producer is responsible for creating the
  HNSW index and rebuilding it on update (offline); the query path only reads.

## Risks identified

- **R1 (pinned by test)**: a regression test `test_should_engage_hnsw_for_topk`
  asserting `HNSW_INDEX_SCAN` appears in `EXPLAIN` for the canonical query —
  guards against vss-version optimizer regressions.
- **R2 (open)**: index build time at production scale (50 M users × 128-dim)
  and incremental-update strategy (rebuild vs HNSW insert API) — follow-up
  spike when S is scheduled.
- **R3**: pin the `vss` extension version in CI to lock optimizer behaviour.
