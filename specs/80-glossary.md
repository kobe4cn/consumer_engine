# Glossary

Disambiguates terms that are overloaded across this spec set. Define only the
words two readers would use differently.

## Audience / Segment / Snapshot

- **Segment** — a *virtual* definition: a DSL expression that, evaluated
  against the lake at a point in time, yields a set of user IDs. A segment is
  cheap to describe, expensive to trust until materialised.
- **Audience package / `audience_snapshot`** — the *materialised* artefact: a
  versioned DuckLake table row-set `(snapshot_id, user_id, frozen features,
  hit_reason, as_of_ts)`. Marketing and compliance reason over **snapshots**,
  never over live segment re-evaluation. (D10)
- **`as_of_ts`** — the data cut-off a result reflects. Distinct from the
  wall-clock time the snapshot was written. Time-travel queries key off this.

## The capability codes (normative)

- **B — Boolean / temporal-relational.** Predicates over raw event/order tables
  (`Filter`, `Lapsed`, `Recency`, set ops). Pure SQL. **Not bounded** — any
  relational composition is allowed.
- **F — Feature predicate.** A filter over a precomputed per-user value in
  `feature_store` (e.g. `cadence_regularity > 0.7`). Bounded by the Feature
  Store catalogue.
- **J — Just-in-time derived metric.** A metric computed at query time **only**
  over the survivor set left by B+F, under guardrails. Bounded by Q4-style
  timeouts/row caps. Use when the metric is not worth precomputing but the
  survivor set is small.
- **S — Similarity / lookalike.** Top-k nearest neighbours to an anchor
  (seed user or seed segment) in a precomputed vector space. **Phase 2.**
- **P — Profile / characterisation.** A comparative description of a segment
  vs the whole population ("this segment's AOV is 2.3× the baseline").

## Feature Store, Producer, Materialisation

- **Feature Store** — the DuckLake `feature_store` table + its wide pivot views.
  Per-user, point-in-time-correct precomputed values. The single seam through
  which any "smarter than SQL" signal (SQL aggregation today, ML score
  tomorrow) enters the query layer. (D9)
- **Producer** — an offline job that writes `(user_id, feature_name, value,
  as_of_ts)` rows into the Feature Store via `IngestionActor`. v1 producers are
  SQL; phase-2 producers are ML model scorers. Same interface. (D9)
- **Materialisation** — writing a segment's result into `audience_snapshot`
  through `IngestionActor` queue Q2. Always asynchronous.

## Freshness / Suppression / Profiler

- **Freshness (graded)** — data recency is **per source**: a CDC source is
  minute-fresh; a batch-only source is stale by its batch interval. Every result
  carries a `freshness` label so the operator is never silently misled. (D5)
- **Suppression** — the durable record that a user has been targeted/delivered/
  converted/opted-out for a campaign. Written back by the external delivery
  system; consumed by `Exclude`. Forms the closed loop. (E1, §13)
- **Profiler (L0)** — the onboarding-time agent that ingests a new `raw_*`
  table, infers types/relationships/PII flags, samples values, and writes the
  semantic catalogue. Runs **once per onboarding**, never at query time. (D4)
- **Intent RAG (L1)** — at query time, embeds the operator utterance and
  retrieves the relevant tables/columns from the semantic catalogue *before*
  DSL construction. Reduces hallucinated columns.

## Trust boundaries (normative)

- **Engine boundary** — the REST API surface ([21](./21-rest-api.md)). Every
  value crossing it is hostile until validated (AGENTS.md § Safety & Security).
- **PII boundary** — the engine **never** holds raw PII; only pseudonymous
  `user_id`. PII resolution is a source/delivery-system concern. (D12)
- **Writer boundary** — exactly one `IngestionActor` may write DuckLake. The
  query path is **read-only**. (D3)

## Cross-references

- Decisions behind these terms: [99-key-decisions.md](./99-key-decisions.md).
