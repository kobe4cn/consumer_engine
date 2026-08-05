# PRD — Consumer Engine (AI-Agent-facing Audience Filtering Engine)

Status: draft v1 · Owner: platform · Last updated: 2025-08-03

## 1. Problem

Enterprise marketing today selects campaign audiences by **hand-written filter
rules** over scattered business tables (users, behaviour, orders, order lines,
products): "bought SKU A in the last 30 days", "similar spend in the past 12
months", "spent ≥ X", "exclude users who already joined a prior campaign".

The failure modes are concrete:

- **Slow & expert-bound.** Every campaign needs a data engineer to translate a
  marketer's intent into SQL. Turn-around is days, not minutes.
- **Opaque & unauditable.** The rule that picked a user lives in a one-off
  query; there is no record of *why* a user was targeted, so post-campaign
  analysis and compliance review are guesswork.
- **No feedback loop.** "Exclude already-participated users" is bolted on per
  campaign; suppression is re-derived each time and drifts.
- **Brittle to schema change.** When a source table adds a column, every
  hand-written rule silently keeps using the old shape.

## 2. Vision

A **Rust engine** that turns a marketing operator's **natural-language intent**
into an **audience package** (a versioned, materialised snapshot of pseudonymous
user IDs plus their frozen features and the reason each was selected), through
a **REST API designed for an AI agent** to drive. The agent never writes raw
SQL on the happy path: it discovers schema via a semantic catalogue, composes a
**structured DSL**, and the engine compiles it to guarded DuckDB SQL over a
**DuckLake** house on object storage. Prediction/similarity enter later through
the same **Feature Store** seam, with zero runtime redesign.

Load-bearing ergonomic contract (pins the agent↔engine surface):

```text
# The agent, given "找最近30天买过SKU A、且之前有周期性复购但近30天没买的人":
POST /query
{ "dsl": {
    "Segment": "Orders",
    "ops": [
      { "Filter":   { "sku": "A" } },
      { "Distinct": "user_id" },
      { "Lapsed":   { "within_days": 30, "of": { "sku": "A" } } },
      { "Feature":  { "name": "cadence_regularity", "op": ">", "value": 0.7 } }
    ] } }
→ 200 { "rows": [...], "count": 48213, "freshness": { "source": "batch", "lag_seconds": 6 } }
```

## 3. Goals

| #  | Goal | Measure |
| -- | ---- | ------- |
| G1 | Agent composes audiences from natural language without a human SQL author | ≥ 80% of campaign segments authored end-to-end by the agent with no SQL written by a human |
| G2 | Every selected user is auditable | 100% of `audience_snapshot` rows carry `hit_reason` + frozen features; any user_id resolves to why/when selected |
| G3 | Filtering latency on the common path | P50 sync query < 1 s, P99 < 5 s over a ≤ 50 M-user corpus (see 71) |
| G4 | Closed suppression loop | A user targeted by campaign C is excluded from C's re-runs by default, enforced by the engine not the caller |
| G5 | Onboarding a new source table is self-service | A new `raw_*` table is queryable by the agent in < 30 min from ingest, with auto-generated semantic descriptions |
| G6 | Prediction-ready without redesign | Adding the first ML propensity score requires **no** change to the query/runtime layer (only a new Feature Store producer) |

## 4. Non-goals (explicit — these prevent scope creep)

- **NG1 — Send-time scheduling.** Deciding *when* to contact a user is the
  downstream delivery system's job. This engine produces audiences and consumes
  suppression; it does not orchestrate sends. (D15)
- **NG2 — ML training pipeline in v1.** Propensity / next-product / similarity
  *models* are phase 2. v1 builds the Feature Store **producer interface** so
  model scores plug in later; it does not train models. (D8)
- **NG3 — Raw PII custody.** The engine holds **pseudonymous user IDs only**.
  Email/phone/name resolution stays in source/delivery systems. (D12)
- **NG4 — Free-form SQL for operators.** The happy path is the DSL. Raw SQL is
  an escape hatch behind human approval, never the default. (D2)
- **NG5 — Multi-writer lake.** Exactly one writer (`IngestionActor`) owns the
  DuckLake catalog connection. No second writer process. (D3)

## 5. Users

- **Primary — Marketing operator (non-technical).** Types intent in natural
  language; consumes the resulting audience + its characterisation. Never sees
  SQL, never sees raw PII (sees pseudonymous IDs).
- **Secondary — AI agent (Python/TS orchestration).** The actual caller of the
  REST API: runs intent retrieval, fills the DSL, interprets characterisations,
  decides sync vs async. The engine is built for *this* caller.
- **Secondary — Delivery/触达 system (external).** Pulls audience snapshots,
  performs sends, writes back suppression events.
- **Secondary — Data/platform engineer.** Onboards new source tables, registers
  feature producers, operates the engine.
- **Anti-persona — Ad-hoc BI analyst.** This is not a general SQL notebook;
  point them at the raw lake directly.

## 6. Success metrics

- % of segments agent-authored without human SQL (G1).
- Median segment authoring latency (intent → snapshot ready).
- Audit completeness: every snapshot row has non-null `hit_reason` (G2).
- Query P50/P99 vs budget (71); guardrail rejection rate (should be low — a high
  rate means the DSL is too weak and agents escape to SQL too often).
- Suppression correctness: zero re-targeting of suppressed users in regression
  tests (G4).
- Time-to-queryable for a newly ingested table (G5).

## 7. Naming conventions (binding)

- **Crate package names**: `consumer_engine-<short>` (e.g. `consumer_engine-query`),
  living in `crates/<short>`. Binary: `consumer_engine-server` in `apps/server`.
  See [61-crates-and-features.md](./61-crates-and-features.md).
- **DuckLake tables**: `raw_<system>_<entity>` (mirrored sources);
  `feature_store`, `audience_snapshot`, `suppression`, `semantic_catalog`
  (engine-owned). See [10-data-model.md](./10-data-model.md).
- **Snapshot identity**: `snapshot_id` (UUIDv7, time-ordered), `campaign_id`
  (caller-supplied opaque string), `as_of_ts` (the data cut-off the snapshot
  reflects, **not** the materialisation wall-clock).
- **REST**: kebab-case paths (`/audience/:snapshot_id`), JSON `camelCase`
  (`#[serde(rename_all = "camelCase")]` per AGENTS.md § Serialization).
- **Capabilities**: the single-letter codes **B / F / J / S / P** are normative
  across the whole spec set (see [80-glossary.md](./80-glossary.md)).

## Cross-references

- → Defines scope consumed by every other spec.
- ↔ Key decisions: [99-key-decisions.md](./99-key-decisions.md) D2, D8, D12, D15.
