# 13-semantic-layer: L0 Profiler (onboarding) + L1 Intent RAG

Status: draft · Depends on: [10](./10-data-model.md), [11](./11-runtime-core.md)

## 1. Purpose

Solves the central problem: *how does the agent know what tables/columns exist
and what they mean*, without hallucinating. Two stages:

- **L0 Profiler** — runs **once per source onboarding** (D4), discovers a
  `raw_*` table's structure, classifies columns, infers relationships, and
  writes `semantic_catalog` ([10 §2](./10-data-model.md)).
- **L1 Intent RAG** — runs **at query time**, embeds the operator utterance,
  retrieves the relevant tables/columns from the catalogue, and hands the agent
  a *bounded candidate set* to compose the DSL from. No agent invents columns.

This is the layer that makes D2 (DSL, not free SQL) viable at all.

## 2. Interface

```text
// Onboarding (engineer / agent triggers once)
pub struct Profiler { /* duckdb read, llm client */ }
impl Profiler {
    pub async fn onboard(&self, system: &str, table: &str) -> Result<CatalogDelta>;
    // samples rows, infers types/PII/FK, generates descriptions + embeddings,
    // returns the delta to write via IngestionActor (Q1 meta).
}

// Query time
pub struct IntentRag { /* catalogue read, embedding model */ }
impl IntentRag {
    pub async fn retrieve(&self, utterance: &str, k: usize) -> Result<Vec<CatalogHit>>;
    // returns bounded candidate tables/columns with semantic_type + description.
}
```

## 2a. Onboarding & query-time flow

```text
  ONBOARDING (rare, controlled — D4)               QUERY TIME (per request)
                                                   
  raw_* table exists                               operator utterance
        │                                                │
        ▼                                                ▼
  Profiler.sample (≤N rows, bounded)              IntentRag.retrieve (embed utterance,
        │                                          top-k from semantic_catalog)
        ▼                                                │
  classify: type / pii / fk / measure / dim            bounded candidate set
        │                                          (tables, columns, semantic_type)
        ▼                                                │
  LLM description + embedding ──▶ agent composes DSL (only named columns)
        │                                                │
        ▼                                                ▼
  IngestionActor Q1 (meta) ──▶ semantic_catalog   QueryEngine (12)
```

## 3. Invariants

- **I1 Onboarding-only writes.** `semantic_catalog` is written only through the
  Profiler via `IngestionActor` (D4). The query path never writes it.
- **I2 Bounded sampling.** Profiler samples at most `sample_rows` (default 1000)
  rows and truncates each value to `sample_value_bytes` (default 64). No
  unbounded `SELECT *` (AGENTS.md § Resource Limits).
- **I3 Bounded retrieval.** `IntentRag::retrieve` returns at most `k` (default
  20) hits; the agent cannot enumerate the whole catalogue.
- **I4 PII never embedded.** `sample_values` with `pii_flag=true` are redacted
  before any embedding/description generation (D12); only the *description* of
  a PII column is embedded, never its values.
- **I5 Freshness of catalogue.** The catalogue records the source snapshot it
  was built from; if the source schema drifts (column added), a re-onboard
  produces a versioned delta. The query path warns if a referenced column's
  catalogue entry is older than the source's latest snapshot.

## 4. Behaviour

- **LLM coupling.** Both Profiler descriptions and IntentRag embeddings call an
  external embedding/LLM service over HTTP with timeout + retry budget
  (AGENTS.md § Resource Limits). Failures degrade gracefully: a Profiler LLM
  failure leaves a human-editable stub description; an IntentRag failure returns
  `Error::CatalogueUnavailable` (the agent must not then guess columns).
- **Editability.** `semantic_catalog.description` is human-editable; edits are
  versioned and re-embedded. This is the lever for correcting LLM
  mis-descriptions.
- **Cost guard.** Profiler is expensive (LLM per column); gated behind the
  onboarding endpoint with authz ([70](./70-security.md)), not callable per
  query.

## 5. Cross-references

- ← Depends on: [10](./10-data-model.md), [11](./11-runtime-core.md).
- → Consumed by: [21](./21-rest-api.md) (`/sources/onboard`, `/catalog`),
  the agent (Python/TS) which calls IntentRag before composing DSL.
- Norms: AGENTS.md § Safety (bounded sampling, PII redaction), § Logging
  (structured, no value logging for pii_flag columns).
