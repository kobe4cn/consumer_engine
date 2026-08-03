# 70-security: Threat Model, PII Boundary, Guardrails-as-DoS-Defense

Status: draft · Depends on: [10](./10-data-model.md), [21](./21-rest-api.md)

## 1. Purpose

Security is hostile-input defense; safety is Rust soundness. This spec covers
the former (AGENTS.md § Safety & Security, § Input Validation, § Injection
Prevention, § Resource Limits, § Cryptography). Soundness (`#![forbid(
unsafe_code)]`, no panic on external input, checked arithmetic on external
integers) is enforced crate-wide per AGENTS.md and is not re-argued here.

## 2. Trust boundaries & data classification

```text
  EXTERNAL (hostile)                         ENGINE (trusted after validation)
  ─────────────────                         ────────────────────────────────
  agent REST calls     ──▶ /query,/jobs ──▶ Ingress validation ──▶ QueryEngine
  delivery writeback   ──▶ /suppression ──▶ IngestionActor Q3
  source CDC/batch     ──▶ adapter        ──▶ IngestionActor Q1
  LLM/embedding API    ──▶ (outbound)     ──▶ Profiler/RAG (timeout/retry)
                                             │
                              PII never crosses into engine (D12):
                              user_id is pseudonymous everywhere inside.
```

- **Data classes**: pseudonymous `user_id` (low sensitivity), feature values
  (derived), snapshots (operationally sensitive — who was targeted), catalogue
  descriptions. **No raw PII class exists inside the engine** (D12).
- **Boundary modules** (`ingress`, adapter front-ends, LLM client) carry the
  `cargo clippy -W clippy::unwrap_used -W clippy::indexing_slicing -W
  clippy::panic -W clippy::expect_used` lint set (AGENTS.md).

## 3. Invariants

- **I1 Reject, don't sanitise.** Invalid input is rejected at the boundary
  (AGENTS.md). Examples: `campaign_id`/`feature_name` must match
  `^[a-zA-Z0-9_.-]{1,64}$`; utterances capped at N bytes; DSL AST validated
  structurally (I5 in [10](./10-data-model.md)).
- **I2 Parameterised SQL only** on the DSL path; the raw-SQL escape hatch
  requires an approval token and is audit-logged ([12 I3](./12-query-engine.md),
  AGENTS.md § Injection Prevention).
- **I3 AuthN/AuthZ every request + tenant isolation** ([21 I1/I2](./21-rest-api.md)).
  IDOR-class risk: `/audience/:id` verifies the caller's tenant owns the
  snapshot; `/suppression` writeback verifies the delivery system's scope.
- **I4 Guardrails = DoS defense.** Memory/timeout/row/scan budgets
  ([71](./71-performance-budgets.md)) are the primary DoS mitigation against a
  runaway or hostile agent query. Concurrency is `Semaphore`-bounded.
- **I5 No secrets in logs.** `tracing` structured fields only; request DTOs
  derive a redacting `Debug` (AGENTS.md § Cryptography & Secrets). A unit test
  asserts no `Authorization`/token appears in formatted log output.
- **I6 Constant-time where it matters.** AuthN token comparison uses
  `subtle::ConstantTimeEq`; tokens are ≥256-bit CSPRNG (`OsRng`/`getrandom`),
  never `thread_rng` (AGENTS.md § Cryptography).

## 4. Behaviour / residual risks

- **Agent prompt-injection via catalogue descriptions**: an attacker who can
  poison a `semantic_catalog.description` could steer the agent. Mitigation:
  descriptions are write-protected (onboarding + human edit only, D4), and the
  agent composes the DSL from **retrieved column names**, not by executing
  description text as SQL. Logged as a tracked risk.
- **LLM outbound SSRF**: the LLM/embedding endpoint URL is config-pinned, not
  caller-supplied; no user-controlled outbound URLs (AGENTS.md § URL/SSRF).
- **Snapshot leakage**: presigned URLs are short-lived + scoped + logged
  ([21 I4](./21-rest-api.md)); snapshots never contain PII (D12).

## 5. Cross-references

- ← Depends on: [10](./10-data-model.md), [21](./21-rest-api.md),
  [12](./12-query-engine.md).
- ↔ Perf budgets are the DoS mechanism: [71](./71-performance-budgets.md).
- Norms: AGENTS.md § Safety & Security, § Input Validation, § Injection,
  § Resource Limits, § Cryptography.
