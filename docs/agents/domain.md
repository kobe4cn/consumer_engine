# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root, or
- **`CONTEXT-MAP.md`** at the repo root if it exists — it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront.

**Effective domain glossary for this repo**: until `CONTEXT.md` exists, the
normative glossary is [`specs/80-glossary.md`](../specs/80-glossary.md) — its
vocabulary (Segment vs Snapshot; the capability codes **B / F / J / S / P**;
Producer; Feature Store; Freshness; Suppression; Profiler; Intent RAG) is
binding across the spec set and should be used verbatim in issues, test names,
and code identifiers.

## File structure

Single-context repo:

```
/
├── CONTEXT.md            (lazily created)
├── docs/adr/             (lazily created)
├── docs/research/        (spike/study/survey memos — research skill)
├── specs/                (numbered spec set — spec skill)
└── src/  (crates/*, apps/*)
```

## Use the glossary's vocabulary

When your output names a domain concept (issue title, refactor proposal,
hypothesis, test name), use the term as defined in `specs/80-glossary.md`. Don't
drift to synonyms the glossary explicitly avoids.

## Flag ADR conflicts

If your output contradicts an existing ADR (or a locked decision in
`specs/99-key-decisions.md`), surface it explicitly rather than silently
overriding.
