# Domain Docs

How the engineering skills should consume this repo's domain documentation when
exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root: the domain glossary.
- **`docs/adr/`**: read ADRs that touch the area you're about to work in.
- **`docs/features/<feature>.md`**: durable documentation of a shipped feature's
  behaviour. Read the one covering your area if it exists.

If any of these files don't exist, **proceed silently**. Don't flag their
absence; don't suggest creating them upfront. The `/domain-modeling` skill
(reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates
them lazily when terms or decisions actually get resolved.

## File structure

This is a single-context repo:

```
/
├── CONTEXT.md
├── docs/
│   ├── adr/
│   │   ├── 0001-target-platform-vertx-4-5-11-scala-3-3-lts.md
│   │   └── …
│   └── features/
│       └── link-attributes.md
└── src/main/scala/
```

If this ever grows into several independent contexts, the multi-context layout is
a root `CONTEXT-MAP.md` pointing at one `CONTEXT.md` per context, with
context-scoped ADRs under `src/<context>/docs/adr/`.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal,
a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift
to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're
inventing language the project doesn't use (reconsider) or there's a real gap
(note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than
silently overriding:

> _Contradicts ADR-0007 (link attribute values are positional), but worth
> reopening because…_
