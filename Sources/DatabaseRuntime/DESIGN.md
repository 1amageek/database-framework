# Database Runtime Composition

## Purpose and Scope

This module owns trait-selected composition of DatabaseEngine extension points.
Parent: [database-framework](../../DESIGN.md). No children.

## Responsibilities and Boundaries

Composition installs only capabilities selected by package traits. It provides
schema-driven entity construction without introducing synthetic application
models. Application policy factories remain explicit injected inputs.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Configuration](../DatabaseEngine/Configuration/DESIGN.md) | depends on | Immutable capability factories | Preserves generation composition | Missing policy remains fail-closed |
| [Schema](../DatabaseEngine/Schema/DESIGN.md) | used by | Candidate runtime construction | Admits transactional SQL DDL | No transport or server dependency |

## Architecture

```text
SwiftPM traits -> providers/readers/maintainers
application -> authorization handlers + optional dynamic policy factory
  -> DatabaseRuntimeConfiguration -> immutable generation
```

## Contracts and Invariants

Schema-driven factory construction uses the same trait-selected provider set as
initial composition. The optional application authorization factory survives
candidate cloning and is never replaced with a synthesized allow policy.

## Verification and Change Impact

Schema-driven native and portable DDL tests exercise provider construction.
Changes must preserve initial/restored runtime equality and explicit policy
behavior in candidate generations.
