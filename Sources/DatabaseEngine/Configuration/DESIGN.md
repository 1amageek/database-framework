# Runtime Configuration

## Purpose and Scope

This component owns explicit immutable runtime capability composition for one
schema generation. Parent: [DatabaseEngine](../DESIGN.md). No children.

## Responsibilities and Boundaries

Runtime configuration retains installed executors, index providers, mutation
maintainers, model adapters, policy handlers and execution identity. SQL DDL
requests new schema-driven adapters through injected factories. It never grants
authority merely because an entity was declared by SQL.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Schema](../Schema/DESIGN.md) | used by | Immutable candidate runtime | Prepares executable generations | Static protection is owned by SQLCatalog |
| [DatabaseRuntime](../../DatabaseRuntime/DESIGN.md) | used by | Trait-selected factories | Supplies installed capabilities | No global registration |

## Architecture

```text
application composition -> immutable RuntimeConfiguration
  -> existing registrations and authorization handlers
  -> injected schema-driven entity factory
  -> optional injected authorization-policy factory
  -> candidate runtime -> candidate generation lease
```

## Contracts and Invariants

A clone preserves installed capability registries and unchanged entity adapters.
New or changed dynamic entities receive a schema-driven adapter. Initially composed schema-driven entities and newly added candidate entities may
receive a policy only from the explicit application policy factory; a missing
policy remains fail-closed under enabled security. Existing handlers retain their
meaning. A clone retains declaration-scoped configuration for every concrete and
polymorphic index in `Schema.allIndexNames`. Removed index declarations remove
only their own configuration values; installed provider capabilities remain unchanged. Factories and configuration contain no mutable global registration.
Application restoration supplies the same factories through its runtime factory.

## Verification and Change Impact

Candidate staging tests exercise new contracts, journal restoration and runtime
selection. Enabled-security CREATE/INSERT and denied-policy integration tests
must establish policy-factory behavior. Factory changes require rechecking Schema
candidate admission and native/portable composition call sites.
