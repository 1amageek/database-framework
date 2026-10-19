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

### Logical SQL authorization

`sqlAuthorizationPolicyFactory: (@Sendable (Schema.Entity, SQLNamespace,
String) -> AuthorizationPolicyHandler?)?` is an optional immutable input.
After catalog validation, runtime rebinding removes every handler for the current
and previous `sqlOwnedEntityNames`, then invokes this factory with each current
physical entity and its exact logical namespace/public relation name. A materialized
view uses its visible view name through the validated backing-relation identity;
ordinary views retain underlying-table invoker authorization. A missing handler
leaves access denied; an initial physical-name handler never bypasses the logical
selection. Returned handler entity identity must match exact UTF-8 bytes or fail
with `authorizationPolicyEntityMismatch`. Validated SQL ownership excludes
compiled protected entities, so their handlers remain unchanged. An absent SQL
factory preserves the existing generic/explicit policy behavior.

Rebinding occurs for every finalized catalog, including unchanged physical
entities after logical table/schema rename and altered contracts. It produces a
new runtime and security delegate before staging or publication; previous leases
retain their original generation. The Schema owner supplies validated ownership,
namespace and relation declarations; this component does not infer SQL ownership.

| State | Native / WASM / Embedded storage and lifetime |
|---|---|
| Factories and policy handlers | Same immutable `Sendable` configuration, retained by generation lease |
| Previous SQL-owned identities | Same immutable exact-byte identity set, replaced during runtime cloning |
| Published generation | Existing Schema generation-store `Mutex<State>`; no new lock or mutation path |

`S17SQLAuthorizationTests` owns logical allow/missing deny, explicit physical
handler rejection, exact names, handler mismatch, rename/rollback/savepoint,
compiled coexistence and reopen regressions. Existing generic-policy suites
remain regression evidence when this factory is absent.
