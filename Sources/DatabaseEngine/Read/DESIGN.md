# Read

## Purpose and Scope

Owns schema/principal-bound sessions and operation admission, including the
DF-06F0 retained polymorphic entry gate. Parent:
[DatabaseEngine](../DESIGN.md). Children: none in this scope.

## Responsibilities and Boundaries

Read owns authorization, schema lease, session scope, transaction, revocation,
and meter identity. It does not resolve stored types, decode values, or build
rows. TaskLocal state and a caller's earlier temporal validation are not
authority.

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [Core](../Core/DESIGN.md) | used by | Session transaction and admission | Core cannot widen authority. |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | Session meter | Identity, not equal budgets. |

## Architecture

```text
ReadPolicy -> sealed authorization -> ReadSession closure
  -> validate required list/fields -> nested admission -> Core
```

## Contracts and Invariants

- The decision is made once and sealed to schema lease and principal.
- Admission is nested, non-forgeable, created only inside the session operation
  after exact coverage validation, and required by Core.
- Scan derives its meter from session scope and has no external meter input.
- Point fetch rejects retained keys from another meter before storage.
- Per-entity authorization after decode supplements, not replaces,
  pre-storage list/field admission.

## Runtime Flows

```text
requirement -> validate coverage -> begin scoped closure/create admission
  -> Core call -> validate operation -> return retained result
```

## State, Ownership, and Lifecycle

The immutable session owns policy, lease, transaction, clocks, root, scope,
and meter. Admission cannot outlive or replace its closure/scope.

## Failure, Concurrency, and Constraints

Authorization and foreign-meter failures precede storage. Lower
failure/cancellation ends the operation before escape. Reads remain serial on
one transaction.

## Verification and Change Impact

[Authorization tests](../../../Tests/DatabaseEngineTests/ReadAuthorizationCapabilityTests.swift)
prove sealed denial; [retained polymorphic tests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
prove point foreign-meter rejection. Scan meter provenance is a signature and
source audit. Admission changes invalidate Core and index-caller evidence.
