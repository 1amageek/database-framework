# Read

## Purpose and Scope

Owns schema/principal-bound sessions and operation admission, including the
DF-06F0 retained polymorphic entry gate and DF-06R regular retained-fetch
admission. Parent:
[DatabaseEngine](../DESIGN.md). Children: none in this scope.

## Responsibilities and Boundaries

Read owns authorization, schema lease, session scope, transaction, snapshot,
revocation, and meter identity. It creates the noncopyable admission consumed
by regular retained fetch. It does not resolve stored types, decode values, or
build rows. TaskLocal state and a caller's earlier temporal validation are not
authority.

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [StorageKit cursor contract](https://github.com/1amageek/storage-kit/blob/53e615faee772d2ee7c1a59311beaf8e8f0cf7c1/Sources/StorageKit/Storage/DESIGN.md) | depends on | State-owned `KeyValueCursor.validatingScope` | The pinned URL revision must provide post-validation before cursor readiness and finish-boundary resolution. |
| [Core](../Core/DESIGN.md) | used by | Session transaction and admission | Core cannot widen authority. |
| [QueryExecution](../QueryExecution/DESIGN.md) | coordinates with | Session meter | Identity, not equal budgets. |

## Architecture

```text
ReadPolicy -> sealed authorization -> ReadSession closure
  -> validate required list/fields -> nested admission -> Core/QueryExecution
```

## Contracts and Invariants

- The decision is made once and sealed to schema lease and principal.
- Admission is nested, non-forgeable, created only inside the session operation
  after exact coverage validation, and required by Core.
- Scan derives its meter from session scope and has no external meter input.
- Point fetch rejects retained keys from another meter before storage.
- Regular retained fetch derives its work meter from the session scope and
  requires complete entity-field authority, then seals entity, admitted
  fields, transaction, snapshot, and schema generation into one non-forgeable
  admission.
- Fusion candidate materialization uses a distinct session entry point. It
  derives the immutable field set from Fusion's explicit sealed authorization
  rather than accepting a caller-supplied field map, so projected reads do not
  widen authority or weaken the complete-model entry point.
- Per-entity authorization after decode supplements, not replaces,
  pre-storage list/field admission.
- A scoped range cursor uses the StorageKit state-owned validation contract;
  Read supplies the scope lease and does not wrap a second cursor state.

## Runtime Flows

```text
requirement -> validate coverage -> begin scoped closure/create admission
  -> async retained-key borrow -> QueryExecution call
  -> validate operation -> return retained result
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
prove sealed denial; [retained regular fetch tests](../../../Tests/DatabaseEngineTests/RetainedRegularModelFetchContractTests.swift)
prove async source lifetime, order, missing slots, and foreign-meter rejection;
[retained polymorphic tests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
prove polymorphic point rejection. Scan meter provenance is a signature and
source audit. Admission changes invalidate QueryExecution, Fusion, and index-caller evidence.
