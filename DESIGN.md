# database-framework

## Purpose and Scope

System and Swift package design authority for `database-framework`. The
current index covers the `DatabaseEngine` contract changed by DF-06F0; unchanged
products add their module authority when their contract changes.

- Parent: none.
- Child: [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md).

## Responsibilities and Boundaries

The package owns in-process database execution, authorization enforcement,
transactions, query/resource enforcement, and specialized index contracts. It
consumes schema/query semantics from `DatabaseKit`, primitive values from
`DatabaseTypes`, and storage semantics from `StorageKit`. It does not own
backend-native storage, wire dispatch, transport, or server process lifecycle.

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [DatabaseEngine](Sources/DatabaseEngine/DESIGN.md) | child | In-process execution | Authorization, ownership, or budget changes require caller review. |

External dependencies have no local `DESIGN.md` in this graph. Their pinned
public APIs remain authoritative and are not restated here.

## Architecture

```text
DatabaseKit + DatabaseTypes + StorageKit
                  -> DatabaseEngine
                      <- specialized index products
```

## Contracts and Invariants

- `DatabaseEngine` does not depend on a specialized index implementation.
- One read uses schema/principal-bound authority and a session transaction.
- Request intermediates retain the exact meter claim until explicit output
  promotion or release.
- Typed authorization, storage, budget, cancellation, and cleanup failures are
  not converted into success.

## State, Ownership, and Lifecycle

`DBContainer` owns runtime/schema state and its injected engine. A read snapshot
owns the schema lease, policy, transaction, operation scope, and work meter.

## Failure, Concurrency, and Constraints

Storage uses the session transaction's serialization contract. Feature traits
cannot weaken authorization, ownership, resource, or synchronization rules.

## Verification and Change Impact

[DatabaseEngine tests](Tests/DatabaseEngineTests) own the changed behavior.
A lower guarantee change invalidates only dependent evidence; an engine
contract change requires affected specialized-index conformance review.
