# Core

## Purpose and Scope

The Core component owns polymorphic runtime orchestration: group-directory
access, type-code resolution, retained identifier construction, runtime-bound
model decoding, per-entity authorization, and ordered assembly of a retained
batch.

- Parent: [DatabaseEngine](../DESIGN.md).
- Children: none in the DF-06F0 scope.

## Responsibilities and Boundaries

Core consumes a Read-issued operation admission and the session-owned
transaction. It resolves only runtimes belonging to the requested group and
produces a purpose-neutral retained aggregate.

Core does not make query-level authorization decisions, own the aggregate
representation, implement stored-value framing, or materialize specialized
index output. Public output conversion occurs at an explicit `DatabaseContext`
boundary after a retained operation succeeds.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | Cross-component flow and failure contract | Defines the composed scan/fetch guarantee. | Core is not a substitute session boundary. |
| [Read](../Read/DESIGN.md) | depends on | Transaction, operation admission, schema/runtime view | Supplies authority and exact request meter. | A bare session plus caller assertion is insufficient admission. |
| [Serialization](../Serialization/DESIGN.md) | depends on | Retained bounded item read | Loads item envelopes and external chunks. | Raw unbounded reads are excluded from the retained path. |
| [QueryExecution](../QueryExecution/DESIGN.md) | depends on | Builder and retained aggregate | Commits pre-admitted present/missing slots. | General model or identifier escape is forbidden. |

## Architecture

```text
Read operation admission
       |
       v
polymorphic group directory + session runtime map
       |
       +-> scan cursor -----------+
       |                          |
       +-> ordered retained keys -+-> retained ItemStorage read
                                      -> runtime-bound decode
                                      -> per-entity authorization
                                      -> pre-admitted aggregate slot
```

## Contracts and Invariants

- Core accepts only a Read-issued admission matching the group, session, and
  meter used for the operation.
- Runtime lookup comes from the session's schema generation. An unknown or
  nonmember type code is a typed failure.
- Identifier bytes, decoded tuple, stored bytes, decoded model, and collection
  slot use the same work meter and remain owned until moved into the aggregate
  or destroyed on failure.
- A scan opens its cursor only after aggregate setup and session admission.
- Ordered fetch produces exactly one slot per retained input key. Directory or
  value absence commits `nil`; malformed identifiers and unknown types fail.
- Core calls dynamic per-entity authorization after decode and before commit.
- It never returns a raw array of `PolymorphicEntity` or `PersistedModel` as an
  intermediate contract.

## Runtime Flows

### Scan

```text
admission -> create aggregate builder -> open group directory/cursor
    -> pre-admit slot -> retain identifier -> resolve runtime
    -> retained decode -> authorize entity -> commit present slot
```

### Ordered point fetch

```text
admission -> pre-admit aggregate capacity -> for each retained key
    -> pre-admit slot -> retain identifier and storage key -> resolve runtime
    -> bounded retained read -> commit missing OR decode/authorize/commit model
```

## State, Ownership, and Lifecycle

Core holds no global mutable state. Directory subspaces and runtime maps are
immutable operation values. Each loop-local owner either moves into the
aggregate entry or is destroyed before the next iteration/error escape.

## Failure, Concurrency, and Constraints

The point-fetch loop is serial on the session transaction. Budget failure,
malformed identifiers, unknown type codes, missing chunks, checksum failure,
authorization failure, cancellation, and session revocation propagate without
synthetic success. Scan cleanup is awaited by the Serialization cursor scope;
point cleanup is owned by the Read operation scope and local retained owners.

## Verification and Change Impact

- [PolymorphicRetainedResourceContractTests](../../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift)
  owns ordering, missing-slot, later-failure, cancellation, meter, and
  admission-before-storage evidence.
- Public full-scan behavior remains covered by existing polymorphic fetch tests
  after consuming output promotion.
- A changed group key, type-code, runtime canonicalization, or transaction
  contract invalidates this component and QueryExecution destination evidence.
