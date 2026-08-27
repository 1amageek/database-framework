# VersionIndex

## Purpose and Scope

Module design authority for version-history maintenance and reads. DF-06K
covers the read-only public history boundary, retained canonical execution,
regular/polymorphic executors, and their resource lifecycle.

- Parent: [database-framework](../../DESIGN.md).
- Children: none.
- Dependency: [DatabaseEngine](../DatabaseEngine/DESIGN.md).

## Responsibilities and Boundaries

VersionIndex owns history key/value interpretation, newest-first history order,
version annotations, and retained history lifetime. It consumes session,
authorization, transaction, work-meter, retained-model, and destination-row
contracts from DatabaseEngine.

It does not decide entity/field authorization, own schema runtime lookup,
decode generic storage envelopes, or define polymorphic type identity. The
public `history` array is an explicit caller-requested output; canonical
execution uses retained history and never uses that array as an intermediate.
The unreferenced `VersionQuery.executeDirect` helper is outside the DF-06K
canonical path and remains unchanged; new callers must use canonical execution
through the session-bound executor instead of treating that helper or its array
result as an intermediate.

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [Package](../../DESIGN.md) | parent | Package dependency direction | VersionIndex depends on DatabaseEngine, never the reverse. |
| [DatabaseEngine](../DatabaseEngine/DESIGN.md) | depends on | Sealed read session, retained owners, destination admission | Authorization precedes runtime/index/storage access. |

## Architecture

```text
DatabaseReadSession + prepared field/list authority
  -> regular OR polymorphic Version executor
      -> authorized runtime/index resolution
          -> VersionIndexReader.retainedHistory
              -> VersionRetainedHistory
                  -> retained model decode
                      -> admitted IndexReadResult rows + version/type metadata
```

## Contracts and Invariants

- Executor parameter parsing may reject caller-supplied malformed input before
  authorization, but runtime-map lookup, readable-index resolution, and
  storage access occur only after sealed authorization coverage succeeds.
- Preflight `additionalRequiredFieldNames` and executor authorization
  validation describe the same field plan; execution neither widens it to all
  fields nor narrows it after the decision.
- Regular and polymorphic executors use the session transaction, consistency,
  and exact work meter without opening another transaction.
- Retained history is newest-first for bounded and unbounded reads and contains
  only validated versionstamp/payload pairs.
- Array capacity, every retained version/payload owner, decoded model, and
  destination row footprint are admitted before their allocation or commit.
- Retained version/payload copies keep their reservation owner; no raw history
  array or decoded model escapes the canonical execution path.
- Regular output adds `version`. Polymorphic output additionally adds the
  runtime-derived type name and type code matching the requested identifier.

## Runtime Flows

```text
parse request
  -> validate sealed authorization
  -> resolve runtime/readable index
  -> open newest-first cursor
  -> validate key/value
  -> admit and retain version/payload
  -> decode retained model
  -> compute/admit complete row footprint
  -> materialize one result row
```

An empty readable-index result returns an admitted empty retained owner. A
malformed later entry, budget failure, cancellation, or cursor failure destroys
all previously retained entries before the error escapes.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Cursor | `retainedHistory` operation | Until authoritative `finish` |
| Version/payload bytes | Retained byte owners in history entry | Entry or retained copy lifetime |
| History array/capacity | `VersionRetainedHistory` | Executor materialization |
| Decoded model | `DatabaseRetainedStoredModel` | Destination append closure |
| Result rows | `IndexReadResult` retained owner | Canonical query pipeline |

## Failure, Concurrency, and Constraints

History cursor access is serial on the session transaction. Invalid limits,
malformed keys/values, authorization mismatch, unknown polymorphic type,
resource exhaustion, cancellation, and cleanup remain distinct failures.
Iteration failure preserves a secondary cursor cleanup failure through
`StorageRangeCleanupError`.

## Verification and Change Impact

| Contract | Required evidence |
|---|---|
| Ordering and retained lifetime | Reader success/release tests for bounded and unbounded history. |
| Partial cleanup | A later malformed/budget failure and cancellation after at least one retained entry end at zero rows/bytes. |
| Authorization order | Denied polymorphic execution observes no runtime-map/index/storage disclosure. |
| Canonical result | Regular and polymorphic executor tests verify row count/order, decoded fields, version/type annotations, meter release, and missing-index behavior. |
| Field-plan consistency | Source audit and denied-field test prove preflight and executor validate the same plan. |

The focused owner is
[VersionReadResourceContractTests](../../Tests/VersionIndexTests/VersionReadResourceContractTests.swift);
reader-only evidence is insufficient for executor authorization and final-row
contracts. A DatabaseEngine session/retained-owner change requires this module
to revalidate the affected table rows before integration.
