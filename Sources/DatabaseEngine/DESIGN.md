# DatabaseEngine

## Purpose and Scope

Module authority for platform-neutral in-process execution. DF-06F0 and
DF-06R compose retained polymorphic and regular-model paths from [Read](Read/DESIGN.md),
[Core](Core/DESIGN.md), [Serialization](Serialization/DESIGN.md), and
[QueryExecution](QueryExecution/DESIGN.md), over keyspaces bound by
[Directory](Directory/DESIGN.md).

- Parent: [database-framework](../../DESIGN.md).
- Children: the five linked component designs above.

## Responsibilities and Boundaries

The module owns schema-bound sessions, persistence decoding, query execution,
and request accounting. It exposes purpose-neutral contracts to index modules;
scoring, ranking, bitmap, full-text, and vector policy remain outside it. It
owns no key layout: every keyspace it reads or writes is resolved from a
StorageKit Directory node through [Directory](Directory/DESIGN.md).

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [Read](Read/DESIGN.md) | child | Session admission | Context methods cannot bypass it. |
| [Core](Core/DESIGN.md) | child | Polymorphic orchestration | Does not decide query authority. |
| [Serialization](Serialization/DESIGN.md) | child | Retained bounded item read | Envelope and chunks are bounded. |
| [QueryExecution](QueryExecution/DESIGN.md) | child | Retained aggregate/destinations | Copyable raw values do not escape. |
| [Directory](Directory/DESIGN.md) | child | Declaration-to-node binding | A resolved node is valid only in its resolving transaction. |

## Architecture

```text
sealed authority + snapshot + exact request meter
                      -> Read
                          -> Core -> Serialization
                              -> QueryExecution retained owner
                                  -> canonical/index/public destination

Directory binds every declaration to a StorageKit node, so Core and
QueryExecution receive a resolved Subspace rather than a computed prefix.
```

## Contracts and Invariants

### Assumptions

- Storage point reads reject the supplied bound before exposing bytes.
- Runtime registrations canonicalize for the session schema generation.

### Guarantees

- Scan obtains its meter only from the session-owned scope; ordered point
  fetch rejects foreign retained-key meters before storage.
- A nested Read-owned non-forgeable admission is created only inside the
  session closure after sealed list/field coverage validation and is required
  by the Core context API.
- Capacity and every present/missing slot are admitted before allocation or
  decode, and all retained values use the session meter.
- Regular model fetch accepts retained primary keys through an async scoped
  borrow. The source owner and claim remain live through each point read, and
  no canonical path requires a raw `Collection<Tuple>` view.
- Complete regular-model output requires whole-entity field authority. Fusion
  candidate materialization remains projection-bound through a distinct
  session entry point that derives fields from its sealed query authorization.
- The aggregate exposes purpose-specific destinations and consuming public
  promotion, not general copyable model/identifier/array access.
- Point-fetch slots preserve input order and absence; scans preserve cursor
  order. Failure/cancellation release partial owners and complete cleanup.

## Runtime Flows

```text
Read closure/admission -> QueryExecution retained primary-key borrow
  -> bounded retained item read
  -> runtime-bound decode/row authorization -> pre-admitted retained slot
  -> admitted destination or consuming public output -> owner release
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Authority/transaction/meter | Read session | Session operation |
| Stored bytes | Serialization byte owner | Decode/returned payload |
| Identifier/model/slot | QueryExecution aggregate | Destination/promotion |
| Canonical/index row | Destination owner | Query pipeline |
| Resolved Directory/Subspace | Directory, per transaction | Resolving transaction |

## Failure, Concurrency, and Constraints

Authorization, meter mismatch, malformed/unknown type, bounded-read, decode,
budget, cancellation, and cleanup remain distinct failures. One transaction is
used serially. Intermediate polymorphic reads expose only the retained owner;
raw entity arrays exist only at the consuming public-output boundary.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| Scan meter provenance | Signature/source audit proves no external meter input and session-meter derivation. |
| Point authority/meter | Regular and polymorphic denial and foreign-key-meter tests observe zero point reads. |
| Admission, order, absence | Budget-before-read and present/missing sequence tests. |
| Failure/cancellation/release | Later-failure and suspended-read tests end at zero retained resources. |
| No raw escape | Source audit rejects general borrows, raw returns, and unmarked bridges. |
| Bounded item path | Envelope and every chunk are observed as bounded reads. |
| Directory binding | [Directory](Directory/DESIGN.md) owns the canonical component, tag derivation, and layout-rejection evidence. |

Owners: [polymorphic retained tests](../../Tests/DatabaseEngineTests/PolymorphicRetainedResourceContractTests.swift),
[item storage tests](../../Tests/DatabaseEngineTests/ItemStorageResourceTests.swift),
and [authorization tests](../../Tests/DatabaseEngineTests/ReadAuthorizationCapabilityTests.swift).
