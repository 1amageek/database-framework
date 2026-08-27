# BitmapIndex

## Purpose and Scope

BitmapIndex owns the read-side execution of bitmap indexes: decoding bounded
bitmap payloads, applying set operations, resolving sequential identifiers to
primary keys, and handing retained values to DatabaseEngine read execution.
The system design is the [workspace design](../../DESIGN.md). This module has
no child design units.

This change covers the read path and its retained-resource proof. It does not
change bitmap write layout, query meaning, Fusion planning, or general
DatabaseEngine behavior.

## Responsibilities and Boundaries

BitmapIndex owns bitmap representation and the mapping between bitmap
sequential identifiers and primary-key tuples. It owns the bounded read
admission needed while those values are decoded and retained.

DatabaseEngine owns the session, authorization, transaction, work-meter
identity, retained polymorphic aggregate, and final result builders. StorageKit
owns the backend point-read contract. BitmapIndex consumes those contracts and
does not authorize independently or open another transaction.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [Workspace](../../DESIGN.md) | parent | Package ownership and dependency direction | BitmapIndex is an execution feature above DatabaseEngine and StorageKit. | Do not move storage or query semantics into this module. |
| [DatabaseEngine](../DatabaseEngine/DESIGN.md) | depends on | Session-bound read transaction and result admission | Supplies authorization, transaction, meter, and retained output contracts. | The session-owned transaction is the only read authority. |
| [Read](../DatabaseEngine/Read/DESIGN.md) | coordinates with | Sealed authorization and meter identity | Admission precedes index reads and all retained values use the session meter. | A tokenless/direct bridge cannot replace session authorization. |
| [QueryExecution](../DatabaseEngine/QueryExecution/DESIGN.md) | used by | Retained polymorphic aggregate and `appendIndexRow` | Converts retained index entries into canonical index rows. | Never return a raw polymorphic entity array from an intermediate path. |
| [Workspace StorageKit ownership](../../DESIGN.md#storage-kit) | depends on | Bounded point read | The StorageKit package owns the backend point-read contract consumed here. | Bitmap decoding cannot use an unbounded `getValue` path. |

## Architecture

```text
sealed session authorization
          |
          v
session-owned read transaction + work meter
          |
          v
bounded bitmap payload -> retained bitmap owner (~Copyable)
          |
          v
equals / union / intersection
          |
          v
bounded id payloads -> retained primary-key collection
          |
          +---------------------> concrete retained model fetch
          |
          +---------------------> retained polymorphic fetch
                                      |
                                      v
                              appendIndexRow -> IndexReadResult
```

## Contracts and Invariants

- Authorization is evaluated before any bitmap or identifier storage read; the
  existing session authorization order is unchanged.
- Every point read uses the session-owned work meter and a backend-enforced
  maximum returned byte count. The meter budget is the bitmap payload upper
  bound and is a correctness limit, not an optimization hint.
- A decoded bitmap is held by a noncopyable retained owner. Its reservation
  remains live until the owner is consumed or destroyed; a raw bitmap does not
  cross an intermediate operator boundary.
- Bitmap iteration scratch is admitted for the duration of the borrow and is
  released before the iterator can outlive the bitmap owner.
- Sequential identifier resolution returns a
  `DatabaseRetainedPrimaryKeyCollection`. Each borrowed tuple is valid only
  inside its borrow closure while its reservation and originating meter remain
  alive.
- Set operations preserve the existing equals, IN/union, AND/intersection,
  unordered result, limit, and missing-value semantics.
- Polymorphic reads pass the retained primary-key collection directly to
  `fetchRetainedPolymorphicItemsPreservingOrder` and append through
  `appendIndexRow`. An intermediate raw entity array is not produced.
- A foreign work meter is rejected before composing retained owners. Every
  failure, cancellation, oversized or malformed payload, missing value, and
  later fetch error releases all reservations.
- Final public arrays or result rows are created only at an explicit output
  boundary owned by DatabaseEngine or the public maintainer API.

## Runtime Flows

1. The DatabaseEngine session authenticates the requested entity/index and
   resolves the readable subspace.
2. BitmapIndex reads each payload through the bounded point-read contract and
   decodes it while admitting retained bitmap storage.
3. Set operators consume the previous retained bitmap and the next one, admit
   the output upper bound, and release consumed owners after the output is
   constructed.
4. Identifier payloads are read with the same meter and appended to a retained
   primary-key collection. Missing mappings remain omitted as before.
5. Concrete reads use the existing retained model path. Polymorphic reads pass
   the retained collection to the F0 retained fetch path and append rows without
   materializing a raw entity array.
6. Any failure or cancellation unwinds owners before the session operation
   returns.

## State, Ownership, and Lifecycle

| Value | Owner | Lifetime |
|---|---|---|
| Serialized bitmap payload | bounded point-read result | Until decode completes |
| Decoded bitmap | noncopyable BitmapIndex owner | Until set operation or output boundary |
| Sequential ID mapping | retained primary-key collection | Until fetch/result consumes it |
| Session meter and transaction | DatabaseEngine read session | Entire session operation |
| Polymorphic fetched entries | DatabaseEngine retained aggregate | Until `appendIndexRow` or final output |

BitmapIndex never stores a session transaction or authorization globally. All
mutable operator state is task-local; backend I/O remains outside any mutex
critical section.

## Failure, Concurrency, and Constraints

Malformed or oversized payloads, missing data, budget exhaustion, foreign-meter
composition, backend errors, and cancellation remain explicit failures or
empty results according to the existing API contract; they are never silently
converted into synthetic rows. Point reads are sequential on the supplied
session transaction, and retained owners are `Sendable` only through their
immutable owner contracts.

## Verification and Change Impact

Focused BitmapIndex tests must exercise bounded maximum propagation on a
controlled backend, set-operation semantics, unordered and limit behavior,
missing mappings, malformed and oversized payloads, later read failure,
cancellation, foreign-meter rejection, and polymorphic authorization denial
with zero storage reads. The result summary must report the reviewed exact
count with zero failures, skips, expected failures, runtime warnings, and
internal tool errors.

Changes to the retained read contract require rechecking DatabaseEngine Read,
QueryExecution, and the package-level integration matrix. Changes to write
layout or query semantics are outside this design and require a separate
design decision.
