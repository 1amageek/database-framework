# VectorIndex

## Purpose and Scope

VectorIndex is the module authority for Flat, IVF, PQ, and HNSW vector indexes.
It owns vector index maintenance, training, persisted layout interpretation,
runtime algorithm selection, direct and canonical query entry points, distance
calculation, and read-result ownership. It consumes the DatabaseEngine session
contract, the DatabaseKit vector/index model, and StorageKit transactions. It
does not own general query-language semantics, database authorization,
transaction creation, or final model-row ownership.

- Parent: [database-framework](../../DESIGN.md)
- Children: [IVF](IVF/DESIGN.md), [PQ](PQ/DESIGN.md)
- HNSW and Flat implementations remain in the module composition root.

## Responsibilities and Boundaries

VectorIndex owns vector-specific persisted state, maintenance, training,
algorithm policy, canonical distance calculation, search ordering, cache
policy, and retained match ownership. DatabaseEngine owns session creation,
authorization, transaction identity, work-meter identity, model
materialization, and final row output. A canonical vector executor consumes a
session-owned read capability; it does not create a transaction, decide
authority, or return an unmetered result array. Public maintainer APIs retain
their existing materialized output boundary and are not the canonical session
path.

The read boundary is:

```text
DatabaseReadSession
    -> one admitted VectorRetainedMatches owner
        -> scoped match borrow
            -> DatabaseEngine retained polymorphic entity owner
                -> IndexReadResultBuilder.appendIndexRow(distance)
                    -> final query output
```

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [database-framework](../../DESIGN.md) | parent | Package dependency direction and global invariants | Module contract changes require the package index to remain current. |
| [DatabaseEngine](../DatabaseEngine/DESIGN.md) | depends on | Session-bound read, authorization, work meter, retained entity and row contracts | A session or retained-owner contract change requires vector caller and focused proof review. |
| [IVF](IVF/DESIGN.md) | child | Centroid/list search preserves canonical distance and ordering | IVF metadata and centroid ownership stay inside the IVF reader. |
| [PQ](PQ/DESIGN.md) | child | Codebook/code search preserves canonical distance and ordering | PQ codebook and vector payload ownership stay inside the PQ reader. |

## Architecture

```text
VectorReadExecutor
    |-- validates query parameters and descriptor shape
    |-- admits session authorization before runtime resolution
    |-- resolves one algorithm from the session-bound configuration
    |-- invokes algorithm reader with session storage transaction + meter
    |-- receives VectorRetainedMatches (PK + distance)
    |-- fetches retained polymorphic models through the session
    `-- appends rows through the retained destination owner

Flat / IVF / PQ / HNSW readers
    -> bounded point reads and range cursors
    -> retained vector views for synchronous distance calculations
    -> VectorRetainedMatches

VectorIndexMaintainerProvider
    -> algorithm maintainer
        -> persisted vector/index layout
        -> optional training state
```

## Contracts and Invariants

1. Authorization is admitted before runtime configuration, descriptor lookup,
   algorithm lookup, or storage read that can disclose index state.
2. One execution uses the session transaction and the session work meter. No
   caller-supplied optional meter or unrelated transaction is accepted.
3. Every persisted point read uses bounded `readPointValue`; raw `getValue` is
   not a vector search path.
4. Match identifiers and distances are retained together in one owner whose
   meter identity cannot be replaced or mixed. The owner is consumed only by a
   destination or final-output boundary.
5. Distance ordering and tie ordering remain equivalent to the pre-migration
   algorithm: lower canonical distance first, then canonical primary-key byte
   order where the existing algorithm defines a tie.
6. Missing, malformed, corrupt, cancelled, over-budget, foreign-meter, and
   later-failure paths throw typed failures and release every retained claim.
7. HNSW normalization is admitted before its allocation and remains owned
   until search completion; a normalization copy is never silently unmetered.
8. `PersistedVectorView` borrows its retained payload only within a synchronous
   calculation and never materializes an element array on the search path.
9. Raw algorithm arrays, heaps, tuples, and polymorphic entity arrays do not
   cross the VectorIndex-to-DatabaseEngine boundary.

## Runtime Flows

```text
request
  -> validate query shape
  -> authorize index/entity/fields
  -> resolve descriptor and one runtime algorithm
  -> create VectorRetainedMatches with session meter
  -> scan/read bounded persisted data
  -> append admitted PK+distance matches
  -> fetch retained models in match order
  -> borrow each match and entity into an admitted output row
  -> consume output at the public boundary
```

The algorithm readers are serial with respect to one StorageKit transaction.
They may borrow retained bytes synchronously while computing a distance, but
they do not retain backend-owned buffers across suspension.

## State, Ownership, and Lifecycle

`VectorRetainedMatches` owns the match storage and its exact reservation. Each
slot contains one retained primary-key payload and one canonical distance. The
reader creates the owner after the session admission and releases it on every
failure or cancellation. The executor consumes it while appending to an
`IndexReadResultBuilder`; only the final result boundary can promote rows to a
public array.

HNSW graph snapshots remain cache-owned immutable state. A request-owned HNSW
normalization is distinct from cache state and is released when the search
operation ends.

## Failure, Concurrency, and Constraints

The session meter is the authority for point-read, scan, sort, tuple, model,
and output reservations. A foreign meter is a typed failure. HNSW cache state
uses its existing synchronization contract; vector readers do not add shared
mutable registration or hold a lock across `await`. Cursor and transaction
cleanup is delegated to the session/storage contracts and remains active on
all error and cancellation paths.

## Verification and Change Impact

The VectorIndex test target owns algorithm behavior, retained-match ownership,
and its concrete regular/polymorphic executor paths. DatabaseEngine retained
fetch suites own model/entity later-failure and cancellation cleanup. Together
the focused evidence covers denial-before-read, algorithm ordering and
distance, bounded reads, malformed/corrupt/missing data, foreign meters,
candidate/output admission, HNSW normalization, final release, and the two
retained-fetch compositions without duplicating DatabaseEngine proofs.

Changes to a child algorithm design require its child focused tests and the
shared VectorRetainedMatches contract tests. Changes to the session or retained
polymorphic contract invalidate the vector focused result until the caller
path is recompiled and behaviorally rechecked.
