# IVF Read Component

## Purpose and Scope

The IVF read component interprets the persisted IVF metadata, centroid, list,
and assignment layout and emits retained vector matches. It owns IVF search
selection and validation only.

- Parent: [VectorIndex](../DESIGN.md)
- Children: none
- Depends on: DatabaseEngine read/session and retention contracts.

## Responsibilities and Boundaries

IVF validates metadata and centroid shape, selects the configured probe lists,
reads list entries, computes canonical distances, and appends matches to the
caller-provided retained match owner. It does not authorize fields, resolve a
runtime registry, create a transaction, own final model rows, or expose raw
arrays/heaps outside the component.

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [VectorIndex](../DESIGN.md) | parent | One session meter, bounded reads, retained match ownership | Algorithm changes must preserve the parent ordering and failure contracts. |
| [DatabaseEngine](../../DatabaseEngine/DESIGN.md) | dependency through parent | Session-bound transaction and retained destination | IVF must not bypass the session through a raw context or new transaction. |

## Architecture

```text
IVF reader
  -> bounded metadata/centroid/list reads
  -> retained centroid views
  -> canonical distance + probe ordering
  -> VectorRetainedMatches.append(primaryKey, distance)
```

## Contracts and Invariants

- Metadata dimensions, list count, training state, and persisted counts agree
  with the descriptor and parameters before search results are emitted.
- Every list primary key and vector payload is validated before match admission.
- A match is admitted before its retained tuple/primary-key representation is
  created; failed admission does not leave a partial claim.
- Trained and exact-search fallback paths produce the same canonical ordering
  and tie behavior as the existing IVF implementation.
- Missing, malformed, count-mismatch, cancellation, and budget errors are
  explicit failures with complete owner release.

## Runtime Flows

```text
metadata -> centroids -> probe selection -> lists -> distance -> retained match
```

The untrained layout uses the existing exact-search semantics after validating
that no persisted centroids contradict the metadata.

## State, Ownership, and Lifecycle

Centroid payloads are retained only for the search operation and are borrowed
as `PersistedVectorView` during distance computation. The match owner outlives
all algorithm-local borrows and is consumed by the VectorIndex executor.

## Failure, Concurrency, and Constraints

IVF uses the supplied session transaction serially. Probe and list collections
are admitted before growth; no optional work meter or unbounded raw result
collection is allowed.

## Verification and Change Impact

IVF tests must cover trained and untrained paths, probe ordering, malformed
metadata/centroids/list keys, missing vectors, bounded point reads, distance
and tie ordering, budget/cancellation, later failure, foreign-meter rejection,
and final owner release.
