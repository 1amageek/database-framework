# PQ Read Component

## Purpose and Scope

The PQ read component interprets the persisted product-quantization metadata,
codebook, code, and canonical-vector layout and emits retained vector matches.
It owns PQ validation and search arithmetic only.

- Parent: [VectorIndex](../DESIGN.md)
- Children: none
- Depends on: DatabaseEngine read/session and retention contracts.

## Responsibilities and Boundaries

PQ validates codebook and code shapes, builds the distance table, scans encoded
entries, validates each canonical vector, computes canonical distances, and
appends retained matches. It does not authorize fields, resolve runtime
configuration, create transactions, own final rows, or expose raw arrays,
codebooks, or heaps beyond the component.

## Related Designs

| Design | Relationship | Contract Used | Cautions |
|---|---|---|---|
| [VectorIndex](../DESIGN.md) | parent | One session meter, bounded reads, retained match ownership | PQ arithmetic changes must preserve canonical metric and ordering. |
| [DatabaseEngine](../../DatabaseEngine/DESIGN.md) | dependency through parent | Session-bound transaction and retained destination | PQ must not bypass the session through a raw context or new transaction. |

## Architecture

```text
PQ reader
  -> bounded metadata/codebook reads
  -> retained codebook views + admitted distance table
  -> bounded code/vector reads
  -> canonical distance
  -> VectorRetainedMatches.append(primaryKey, distance)
```

## Contracts and Invariants

- Dimensions, subquantizer count, codebook count, and metadata agree before
  encoded entries are consumed.
- Distance-table and per-entry working storage are admitted before allocation
  or use and remain charged to the same session meter.
- Every code and canonical vector is validated before match admission.
- Trained and exact-search fallback paths preserve existing canonical distance,
  ordering, tie, and vector-count semantics.
- Missing, malformed, count-mismatch, cancellation, and budget errors are
  explicit failures with complete owner release.

## Runtime Flows

```text
metadata -> codebooks -> distance table -> codes/vectors -> distance -> match
```

The untrained layout uses exact canonical-vector search after validating that
no codebook state contradicts the metadata.

## State, Ownership, and Lifecycle

Codebook and vector payloads remain retained for the operation and are borrowed
as `PersistedVectorView` only during synchronous calculations. The retained
match owner outlives algorithm-local borrows and is consumed by the VectorIndex
executor.

## Failure, Concurrency, and Constraints

PQ uses the supplied session transaction serially. Distance-table, codebook,
candidate, and match capacity are admitted before materialization; an
optional meter or unbounded raw result collection is not a valid path.

## Verification and Change Impact

PQ tests must cover trained and untrained paths, codebook/code shape failures,
missing canonical vectors, bounded point-read maxima, distance/order parity,
budget/cancellation, later failure, foreign-meter rejection, and final owner
release.
