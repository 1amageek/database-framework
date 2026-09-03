# RankIndex

## Purpose and Scope

RankIndex owns score-ordered reads over a rank index. It decodes ordered index
entries, preserves the rank assigned by the storage order, transfers one
request-owned primary-key result to the DatabaseEngine retained fetch paths,
and materializes each returned entry into the destination row while retaining
the same work-meter claim.

- Parent: [database-framework](../../DESIGN.md).
- Children: none.
- Test owner: [RankIndexTests](../../Tests/RankIndexTests).

This design covers read execution and the read-side scanner. Index write
layout, score encoding, query language semantics, Fusion, and general
DatabaseEngine contracts remain owned by their existing designs.

## Responsibilities and Boundaries

RankIndex owns:

- ordered top, bottom, range, and percentile scan selection;
- decoding a score and composite primary key from one index key;
- bounded `_count` point reads;
- preservation of scan position as the returned rank;
- the handoff from ordered retained primary keys to the DatabaseEngine retained
  model and polymorphic owners;
- rank annotation and footprint-first destination-row admission.

RankIndex does not own:

- authorization policy, snapshot, or session-owned transaction creation;
- polymorphic runtime lookup, stored-value decoding, or model retention;
- raw entity arrays or an unmetered intermediate tuple array. Standalone
  Top-K, count, and percentile reads require a caller-owned meter because they
  have no session boundary; the session executor always consumes its meter.
- persistence layout changes or query semantics.

The DatabaseEngine `DatabaseReadSession` is the authority for authorization and
the session-owned transaction. RankIndex consumes that capability and cannot
open a nested transaction. Session-backed reads never construct an independent
clock or meter.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [database-framework](../../DESIGN.md) | parent | In-process execution and final row contract | RankIndex is a specialized read executor. | Changes to session, authorization, or destination ownership require the parent review. |
| [DatabaseEngine](../DatabaseEngine/DESIGN.md) | depends on | `DatabaseReadSession`, retained primary-key collection, retained persisted models, retained polymorphic entities, `IndexReadResultBuilder` | Supplies the transaction-bound read and destination owners. | RankIndex must not reconstruct raw entity arrays or bypass session authority. |
| [RankIndexTests](../../Tests/RankIndexTests) | used by | Behavioral rank and failure evidence | Verifies ordering, bounds, ownership, and failure behavior. | A source-only compile is not evidence for ownership or cleanup. |

## Architecture

```text
DatabaseReadSession
    -> Rank executor
        -> bounded ordered RankScanner
            -> retained primary-key owner (same work meter)
                -> retained model or polymorphic fetch (same session)
                    -> IndexReadResultBuilder
                        -> rank annotation + retained row output
```

The ownership boundary is one-way:

```text
scan entry: (score, primary key, position)
       | decode and retain
       v
primary-key collection
       | consume through session API
       v
polymorphic retained entry
       | borrow only while destination admission is live
       v
final IndexReadRow
```

## Contracts and Invariants

- Storage order is the ranking order: top is descending, bottom is ascending
  storage order mapped back to descending rank, range is `[from, to)`, and
  percentile selects the same zero-based descending position as before.
- Composite primary-key tuple elements remain in their encoded order and are
  never reduced to a scalar identifier.
- Each physical key is admitted for its exact packed byte count before it is
  retained. The retained key owner keeps tuple views and its decode
  reservation alive; enclosing backend range owners are copied at this
  boundary.
- Tuple decoding uses one cursor pass: the score is decoded once and the
  remaining elements form one retained primary-key tuple. Every tuple payload
  or existential-capacity increment is admitted by the same reservation before
  its allocation.
- The scanner does not return a raw `[RankScanEntry]`; each decoded entry is
  retained until the ordered primary-key owner receives it or the operation
  fails and releases it.
- `_count` is read through the bounded point-read contract before decoding; an
  unbounded raw point read is not part of rank execution.
- Authorization and session admission happen before index or model storage
  reads. RankIndex does not use a task-local or caller-supplied authorization
  shortcut.
- The primary-key owner, fetched polymorphic entry, and destination row use
  one `DatabaseWorkMeter`; foreign-meter composition is rejected.
- The regular and polymorphic fetch APIs consume the
  `DatabaseRetainedPrimaryKeyCollection` directly. RankIndex exposes only
  scoped synchronous entry borrows for annotation and scoped asynchronous
  primary-key borrows for the fetch boundary; no collection or tuple subscript
  is part of the scan result contract.
- Destination footprint is admitted before row materialization. Rank is added
  as the `rank` annotation in the same destination admission as the model.
- Missing index entries remain explicit failures where the existing rank
  contract requires them; they are not silently removed from an ordered page.
- Cursor completion is awaited. If iteration and cursor cleanup both fail, the
  combined cleanup error is preserved.
- Malformed entries, missing entities, authorization failures, budget failures,
  cancellation, and later-entry failures release all earlier owners and do not
  become successful partial results.
- `RankReadResultAssembler` was not a production execution boundary. Its raw
  entity-array contract is removed because retained row assembly is now the
  only execution path; no compatibility alias is required.

## Runtime Flows

### Ordered scan and row materialization

```text
authorize session/index
    -> bounded scan (top/bottom/range/percentile)
    -> retain each composite primary key with its scan position
    -> consume key collection through the session-owned retained fetch
    -> for each position, borrow key-free rank annotation
    -> borrow the primary key only at a fetch or error boundary
    -> admit row footprint including rank annotation
    -> append the row
    -> release source owners after destination completion
```

The loop preserves one output position for every requested index position. A
missing fetched entity raises the existing typed rank failure at its position.

## State, Ownership, and Lifecycle

The scanner owns only operation-local cursor state. A scan owner is released
after cursor completion and destination transfer. The retained primary-key
collection owns decoded tuple storage and its reservation until the session
fetch consumes it. Each entry also retains the source key owner so byte-backed
score or key elements cannot outlive their storage. Rank annotations are
borrowed without exposing the primary key. A primary key is borrowed only
through the scoped collection API at a fetch, missing-entity error, or
public-output boundary. The retained polymorphic aggregate owns decoded model,
identifier, and type metadata until the destination row is appended or the
operation fails. No owner escapes as an unconstrained array.

## Failure, Concurrency, and Constraints

Rank reads stay bound to the session transaction. The scanner uses a
bounded range limit and does not sort a materialized full index. `_count` is a
single bounded read. When a bottom page reaches the recorded tail, the scan
inspects at most one additional entry and reports an undercount at that scanned
boundary; it does not audit the unscanned remainder. A cancellation or budget
error interrupts the operation, awaits cursor cleanup, and releases retained
source and destination owners.

The rank implementation does not add a fallback when an index or entity is
missing. It propagates the existing typed error and preserves the previous
ordering and rank semantics.

## Verification and Change Impact

`RankIndexTests` must prove the behavior rather than only declarations:

| Invariant | Evidence |
|---|---|
| Ordered top/bottom/range/percentile rank and composite keys | scanner and executor behavioral tests |
| Bounded `_count` read | storage read counter and maximum-byte assertion |
| Auth before storage | denied policy test with zero storage reads |
| Same-meter ownership and destination admission | foreign-meter and budget tests |
| Malformed, missing, later failure, cancellation, and cleanup | failure-path tests requiring zero remaining meter |
| Cursor cleanup composition | scanner cleanup error test |

The affected RankIndex target is compiled before focused tests. The focused
lane runs once at a stable implementation boundary and reports exact passed,
failed, skipped, expected-failure, runtime-warning, and internal-error counts.
Changes to DatabaseEngine retained APIs invalidate this design's integration
evidence and require a new focused run.
