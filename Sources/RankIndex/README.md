# RankIndex

RankIndex provides persistent leaderboard reads over an exact numeric field.
It is an index runtime owned by `database-framework`; the model-level index
declaration and metadata contract are owned by `database-kit`.

## Contract

```text
model #Index(.rank)
        |
        v
IndexKindMetadata { scoreType }
        |
        v
[index]/scores/[score][primaryKey] = covering value
[index]/_count                    = atomic Int64
        |
        v
top / bottom / range / percentile / score rank
```

- The score must be an exact numeric `IndexScalarType` supported by the rank
  maintainer.
- `scoreType` is the only kind-specific metadata value.
- A missing or ambiguous rank index is a typed failure. Queries never fall
  back to an in-memory full scan.
- A `nil` optional score is sparse: the entity has no rank entry.
- Non-finite `Float` and `Double` scores are rejected because they do not form
  the required total ordering.
- Position zero is the highest score. Equal scores are ordered by their encoded
  primary key for deterministic traversal; `getRank(score:)` returns the first
  position occupied by that score because it counts only strictly greater
  scores.

## Model declaration

```swift
@Persistable
struct Player {
    var id: String
    var name: String
    var score: Int64

    #Index(.rank, field: \Player.score, name: "player_score_rank")
}
```

The macro compiles the field identity and `scoreType` into the schema. Runtime
construction validates that metadata before selecting the concrete generic
score maintainer.

## Queries

```swift
let leaders = try await context.rank(Player.self)
    .by(Player.fields.score)
    .top(100)
    .execute()

let lowest = try await context.rank(Player.self)
    .by(Player.fields.score)
    .bottom(10)
    .execute()

let page = try await context.rank(Player.self)
    .by(Player.fields.score)
    .range(from: 100, to: 125)
    .execute()

let median = try await context.rank(Player.self)
    .by(Player.fields.score)
    .percentile(0.5)
    .executeOne()
```

Every result returned by `execute()` includes its zero-based position from the
top of the complete leaderboard. Bottom queries return entries in ascending
score order, so their reported positions descend.

Invalid counts, ranges, percentiles, missing entities, malformed counters, and
missing indexes are reported as typed errors. They are not converted to empty
successful results.

## Direct maintainer operations

`RankIndexMaintainer<Item, Score>` exposes the storage-level operations used by
the canonical read executors:

```swift
let top = try await maintainer.getTopK(k: 20, transaction: transaction)
let position = try await maintainer.getRank(
    score: Int64(1_500),
    transaction: transaction
)
let count = try await maintainer.getCount(transaction: transaction)
let percentile = try await maintainer.getPercentile(
    0.95,
    transaction: transaction
)
```

`getTopK(k:)` accepts zero as an empty request and rejects negative values.
The query builder requires a positive count for `top` and `bottom`.

## Storage and complexity

Tuple encoding preserves numeric score ordering. RankIndex therefore uses the
storage engine's bounded ordered range reads directly; it does not materialize
the complete index and does not use a heap.

| Operation | Storage work | Complexity |
|---|---|---:|
| Insert | score key write + atomic count add | O(1) |
| Delete | score key clear + atomic count add | O(1) |
| Top K | reverse ordered range, limit K | O(K) |
| Bottom K | forward ordered range, limit K + count read | O(K) |
| Range `[from, to)` | reverse ordered range, limit `to` | O(to) |
| Rank of score | scan keys with strictly greater score | O(rank) |
| Count | atomic counter read | O(1) |
| Percentile | count read + bounded reverse range | O(target rank + 1) |

The table describes storage entries read, excluding entity materialization for
the returned primary keys.

## Consistency and ownership

Index entry mutation and the atomic counter update occur in the caller's
transaction. Reads use the consistency and cache policy selected by canonical
query execution. A score entry whose entity is missing is an index consistency
failure, not a row to silently omit.

The hot path keeps encoded keys and values as `Bytes`. Conversion to model
values happens only when an output entity is materialized. Ordered range reads
are bounded at the storage boundary, so top and bottom queries do not create a
full-index intermediate collection.

## Verification

Rank behavior tests cover insert, update, delete, sparse scores, ties, exact
numeric ordering, counter failures, negative limits, and key decoding. The
FoundationDB behavior suite exercises the real transaction and ordered-range
path; pure decoding tests cover malformed keys and count invariants without a
backend.
