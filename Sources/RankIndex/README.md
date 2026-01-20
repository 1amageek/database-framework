# RankIndex

Leaderboard and ranking queries using Range Tree algorithm.

## Overview

RankIndex provides efficient ranking operations for leaderboard-style queries. It maintains a score-ordered index with atomic counters for O(1) count operations and O(n log k) top-K queries using a bounded min-heap.

**Algorithms**:
- **Range Tree (Simplified)**: Score-ordered entries for rank calculation
- **TopKHeap**: Bounded min-heap for O(n log k) top-K queries
- **Atomic Counter**: O(1) count queries via FDB atomic operations

**Storage Layout**:
```
[indexSubspace]/scores/[score][primaryKey] = ''
[indexSubspace]/_count = Int64 (total entries)
```

Where `score` is the ranking value (Int64, Double, etc.) and entries are naturally sorted.

## Use Cases

### 1. Game Leaderboard

**Scenario**: Display top players by score.

```swift
@Persistable
struct Player {
    var id: String = ULID().ulidString
    var name: String = ""
    var score: Int64 = 0

    #Index<Player>(
        type: RankIndexKind(field: \.score)
    )
}

// Get top 100 players
let leaderboard = try await context.rank(Player.self)
    .by(\.score)
    .top(100)
    .execute()

for (player, rank) in leaderboard {
    print("\(rank + 1). \(player.name): \(player.score)")
}
```

**Performance**: O(n log k) where n = total entries, k = requested count.

### 2. Player Rank Lookup

**Scenario**: Show a player their current rank.

```swift
// Find player's rank
let myScore: Int64 = 1500
let rank = try await maintainer.getRank(score: myScore, transaction: tx)
print("Your rank: #\(rank + 1)")  // 0-based to 1-based

// Total player count
let total = try await maintainer.getCount(transaction: tx)
print("Out of \(total) players")
```

**Performance**: O(n - rank) - faster for high-ranked players.

### 3. Percentile Queries

**Scenario**: Find the 95th percentile score for matchmaking.

```swift
// Get 95th percentile score
let percentile95 = try await maintainer.getPercentile(
    0.95,
    transaction: tx
)

if let score = percentile95 {
    print("95th percentile score: \(score)")
}

// Get median score
let median = try await context.rank(Player.self)
    .by(\.score)
    .percentile(0.5)
    .executeOne()
```

**Performance**: O(n log k) where k = targetRank + 1.

### 4. Paginated Leaderboard

**Scenario**: Show leaderboard page by page.

```swift
// Get ranks 20-29 (page 3)
let page3 = try await context.rank(Player.self)
    .by(\.score)
    .range(from: 20, to: 30)
    .execute()

for (player, rank) in page3 {
    print("\(rank + 1). \(player.name): \(player.score)")
}
```

### 5. Multi-Region Leaderboard

**Scenario**: Separate leaderboards per region.

```swift
@Persistable
struct RegionalPlayer {
    var id: String = ULID().ulidString
    var name: String = ""
    var region: String = ""
    var score: Int64 = 0

    // Index per region (using dynamic directory)
    #Directory<RegionalPlayer>("game", Field(\.region), "players")

    #Index<RegionalPlayer>(
        type: RankIndexKind(field: \.score)
    )
}

// Query specific region
let asiaLeaderboard = try await context.rank(RegionalPlayer.self)
    .partition(\.region, equals: "asia")
    .by(\.score)
    .top(100)
    .execute()
```

### 6. Sparse Leaderboard (Optional Scores)

**Scenario**: Only rank players who have submitted scores.

```swift
@Persistable
struct Tournament {
    var id: String = ULID().ulidString
    var playerName: String = ""
    var submittedScore: Int64? = nil  // nil until submitted

    #Index<Tournament>(
        type: RankIndexKind(field: \.submittedScore)
    )
}

// Players with nil score are NOT indexed
// Only submitted scores appear in rankings
```

## Design Patterns

### Type-Safe Score Parameter

RankIndex uses a generic Score type parameter for compile-time safety:

```swift
// Int64 scores (default)
RankIndexKind<Player, Int64>(field: \.score)

// Double scores (for ratings like 4.5 stars)
RankIndexKind<Review, Double>(field: \.rating)

// Int32 scores
RankIndexKind<Game, Int32>(field: \.level)
```

**Supported Types**: `Int64`, `Int`, `Int32`, `Double`, `Float`

### TopKHeap Algorithm

For top-K queries, RankIndex uses a bounded min-heap:

```
Algorithm: Maintain min-heap of size k
- For each entry in index:
  - If heap.count < k: insert entry
  - If entry.score > heap.min: replace heap.min with entry
- Result: top-k highest scores in O(n log k) time

┌─────────────────────────────────────────────────────────────┐
│ Min-Heap (k=3)              After processing all entries    │
│                                                             │
│      [700]  ← smallest of top-3                             │
│      /    \                                                 │
│   [800]  [900]                                              │
│                                                             │
│ Final sorted output: [900, 800, 700] (descending)           │
└─────────────────────────────────────────────────────────────┘
```

**Comparison**:
- Full sort: O(n log n)
- TopKHeap: O(n log k)
- For k << n, this is significantly faster

### Atomic Counter

Total count is maintained using FDB atomic operations:

```swift
// On insert
transaction.atomicOp(key: countKey, param: +1, mutationType: .add)

// On delete
transaction.atomicOp(key: countKey, param: -1, mutationType: .add)

// Query count: O(1)
let count = try await transaction.getValue(for: countKey)
```

**Benefits**:
- O(1) count queries (no scanning)
- Concurrent updates without conflicts
- Exact count (not approximate)

### Tie-Breaking

When multiple entries have the same score, they are ordered by primary key:

```
Index entries:
[scores][1000]["alice"] = ''
[scores][1000]["bob"] = ''
[scores][1000]["charlie"] = ''
[scores][500]["david"] = ''

Top-3 result: alice(1000), bob(1000), charlie(1000)
Rank of david: 3 (three entries above)
```

**Note**: Primary key ordering is lexicographic, not insertion order.

### Rank Calculation Optimization

`getRank()` only scans entries with higher scores:

```swift
// To find rank of score 500:
// Only scan entries where score > 500
let boundaryKey = scoresSubspace.pack(Tuple(score + 1))
let sequence = tx.getRange(from: boundaryKey, to: rangeEnd)
// Count = number of entries with higher score = rank
```

**Performance**:
- Rank 0 (highest): O(0) - no scanning needed
- Rank N: O(N) - scan N entries
- Worst case: O(n) for lowest score

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Top-K query | ✅ Complete | O(n log k) with TopKHeap |
| Bottom-K query | ✅ Complete | Reverse ordering |
| Rank lookup | ✅ Complete | O(n - rank) optimized |
| Percentile query | ✅ Complete | Via getTopK |
| Count query | ✅ Complete | O(1) atomic counter |
| Sparse index (nil) | ✅ Complete | nil scores not indexed |
| Range query | ✅ Complete | Get ranks [from, to) |
| Full Range Tree | ⚠️ Simplified | Hierarchical buckets not implemented |
| Reverse iteration | ❌ Workaround | FDB bindings don't support reverse |

### Future Optimization: Full Range Tree

The current implementation is O(n log k) for top-K. A full Range Tree implementation would provide:

```
Full Range Tree (fdb-record-layer style):
- Hierarchical bucket counts
- O(log n) rank lookup
- O(log n + k) top-K query

For datasets > 100K entries, this would be beneficial.
Reference: FoundationDB Record Layer RankedSet
```

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert | O(1) | Single key write + atomic add |
| Delete | O(1) | Single key clear + atomic add |
| Update | O(1) | Clear old + write new |
| Top-K | O(n log k) | Bounded heap scan |
| Rank lookup | O(n - rank) | Partial scan (high ranks faster) |
| Count | O(1) | Atomic counter read |
| Percentile | O(n log k) | Via top-K |

### FDB Considerations

- **Key size**: Score (8 bytes) + Primary key
- **Atomic operations**: Count uses `atomicOp(.add)` for conflict-free updates
- **Snapshot reads**: Queries use snapshot isolation for consistency

## Benchmark Results

Run with: `swift test --filter RankIndexPerformanceTests`

### Indexing

| Players | Insert Time | Throughput |
|---------|-------------|------------|
| 100 | ~20ms | ~5,000/s |
| 1,000 | ~200ms | ~5,000/s |
| 10,000 | ~2s | ~5,000/s |

### Top-K Query

| Players | K | Latency (p50) |
|---------|---|---------------|
| 1,000 | 10 | ~5ms |
| 1,000 | 100 | ~10ms |
| 10,000 | 10 | ~20ms |
| 10,000 | 100 | ~30ms |
| 10,000 | 1000 | ~100ms |

### Rank Lookup

| Players | Target Rank | Latency (p50) |
|---------|-------------|---------------|
| 1,000 | 0 (highest) | ~2ms |
| 1,000 | 500 (middle) | ~10ms |
| 1,000 | 999 (lowest) | ~20ms |
| 10,000 | 0 | ~2ms |
| 10,000 | 5000 | ~50ms |

### Count Query

| Players | Latency (p50) |
|---------|---------------|
| Any | ~1ms |

*Count is O(1) regardless of dataset size.*

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [FoundationDB Record Layer RankedSet](https://github.com/FoundationDB/fdb-record-layer) - Production-grade Range Tree
- [Range Tree](https://en.wikipedia.org/wiki/Range_tree) - Data structure for range queries
- [Binary Heap](https://en.wikipedia.org/wiki/Binary_heap) - TopKHeap implementation
- [FDB Atomic Operations](https://apple.github.io/foundationdb/api-general.html#atomic-operations) - Conflict-free counters
