# LeaderboardIndex

Time-windowed leaderboard rankings with automatic window rotation and efficient top-K queries.

## Overview

LeaderboardIndex provides high-performance leaderboard functionality with time-based windows (hourly, daily, weekly, monthly). Entries are automatically organized into time windows, enabling queries for current rankings, historical data, and automatic cleanup of old windows.

**Algorithm**:
- **Score Inversion**: Stores `UInt64.max - score` for descending order via FDB's ascending key sort
- **Time Windows**: Windows are identified by `floor(timestamp / windowDuration)`
- **Reference**: FDB Record Layer TIME_WINDOW_LEADERBOARD index type

**Storage Layout**:
```
// Window entries (sorted by score descending via inversion)
Key: [indexSubspace]["window"][windowId][groupKey...][invertedScore][primaryKey]
Value: '' (empty)

// Position tracking (for updates)
Key: [indexSubspace]["pos"][primaryKey]
Value: Tuple(windowId, score, grouping...)

// Window metadata
Key: [indexSubspace]["meta"]["start"][windowId]
Value: Int64 (Unix timestamp of window start)

Example:
  [I]/Game_leaderboard/["window"]/[19750]/[-9223372036854774808]/["player1"] = ''
  [I]/Game_leaderboard/["pos"]/["player1"] = Tuple(19750, 1000)
  [I]/Game_leaderboard/["meta"]["start"]/[19750] = 1706400000
```

**Score Inversion for Descending Order**:
```
┌─────────────────────────────────────────────────────────────────┐
│                     Score Inversion                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Problem: FDB sorts keys in ascending order                     │
│  Solution: Store inverted scores so smaller keys = higher scores │
│                                                                  │
│  Inversion formula (using UInt64 for full Int64 range):         │
│    invertedScore = Int64(bitPattern: UInt64.max - UInt64(score))│
│                                                                  │
│  Example:                                                        │
│    score 100  → invertedScore -101  (smaller in signed order)   │
│    score 50   → invertedScore -51                               │
│    score 0    → invertedScore -1                                │
│                                                                  │
│  FDB key order (ascending): -101, -51, -1                       │
│  Logical score order (descending): 100, 50, 0  ✓                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Use Cases

### 1. Game Leaderboard

**Scenario**: Daily high score rankings for a mobile game.

```swift
@Persistable
struct GameScore {
    var id: String = ULID().ulidString
    var playerId: String = ""
    var score: Int64 = 0
    var level: Int = 1

    // Daily leaderboard, keep 7 days of history
    #Index<GameScore>(type: TimeWindowLeaderboardIndexKind(
        scoreField: \.score,
        window: .daily,
        windowCount: 7
    ))
}

// Get top 10 players today
let top10 = try await context.leaderboard(GameScore.self)
    .index(\.score)
    .top(10)
    .execute()

for (rank, entry) in top10.enumerated() {
    print("\(rank + 1). \(entry.item.playerId): \(entry.score) points")
}

// Get player's current rank
let myRank = try await context.leaderboard(GameScore.self)
    .index(\.score)
    .rank(for: "player123")

if let rank = myRank {
    print("Your rank: #\(rank)")
}
```

**Performance**: O(k) for top-K queries, O(n) for rank lookup.

### 2. Regional Leaderboards

**Scenario**: Separate leaderboards per region.

```swift
@Persistable
struct RegionalScore {
    var id: String = ULID().ulidString
    var playerId: String = ""
    var region: String = "global"
    var score: Int64 = 0

    // Leaderboard grouped by region
    #Index<RegionalScore>(type: TimeWindowLeaderboardIndexKind(
        scoreField: \.score,
        groupBy: [\.region],
        window: .daily,
        windowCount: 7
    ))
}

// Get top 10 in Asia region
let asiaTop = try await context.leaderboard(RegionalScore.self)
    .index(\.score)
    .group(by: ["asia"])
    .top(10)
    .execute()

// Get rank within specific region
let asiaRank = try await context.leaderboard(RegionalScore.self)
    .index(\.score)
    .group(by: ["asia"])
    .rank(for: "player123")
```

### 3. Weekly Contest

**Scenario**: Weekly competition with historical access.

```swift
@Persistable
struct ContestEntry {
    var id: String = ULID().ulidString
    var userId: String = ""
    var points: Int64 = 0
    var contestType: String = "standard"

    // Weekly leaderboard, keep 4 weeks
    #Index<ContestEntry>(type: TimeWindowLeaderboardIndexKind(
        scoreField: \.points,
        window: .weekly,
        windowCount: 4
    ))
}

// Get current week's top 100
let currentTop = try await context.leaderboard(ContestEntry.self)
    .index(\.points)
    .top(100)
    .execute()

// Get available historical windows
let windows = try await context.leaderboard(ContestEntry.self)
    .index(\.points)
    .availableWindows()

// Query last week's results
if let lastWeekWindow = windows.dropFirst().first {
    let lastWeekTop = try await context.leaderboard(ContestEntry.self)
        .index(\.points)
        .window(lastWeekWindow)
        .top(100)
        .execute()
}
```

### 4. Fusion Query with Leaderboard

**Scenario**: Combine leaderboard ranking with vector similarity.

```swift
@Persistable
struct Streamer {
    var id: String = ULID().ulidString
    var name: String = ""
    var viewerCount: Int64 = 0
    var contentEmbedding: [Float] = []

    #Index<Streamer>(type: TimeWindowLeaderboardIndexKind(
        scoreField: \.viewerCount,
        window: .hourly,
        windowCount: 24
    ))
    #Index<Streamer>(type: HNSWIndexKind(
        field: \.contentEmbedding, dimensions: 256
    ))
}

// Find streamers that are both popular AND match user interests
let results = try await context.fuse(Streamer.self) {
    // Top 100 by viewer count
    Leaderboard(\.viewerCount).top(100)

    // Similar content to user's preferences
    Similar(\.contentEmbedding, dimensions: 256).nearest(to: userInterestVector, k: 50)
}
.algorithm(.rrf())
.execute()
```

### 5. Hourly Activity Tracking

**Scenario**: Track and rank user activity by the hour.

```swift
@Persistable
struct UserActivity {
    var id: String = ULID().ulidString
    var userId: String = ""
    var activityScore: Int64 = 0
    var activityType: String = "general"

    // Hourly leaderboard, keep 24 hours
    #Index<UserActivity>(type: TimeWindowLeaderboardIndexKind(
        scoreField: \.activityScore,
        window: .hourly,
        windowCount: 24
    ))
}

// Get most active users this hour
let activeUsers = try await context.leaderboard(UserActivity.self)
    .index(\.activityScore)
    .top(50)
    .execute()
```

## Design Patterns

### Window Types

```
┌─────────────────────────────────────────────────────────────────┐
│                    Window Type Selection                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  LeaderboardWindowType:                                          │
│                                                                  │
│  .hourly    → 3600 seconds    → Real-time competitions          │
│  .daily     → 86400 seconds   → Daily challenges (default)      │
│  .weekly    → 604800 seconds  → Weekly contests                 │
│  .monthly   → ~2592000 seconds → Monthly rankings               │
│                                                                  │
│  Window ID calculation:                                          │
│    windowId = floor(timestamp / windowDuration)                  │
│                                                                  │
│  Example (daily, Jan 28, 2024 12:00 UTC):                       │
│    timestamp = 1706443200                                        │
│    windowId = 1706443200 / 86400 = 19750                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

| Window Type | Duration | Use Case |
|-------------|----------|----------|
| `.hourly` | 1 hour | Live events, real-time rankings |
| `.daily` | 24 hours | Daily challenges, standard games |
| `.weekly` | 7 days | Weekly contests, tournaments |
| `.monthly` | ~30 days | Monthly rankings, seasonal stats |

### Automatic Window Cleanup

Old windows are automatically cleaned up based on `windowCount`:

```swift
// Keep only last 7 daily windows
#Index<GameScore>(type: TimeWindowLeaderboardIndexKind(
    scoreField: \.score,
    window: .daily,
    windowCount: 7  // Windows older than 7 days are deleted
))
```

### Tie Breaking

When scores are equal, entries are ordered by primary key (stable, deterministic):

```
Score 1000: player-aaa (inserted first)
Score 1000: player-bbb (inserted second)
Score 1000: player-ccc (inserted third)
```

All three appear in top-K results with the same score, ordered by primary key.

### Position Tracking for Updates

Each entry's position is tracked for efficient updates:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Update Flow                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Score update received for player123                         │
│                                                                  │
│  2. Read position: ["pos"]["player123"]                         │
│     → Tuple(windowId: 19750, score: 500, grouping...)           │
│                                                                  │
│  3. Delete old window entry:                                     │
│     ["window"][19750][...][inverted(500)]["player123"]          │
│                                                                  │
│  4. Insert new window entry:                                     │
│     ["window"][19751][...][inverted(800)]["player123"]          │
│                                                                  │
│  5. Update position:                                             │
│     ["pos"]["player123"] → Tuple(19751, 800, grouping...)       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Top-K queries | ✅ Complete | Descending order via inversion |
| Rank lookup | ✅ Complete | Count entries with higher score |
| Time windows (hourly/daily/weekly/monthly) | ✅ Complete | Configurable duration |
| Window rotation | ✅ Complete | Automatic cleanup of old windows |
| Grouping support | ✅ Complete | Multi-level grouping |
| Position tracking | ✅ Complete | For efficient updates |
| Historical window queries | ✅ Complete | Query past windows |
| Negative scores | ✅ Complete | Full Int64 range |
| Sparse index (nil scores) | ✅ Complete | Nil values excluded |
| Fusion integration | ✅ Complete | Leaderboard query |
| Bottom-K queries | ✅ Implemented (internal) | Available in maintainer, not yet exposed via public query API |
| Percentile queries | ✅ Implemented (internal) | Available in maintainer, not yet exposed via public query API |
| Dense ranking | ✅ Implemented (internal) | Available in maintainer, not yet exposed via public query API |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert | O(1) | Single key write + position |
| Update | O(1) | Delete old + insert new |
| Delete | O(1) | Clear entry + position |
| Get top K | O(k) | Range scan, k entries |
| Get rank | O(n) | Scan entries with higher score |
| Get available windows | O(w) | w = number of windows |
| Window cleanup | O(n) | n = entries in old window |

### Storage Overhead

| Component | Storage |
|-----------|---------|
| Window entry | ~30-50 bytes per entry |
| Position tracking | ~20-40 bytes per entry |
| Window metadata | ~16 bytes per window |

**Total per entry**: ~50-90 bytes (excluding primary key size)

### FDB Considerations

- **Score Range**: Full Int64 range supported (-2^63 to 2^63-1)
- **Window IDs**: Int64 (sufficient for billions of windows)
- **Key Size**: Primary key must fit within FDB's 10KB key limit
- **Transaction Size**: Large batch insertions may approach 10MB limit

## Benchmark Results

Run with: `swift test --filter LeaderboardIndexPerformanceTests`

### Insert Performance

| Records | Window | Insert Time | Throughput |
|---------|--------|-------------|------------|
| 100 | daily | ~20ms | ~5,000/s |
| 1,000 | daily | ~200ms | ~5,000/s |
| 10,000 | daily | ~2s | ~5,000/s |

### Query Performance

| Entries in Window | Query Type | Latency (p50) |
|-------------------|------------|---------------|
| 1,000 | Top 10 | ~2ms |
| 1,000 | Top 100 | ~5ms |
| 10,000 | Top 10 | ~2ms |
| 10,000 | Top 100 | ~5ms |
| 10,000 | Top 1,000 | ~15ms |

### Rank Lookup Performance

| Entries in Window | Rank Position | Latency (p50) |
|-------------------|---------------|---------------|
| 1,000 | #1 | ~1ms |
| 1,000 | #500 | ~5ms |
| 1,000 | #1000 | ~10ms |
| 10,000 | #1 | ~1ms |
| 10,000 | #5000 | ~30ms |

### Update Performance

| Operation | Latency (p50) |
|-----------|---------------|
| Score update (same window) | ~3ms |
| Score update (new window) | ~5ms |
| Delete | ~2ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [FDB Record Layer Leaderboards](https://github.com/FoundationDB/fdb-record-layer) - Reference implementation
- [Time-Windowed Ranking](https://en.wikipedia.org/wiki/Ranking) - Ranking algorithms
- [Key Ordering in FDB](https://apple.github.io/foundationdb/developer-guide.html#key-ordering) - FoundationDB documentation
