# Feature Implementation Design Document

## Overview

This document outlines the design for implementing missing features from FDB Record Layer.

---

## 1. Cascades Optimizer

### Background

The Cascades optimizer is a rule-based query optimizer framework developed by Goetz Graefe.
Reference: "The Cascades Framework for Query Optimization" (Graefe, 1995)

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CascadesOptimizer                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Memo      │  │  RuleSet    │  │ PropertyEnforcer    │  │
│  │ (Groups +   │  │ (Transform  │  │ (Physical Props)    │  │
│  │  Expressions)│  │  + Impl)   │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  Search Engine                       │    │
│  │  - Top-down optimization with branch and bound       │    │
│  │  - Memoization of best plans per group              │    │
│  │  - Property-based pruning                           │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. **Memo**: Hash-consed DAG of expression equivalence classes
2. **Group**: Set of logically equivalent expressions
3. **Expression**: Operator + children (references to groups)
4. **Rule**: Transformation or implementation pattern
5. **Property**: Required physical properties (ordering, distribution)

### Implementation Plan

```swift
// Expression representation
public enum LogicalOperator {
    case scan(table: String)
    case filter(predicate: Expression)
    case project(fields: [String])
    case join(type: JoinType, condition: Expression)
    case aggregate(groupBy: [String], aggregates: [AggregateExpr])
    case union, intersection
    case sort(keys: [SortKey])
    case limit(count: Int, offset: Int?)
}

// Memo structure
public final class Memo {
    private var groups: [GroupID: Group]
    private var expressionIndex: [ExpressionHash: GroupID]

    func getOrCreateGroup(for expr: LogicalOperator) -> GroupID
    func addExpression(_ expr: Expression, to group: GroupID)
}

// Rule interface
public protocol TransformationRule {
    var pattern: Pattern { get }
    func apply(to match: Match, memo: Memo) -> [Expression]
}

public protocol ImplementationRule {
    var pattern: Pattern { get }
    func apply(to match: Match, context: OptimizationContext) -> PhysicalOperator?
}
```

---

## 2. Plan Caching

### Design

Plan cache stores compiled query plans for reuse.

```swift
public final class PlanCache<T: Persistable>: Sendable {
    private let cache: Mutex<LRUCache<CacheKey, CachedPlan<T>>>
    private let maxSize: Int
    private let ttl: TimeInterval

    struct CacheKey: Hashable {
        let predicateSignature: String  // Parameterized predicate hash
        let sortSignature: String
        let projectionSignature: String
    }

    struct CachedPlan<T> {
        let plan: QueryPlan<T>
        let createdAt: Date
        let hitCount: Int
        let statisticsVersion: Int
    }
}
```

### Cache Invalidation

1. **TTL-based**: Plans expire after configurable duration
2. **Statistics-based**: Invalidate when table statistics change significantly
3. **Schema-based**: Invalidate on schema changes (index add/drop)

---

## 3. Remote Fetch Optimization

### Concept

Minimize round-trips by batching fetches and using "covering" information from indexes.

### Implementation

```swift
/// Batched fetch strategy
public struct RemoteFetchOptimizer<T: Persistable> {
    /// Analyze plan for fetch patterns
    func analyzeFetchPattern(_ plan: QueryPlan<T>) -> FetchStrategy

    /// Rewrite plan to use batched fetches
    func optimizeFetches(_ plan: QueryPlan<T>) -> QueryPlan<T>
}

public enum FetchStrategy {
    case sequential           // Fetch one by one (simple)
    case batched(size: Int)   // Batch multiple IDs
    case prefetch            // Prefetch based on pattern
    case coveringIndex       // No fetch needed (index-only)
}
```

---

## 4. COUNT_UPDATES Index

### Purpose

Track the number of times a record has been updated.

### Key Structure

```
Key: [indexSubspace][primaryKey]
Value: Int64 (update count)
```

### Implementation

```swift
public struct CountUpdatesIndexKind<Root: Persistable>: IndexKind {
    public static var identifier: String { "count_updates" }
    public static var subspaceStructure: SubspaceStructure { .flat }

    public let fieldNames: [String]  // Primary key field

    public var indexName: String {
        "\(Root.persistableType)_updates_\(fieldNames.joined(separator: "_"))"
    }
}
```

---

## 5. COUNT_NOT_NULL Index

### Purpose

Count records where a specific field is not null, grouped by other fields.

### Key Structure

```
Key: [indexSubspace][groupKey1][groupKey2]...
Value: Int64 (count of non-null values)
```

### Implementation

```swift
public struct CountNotNullIndexKind<Root: Persistable>: IndexKind {
    public static var identifier: String { "count_not_null" }
    public static var subspaceStructure: SubspaceStructure { .aggregation }

    public let groupByFieldNames: [String]
    public let valueFieldName: String  // Field to check for null

    public var fieldNames: [String] {
        groupByFieldNames + [valueFieldName]
    }
}
```

---

## 6. BITMAP_VALUE Index

### Purpose

Efficient set membership and cardinality operations for low-cardinality fields.

### Algorithm

Reference: Roaring Bitmaps (Lemire et al., 2016)

### Key Structure

```
Key: [indexSubspace][fieldValue][containerIndex]
Value: Roaring bitmap container (array, bitmap, or run container)
```

### Implementation

```swift
public struct BitmapIndexKind<Root: Persistable>: IndexKind {
    public static var identifier: String { "bitmap" }
    public static var subspaceStructure: SubspaceStructure { .hierarchical }

    public let fieldNames: [String]

    public var indexName: String {
        "\(Root.persistableType)_bitmap_\(fieldNames.joined(separator: "_"))"
    }
}

/// Roaring bitmap container types
enum RoaringContainer {
    case array([UInt16])           // For sparse containers (<4096 values)
    case bitmap([UInt64])          // For dense containers (1024 x 64-bit words)
    case run([(start: UInt16, length: UInt16)])  // For runs of consecutive values
}
```

### Operations

- **AND**: Bitmap intersection (for AND queries)
- **OR**: Bitmap union (for OR queries)
- **NOT**: Bitmap complement
- **COUNT**: Fast cardinality via POPCNT

---

## 7. TIME_WINDOW_LEADERBOARD Index

### Purpose

Time-windowed ranking with automatic window rotation.

### Key Structure

```
// Current window scores
Key: [indexSubspace]["window"][windowId][score][primaryKey]
Value: ''

// Window metadata
Key: [indexSubspace]["meta"]["current"]
Value: windowId

// Historical aggregates (optional)
Key: [indexSubspace]["history"][windowId]
Value: Tuple(startTime, endTime, topScores...)
```

### Implementation

```swift
public struct TimeWindowLeaderboardIndexKind<Root: Persistable>: IndexKind {
    public static var identifier: String { "time_window_leaderboard" }
    public static var subspaceStructure: SubspaceStructure { .hierarchical }

    /// Field for ranking
    public let scoreFieldName: String

    /// Window duration in seconds
    public let windowDuration: TimeInterval

    /// Number of windows to keep
    public let windowCount: Int

    /// Optional: bucket size for rank queries within window
    public let bucketSize: Int
}

public enum LeaderboardWindow: Sendable {
    case hourly
    case daily
    case weekly
    case monthly
    case custom(duration: TimeInterval)
}
```

---

## 8. Multi-Target OnlineIndexer

### Purpose

Build multiple indexes simultaneously with a single scan of the data.

### Architecture

```
                    ┌──────────────────────────────┐
                    │     MultiTargetIndexer       │
                    ├──────────────────────────────┤
                    │  ┌────────────────────────┐  │
                    │  │     Item Scanner       │  │
                    │  │  (single pass scan)    │  │
                    │  └──────────┬─────────────┘  │
                    │             │                │
                    │      ┌──────┴──────┐         │
                    │      ▼             ▼         │
                    │  ┌────────┐   ┌────────┐     │
                    │  │Index 1 │   │Index 2 │ ... │
                    │  │Maintainer│ │Maintainer│    │
                    │  └────────┘   └────────┘     │
                    └──────────────────────────────┘
```

### Implementation

```swift
public final class MultiTargetOnlineIndexer<Item: Persistable>: Sendable {
    private let targets: [IndexBuildTarget<Item>]

    struct IndexBuildTarget<Item: Persistable> {
        let index: Index
        let maintainer: any IndexMaintainer<Item>
        let stateManager: IndexStateManager
    }

    /// Build all target indexes with single data scan
    public func buildIndexes(clearFirst: Bool = false) async throws {
        // 1. Set all indexes to write-only state
        // 2. Single scan of items
        // 3. For each item, call all maintainers
        // 4. Transition all to readable state
    }
}
```

---

## 9. Mutual Indexing Strategy

### Purpose

Build indexes that have bidirectional dependencies (e.g., graph edges).

### Use Cases

- Graph indexes: forward and reverse edges
- Many-to-many relationships
- Symmetric relationships

### Implementation

```swift
public enum IndexingStrategy: Sendable {
    case byRecords         // Standard: scan records, build index entries
    case byStamps          // Use versionstamps for incremental updates
    case mutual(pairs: [(Index, Index)])  // Bidirectional index building
}

public final class MutualOnlineIndexer<Item: Persistable>: Sendable {
    private let primaryIndex: Index
    private let secondaryIndex: Index
    private let primaryMaintainer: any IndexMaintainer<Item>
    private let secondaryMaintainer: any IndexMaintainer<Item>

    /// Build both indexes ensuring consistency
    public func buildIndexes() async throws {
        // 1. Scan primary data
        // 2. For each item:
        //    - Build primary index entry
        //    - Build secondary (reverse) index entry
        // 3. Verify bidirectional consistency
    }
}
```

---

## Implementation Order

1. **Phase 1: Index Types** (database-kit)
   - COUNT_UPDATES
   - COUNT_NOT_NULL
   - BITMAP_VALUE
   - TIME_WINDOW_LEADERBOARD

2. **Phase 2: Index Maintainers** (database-framework)
   - CountUpdatesIndexMaintainer
   - CountNotNullIndexMaintainer
   - BitmapIndexMaintainer
   - TimeWindowLeaderboardIndexMaintainer

3. **Phase 3: Query Optimizer**
   - Cascades framework
   - Plan caching
   - Remote fetch optimization

4. **Phase 4: OnlineIndexer Extensions**
   - Multi-target indexing
   - Mutual indexing

---

## File Structure

```
database-kit/Sources/
├── Core/
│   └── StandardIndexKinds.swift  # Add new IndexKind types
│
database-framework/Sources/
├── AggregationIndex/
│   ├── CountUpdatesIndexKind.swift
│   ├── CountUpdatesIndexMaintainer.swift
│   ├── CountNotNullIndexKind.swift
│   └── CountNotNullIndexMaintainer.swift
├── BitmapIndex/
│   ├── BitmapIndexKind+Maintainable.swift
│   ├── BitmapIndexMaintainer.swift
│   └── RoaringBitmap.swift
├── LeaderboardIndex/
│   ├── TimeWindowLeaderboardIndexKind+Maintainable.swift
│   └── TimeWindowLeaderboardIndexMaintainer.swift
├── DatabaseEngine/
│   ├── QueryPlanner/
│   │   ├── Cascades/
│   │   │   ├── Memo.swift
│   │   │   ├── Group.swift
│   │   │   ├── Expression.swift
│   │   │   ├── Rule.swift
│   │   │   ├── TransformationRules.swift
│   │   │   ├── ImplementationRules.swift
│   │   │   └── CascadesOptimizer.swift
│   │   ├── PlanCache.swift
│   │   └── RemoteFetchOptimizer.swift
│   └── MultiTargetOnlineIndexer.swift
│   └── MutualOnlineIndexer.swift
```
