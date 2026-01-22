# AggregationIndex

Materialized aggregations maintained incrementally with atomic FDB operations.

## Overview

AggregationIndex provides pre-computed aggregation values that are maintained atomically as data changes. Instead of computing aggregates on-the-fly (which requires scanning all records), AggregationIndex stores computed values that are updated incrementally with each insert, update, or delete operation.

**Aggregation Types**:
- **COUNT**: Count of records grouped by fields
- **COUNT_NOT_NULL**: Count of records where a specific field is not null
- **COUNT_UPDATES**: Track update frequency per record
- **SUM**: Sum of numeric values grouped by fields
- **MIN/MAX**: Minimum/Maximum values using FDB tuple ordering
- **AVERAGE**: Average values (maintains sum + count internally)
- **DISTINCT**: Approximate cardinality using HyperLogLog++ (~0.81% error)
- **PERCENTILE**: Streaming quantile estimation using t-digest (high accuracy at extremes)

**Storage Layout**:
```
COUNT / COUNT_NOT_NULL:
  [indexSubspace][groupValue1][groupValue2]... = Int64

SUM:
  [indexSubspace][groupValue1][groupValue2]... = Int64 (integers)
  [indexSubspace][groupValue1][groupValue2]... = scaled Int64 (floats, 6 decimals)

MIN/MAX:
  [indexSubspace][groupValue1]...[value][primaryKey] = '' (empty)

AVERAGE:
  [indexSubspace][groupValue1]...["sum"] = Int64/scaled Int64
  [indexSubspace][groupValue1]...["count"] = Int64

COUNT_UPDATES:
  [indexSubspace][primaryKey] = Int64 (update count)

DISTINCT (HyperLogLog++):
  [indexSubspace][groupValue1][groupValue2]... = Serialized HLL (~16KB JSON)

PERCENTILE (t-digest):
  [indexSubspace][groupValue1][groupValue2]... = Serialized TDigest (~10KB binary)
```

## Architecture

AggregationIndex follows the EntryPoint → QueryBuilder pattern used by other index types (Vector, FullText, Spatial):

```
┌───────────────────────────────────────────────────────────────────────┐
│                     AggregationQuery Architecture                      │
├───────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  context.aggregate(Order.self)                                         │
│       │                                                                │
│       ▼                                                                │
│  AggregationEntryPoint<Order>                                          │
│       │                                                                │
│       ├── .groupBy(\.region) → AggregationQueryBuilder                 │
│       ├── .count() / .sum() → AggregationQueryBuilder (global)         │
│       └── .using(index:) → force specific index (optional)             │
│                                                                        │
│  AggregationQueryBuilder                                               │
│       │                                                                │
│       ├── .count(as:) / .sum(\_:as:) / .avg(\_:as:)                    │
│       ├── .min(\_:as:) / .max(\_:as:)                                  │
│       ├── .distinct(\_:as:) / .percentile(\_:p:as:)                    │
│       ├── .having { predicate }                                        │
│       └── .execute()                                                   │
│              │                                                         │
│              ▼                                                         │
│       ┌─────────────────────────────────────────┐                      │
│       │   Execution Strategy Selector           │                      │
│       │   (AggregationIndexKindProtocol)        │                      │
│       ├─────────────────────────────────────────┤                      │
│       │ • Check matching indexes for each agg   │                      │
│       │ • All matched? → Index-backed [O(1)]    │                      │
│       │ • Any unmatched? → In-memory [O(n)]     │                      │
│       └─────────────────────────────────────────┘                      │
│              │                                                         │
│              ▼                                                         │
│       [AggregateResult<Order>]                                         │
│                                                                        │
└───────────────────────────────────────────────────────────────────────┘
```

**Key Components**:
| Component | File | Role |
|-----------|------|------|
| `AggregationEntryPoint` | `AggregationEntryPoint.swift` | Entry point from `FDBContext.aggregate()` |
| `AggregationQueryBuilder` | `AggregationQuery.swift` | Fluent API for building queries |
| `AggregationIndexKindProtocol` | `AggregationIndexKindProtocol.swift` | Common protocol for index matching |
| `*IndexMaintainer` | `*IndexMaintainer.swift` | Index-specific maintenance and query |

## Use Cases

### 1. Sales Analytics Dashboard

**Scenario**: Real-time sales metrics by region and category.

```swift
@Persistable
struct Sale {
    var id: String = ULID().ulidString
    var region: String = ""
    var category: String = ""
    var amount: Double = 0.0
    var quantity: Int64 = 0

    // Count by region
    #Index<Sale>(type: CountIndexKind(groupBy: [\.region]))

    // Sum by region
    #Index<Sale>(type: SumIndexKind(groupBy: [\.region], value: \.amount))

    // Sum by region and category (composite grouping)
    #Index<Sale>(type: SumIndexKind(groupBy: [\.region, \.category], value: \.amount))

    // Average order value by category
    #Index<Sale>(type: AverageIndexKind(groupBy: [\.category], value: \.amount))
}

// Get real-time sales count per region (O(1) lookup)
let tokyoSalesCount = try await maintainer.getCount(
    groupingValues: ["Tokyo"],
    transaction: transaction
)

// Get real-time revenue per region (O(1) lookup)
let tokyoRevenue = try await maintainer.getSum(
    groupingValues: ["Tokyo"],
    transaction: transaction
)

// Get average order value for category
let result = try await maintainer.getAverage(
    groupingValues: ["Electronics"],
    transaction: transaction
)
print("Avg order: \(result.average), Count: \(result.count)")
```

**Performance**: O(1) for single group queries, maintained atomically on each sale.

### 2. Min/Max Price Tracking

**Scenario**: Track minimum and maximum prices per category.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var category: String = ""
    var price: Double = 0.0
    var name: String = ""

    // Min price by category
    #Index<Product>(type: MinIndexKind(groupBy: [\.category], value: \.price))

    // Max price by category
    #Index<Product>(type: MaxIndexKind(groupBy: [\.category], value: \.price))
}

// Get cheapest product price in Electronics
let minPrice = try await minMaintainer.getMin(
    groupingValues: ["Electronics"],
    transaction: transaction
)

// Get most expensive product price in Electronics
let maxPrice = try await maxMaintainer.getMax(
    groupingValues: ["Electronics"],
    transaction: transaction
)
```

**Performance**: O(1) using FDB tuple ordering (first/last key in range).

### 3. User Activity Monitoring

**Scenario**: Track active vs inactive users and update frequency.

```swift
@Persistable
struct UserProfile {
    var id: String = ULID().ulidString
    var department: String = ""
    var email: String? = nil  // Optional - some users may not have email
    var status: String = "active"

    // Count by department
    #Index<UserProfile>(type: CountIndexKind(groupBy: [\.department]))

    // Count users with non-null email by department
    #Index<UserProfile>(type: CountNotNullIndexKind(
        groupBy: [\.department],
        value: \.email
    ))

    // Track update frequency per user
    #Index<UserProfile>(type: CountUpdatesIndexKind())
}

// Get user count by department
let engineeringCount = try await countMaintainer.getCount(
    groupingValues: ["Engineering"],
    transaction: transaction
)

// Get count of users with email configured
let usersWithEmail = try await countNotNullMaintainer.getCount(
    groupingValues: ["Engineering"],
    transaction: transaction
)

// Get frequently updated records (hotspots)
let frequentlyUpdated = try await updatesMaintainer.getFrequentlyUpdated(
    threshold: 100,
    transaction: transaction
)
```

### 4. Unique Visitor Tracking (DISTINCT)

**Scenario**: Count unique visitors per page using approximate cardinality.

```swift
@Persistable
struct PageView {
    var id: String = ULID().uuidString
    var pageId: String = ""
    var userId: String = ""
    var timestamp: Date = Date()

    // Unique visitors per page (HyperLogLog++)
    #Index<PageView>(type: DistinctIndexKind(groupBy: [\.pageId], value: \.userId))
}

// Get unique visitor count for a page (O(1) lookup, ~0.81% error)
let (estimated, errorRate) = try await distinctMaintainer.getDistinctCount(
    groupingValues: ["homepage"],
    transaction: transaction
)
print("Unique visitors: ~\(estimated) (±\(errorRate * 100)%)")

// Get all pages with their unique visitor counts
let allCounts = try await distinctMaintainer.getAllDistinctCounts(
    transaction: transaction
)
for (grouping, count, _) in allCounts {
    print("\(grouping): \(count) unique visitors")
}
```

**Important**: HyperLogLog is add-only. Deleting a PageView does NOT decrease the unique count. This index reflects "visitors ever seen", not "current visitors".

### 5. Latency Percentile Monitoring (PERCENTILE)

**Scenario**: Track API response time percentiles (p50, p90, p99).

```swift
@Persistable
struct APIRequest {
    var id: String = ULID().uuidString
    var endpoint: String = ""
    var latencyMs: Double = 0
    var statusCode: Int64 = 200

    // Latency percentiles per endpoint (t-digest)
    #Index<APIRequest>(type: PercentileIndexKind(groupBy: [\.endpoint], value: \.latencyMs))
}

// Get p99 latency for an endpoint (O(1) lookup)
let p99 = try await percentileMaintainer.getPercentile(
    percentile: 0.99,
    groupingValues: ["/api/users"],
    transaction: transaction
)
print("p99 latency: \(p99 ?? 0)ms")

// Get multiple percentiles efficiently
let percentiles = try await percentileMaintainer.getPercentiles(
    percentiles: [0.50, 0.90, 0.95, 0.99],
    groupingValues: ["/api/users"],
    transaction: transaction
)
// percentiles = [0.50: 45.2, 0.90: 120.5, 0.95: 180.3, 0.99: 350.1]

// Get statistics (count, min, max, median)
let stats = try await percentileMaintainer.getStatistics(
    groupingValues: ["/api/users"],
    transaction: transaction
)
if let s = stats {
    print("Requests: \(s.count), Min: \(s.min)ms, Max: \(s.max)ms, Median: \(s.median)ms")
}
```

**Important**: t-digest is add-only. Deleting a request does NOT update the percentiles. This index reflects "latencies ever recorded", not "current request latencies".

### 6. Query Builder API

**Scenario**: SQL-like aggregation queries using fluent API.

```swift
// GROUP BY region with multiple aggregates
let stats = try await context.aggregate(Sale.self)
    .groupBy(\.region)
    .count(as: "orderCount")
    .sum(\.amount, as: "totalSales")
    .avg(\.amount, as: "avgOrderValue")
    .having { $0.aggregateInt64("orderCount") ?? 0 > 10 }
    .execute()

for result in stats {
    // Type-safe accessors for group keys
    if let region = result.groupKeyString("region") {
        print("Region: \(region)")
    }

    // Type-safe accessors for aggregates
    if let orderCount = result.aggregateInt64("orderCount") {
        print("  Orders: \(orderCount)")
    }
    if let totalSales = result.aggregateDouble("totalSales") {
        print("  Total Sales: \(totalSales)")
    }
    if let avgOrder = result.aggregateDouble("avgOrderValue") {
        print("  Avg Order: \(avgOrder)")
    }
}
```

**Type-Safe Result Access**:

| Accessor Method | Return Type | Use For |
|-----------------|-------------|---------|
| `aggregateInt64(_:)` | `Int64?` | count, distinct |
| `aggregateDouble(_:)` | `Double?` | sum, avg, percentile, numeric min/max |
| `aggregateString(_:)` | `String?` | string min/max |
| `groupKeyInt64(_:)` | `Int64?` | integer group keys |
| `groupKeyString(_:)` | `String?` | string group keys |
| `groupKeyDouble(_:)` | `Double?` | double group keys |

**Automatic Index Selection**: The query builder automatically uses precomputed indexes when available (see "Automatic Index Selection" below).

### 7. Sparse Aggregation (Optional Fields)

**Scenario**: Aggregate only records with non-null values.

```swift
@Persistable
struct Survey {
    var id: String = ULID().ulidString
    var category: String = ""
    var rating: Double? = nil  // Optional - not all surveys have ratings

    // Average rating by category (nil ratings excluded)
    #Index<Survey>(type: AverageIndexKind(groupBy: [\.category], value: \.rating))
}

// Average only includes surveys with ratings
let avgRating = try await maintainer.getAverage(
    groupingValues: ["ProductQuality"],
    transaction: transaction
)
// avgRating.count = number of surveys WITH ratings
// avgRating.average = sum of ratings / count
```

**Sparse Index Behavior**: Records with nil values in the aggregated field are automatically excluded from the aggregate.

### 8. Direct Index Access (O(1) Performance)

For performance-critical single aggregations or MIN/MAX queries, use the maintainers directly:

```swift
// O(1) count lookup
let count = try await countMaintainer.getCount(
    groupingValues: ["Tokyo"],
    transaction: transaction
)

// O(1) sum lookup
let sum = try await sumMaintainer.getSum(
    groupingValues: ["Tokyo"],
    transaction: transaction
)

// O(1) average lookup
let result = try await averageMaintainer.getAverage(
    groupingValues: ["Electronics"],
    transaction: transaction
)
print("Sum: \(result.sum), Count: \(result.count), Avg: \(result.average)")

// O(1) min/max lookup (direct access REQUIRED for index-backed MIN/MAX)
let minPrice = try await minMaintainer.getMin(
    groupingValues: ["Electronics"],
    transaction: transaction
)
```

**When to Use Direct Access?**

| Scenario | Recommended Approach |
|----------|---------------------|
| Single aggregation, high frequency | Direct Maintainer (O(1)) |
| Multiple aggregations, same groupBy | Query Builder (automatic index selection) |
| MIN/MAX queries | Direct Maintainer (not supported in batch queries) |
| Ad-hoc analysis | Query Builder (flexible) |

**Note**: MIN/MAX indexes use sorted storage optimized for individual lookups (`getMin()`/`getMax()`), not batch queries. The Query Builder always uses in-memory computation for MIN/MAX aggregations.

## Type Preservation

AggregateResult uses `FieldValue` enum internally for type-safe value handling:

**Group Keys** preserve original types:
```swift
// Group keys retain their original types
let year: Int64? = result.groupKeyInt64("year")      // Int fields
let region: String? = result.groupKeyString("region") // String fields
let rate: Double? = result.groupKeyDouble("rate")     // Double fields
```

**Aggregates** return typed results:
| Aggregation | Return Type | Empty Group |
|-------------|-------------|-------------|
| count | `FieldValue.int64` | `0` |
| sum | `FieldValue.double` | `0.0` |
| avg | `FieldValue.double` | `0.0` |
| min | `FieldValue?` | `nil` |
| max | `FieldValue?` | `nil` |

**Important**: `min`/`max` return `nil` for empty groups, not `0`. This distinguishes "no data" from "minimum is zero".

```swift
// Correctly handle empty groups
if let minAmount = result.aggregateDouble("minAmount") {
    print("Minimum: \(minAmount)")
} else {
    print("No data in this group")
}
```

**Supported Numeric Types** (via `FieldValue`):
- Integers: `Int`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt`, `UInt8`, `UInt16`, `UInt32`, `UInt64`
- Floating-point: `Float`, `Double`

**Grouping Behavior**:
- Empty `groupByFieldNames`: All items grouped into single group (global aggregation)
- Null field values: Treated as `FieldValue.null` and grouped together

## Design Patterns

### Atomic Operations

All aggregation indexes use FDB's atomic mutation operations:

```
┌─────────────────────────────────────────────────────────────────┐
│                 Atomic Update Pattern                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Insert Record                                                  │
│     └── atomic_add(groupKey, +value)                            │
│                                                                 │
│  Delete Record                                                  │
│     └── atomic_add(groupKey, -value)                            │
│                                                                 │
│  Update Record (same group)                                     │
│     └── atomic_add(groupKey, newValue - oldValue)               │
│                                                                 │
│  Update Record (different group)                                │
│     ├── atomic_add(oldGroupKey, -oldValue)                      │
│     └── atomic_add(newGroupKey, +newValue)                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits**:
- No read-before-write (reduces conflict)
- Thread-safe without explicit locking
- Commutative (order-independent)

### Floating-Point Storage

Double values are stored as scaled Int64 for atomic addition:

```swift
// Double → Int64 (6 decimal places preserved)
let scaled: Int64 = Int64(value * 1_000_000)

// Int64 → Double
let original: Double = Double(scaled) / 1_000_000

// Precision: 6 decimal places (e.g., 123456.789012)
// Range: ±9,223,372,036,854.775807
```

**Trade-off**: 6 decimal places is sufficient for most financial calculations. For higher precision, use integer cents/units instead of floating-point currency.

### Min/Max Using Tuple Ordering

Min/Max indexes store values in keys (not values) to leverage FDB's sorted key ordering:

```
Min Query: Get first key in grouping subspace
  ┌─ [Electronics] ─┬─ [100.00] ─┬─ [product1] = ''
  │                 ├─ [150.00] ─┼─ [product3] = ''
  └─ ...            └─ [200.00] ─┴─ [product2] = ''

Max Query: Get last key in grouping subspace (reverse scan)
```

**Complexity**: O(1) for min/max queries (single key lookup with getKey selector).

### Index Selection Guide

| Metric | Index Type | Storage | Query O(n) |
|--------|------------|---------|------------|
| Record count per group | `CountIndexKind` | 8 bytes/group | O(1) |
| Non-null field count | `CountNotNullIndexKind` | 8 bytes/group | O(1) |
| Update frequency | `CountUpdatesIndexKind` | 8 bytes/record | O(1) |
| Sum of values | `SumIndexKind` | 8 bytes/group | O(1) |
| Min value | `MinIndexKind` | ~20 bytes/record | O(1) |
| Max value | `MaxIndexKind` | ~20 bytes/record | O(1) |
| Average (sum/count) | `AverageIndexKind` | 16 bytes/group | O(1) |
| Unique count (approx) | `DistinctIndexKind` | ~16KB/group | O(1) |
| Percentiles (approx) | `PercentileIndexKind` | ~10KB/group | O(1) |

### Grouping Field Selection

```
┌─────────────────────────────────────────────────────────────────┐
│                  Grouping Strategy Guide                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  High cardinality field (userId, orderId)?                      │
│     └── Avoid: Creates too many groups, high storage            │
│                                                                 │
│  Low cardinality field (status, region, category)?              │
│     └── Ideal: Few groups, efficient storage                    │
│                                                                 │
│  Time-based analysis (daily, weekly)?                           │
│     └── Use: date field as grouping (e.g., "2024-01-15")        │
│                                                                 │
│  Multiple dimensions (region + category)?                       │
│     └── Create: Composite grouping index                        │
│                                                                 │
│  Need both (region) and (region+category)?                      │
│     └── Create: Two separate indexes                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## DISTINCT / PERCENTILE Aggregation

### Design Philosophy: Two-Layer Architecture

DISTINCT and PERCENTILE aggregations support both **in-memory computation** and **precomputed indexes**:

| Layer | Method | Index Required? | Complexity | Use Case |
|-------|--------|-----------------|------------|----------|
| **Query Builder** | In-memory | No | O(n) | Most users (90%) |
| **IndexMaintainer** | Precomputed | Yes (`#Index`) | O(1) | High-frequency, large-scale |

**User Experience**: Queries work without explicit index definition. When a matching index exists, it's automatically used for O(1) performance.

### Usage Examples

**Basic Usage (No Index Required)**:
```swift
// Works immediately - computed in-memory
let stats = try await context.aggregate(PageView.self)
    .groupBy(\.pageId)
    .count(as: "totalViews")
    .distinct(\.userId, as: "uniqueVisitors")
    .execute()

let latencyStats = try await context.aggregate(Request.self)
    .groupBy(\.endpoint)
    .avg(\.latencyMs, as: "avgLatency")
    .percentile(\.latencyMs, p: 0.99, as: "p99Latency")
    .execute()
```

**With Precomputed Index (Optional, for Performance)**:
```swift
@Persistable
struct PageView {
    var id: String = ULID().ulidString
    var pageId: String = ""
    var userId: String = ""

    // Define index for O(1) distinct count
    #Index<PageView>(type: DistinctIndexKind(groupBy: [\.pageId], value: \.userId))
}

// Same query - automatically uses index when available
let stats = try await context.aggregate(PageView.self)
    .groupBy(\.pageId)
    .distinct(\.userId, as: "uniqueVisitors")  // O(1) from index
    .execute()
```

### When to Define Precomputed Index?

```
┌─────────────────────────────────────────────────────────────────┐
│        Should I define DISTINCT/PERCENTILE index?               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Dataset size > 1 million records?                              │
│     └── No → Index not needed (in-memory is fast enough)        │
│     └── Yes ↓                                                   │
│                                                                 │
│  Query executed multiple times per second?                      │
│     └── No → Index not needed                                   │
│     └── Yes ↓                                                   │
│                                                                 │
│  Frequent deletions? (HLL/TDigest are add-only)                 │
│     └── Yes → Index not recommended (becomes inaccurate)        │
│     └── No → ✅ Define the index                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Algorithms

| Aggregation | Algorithm | Accuracy | Memory/Group | Reference |
|-------------|-----------|----------|--------------|-----------|
| **DISTINCT** | HyperLogLog++ | ~0.81% error | ~16KB | Heule et al. (Google, 2013) |
| **PERCENTILE** | t-digest | High at extremes (p99.9) | ~10KB | Dunning & Ertl (2019) |

### Important Limitations (Precomputed Index Only)

| Operation | Behavior | Reason |
|-----------|----------|--------|
| Insert | Value added to sketch | Normal |
| Update | New value added, old remains | Sketches are add-only |
| Delete | **Count/percentile unchanged** | Cannot remove from sketch |

**Note**: Precomputed DISTINCT/PERCENTILE indexes reflect "values ever seen", not "current values". For accurate current-state aggregations, use in-memory computation (no index).

### Automatic Index Selection

The Query Builder automatically selects the optimal execution path:

```
AggregationQueryBuilder.execute()
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│              determineExecutionStrategies()                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  For each aggregation:                                           │
│    1. Is it MIN or MAX?                                          │
│       └── Yes → Always in-memory (no batch API)                  │
│                                                                  │
│    2. Find matching IndexDescriptor:                             │
│       • Index kind conforms to AggregationIndexKindProtocol?     │
│       • aggregationType matches? (count, sum, avg, etc.)         │
│       • groupByFieldNames match exactly?                         │
│       • aggregationValueField matches? (if applicable)           │
│                                                                  │
│    3. Match found?                                               │
│       └── Yes → Use IndexMaintainer [O(1)]                       │
│       └── No → Compute in-memory [O(n)]                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Execution Strategy                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  All aggregations have matching indexes?                         │
│    └── Yes → executeWithIndexes() [O(groups)]                    │
│    └── No → In-memory computation [O(n)]                         │
│                                                                  │
│  Note: "All-or-nothing" approach. If ANY aggregation requires    │
│  in-memory, the entire query uses in-memory (O(n) anyway).       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Index Matching Criteria**:
1. Index kind conforms to `AggregationIndexKindProtocol`
2. `aggregationType` matches (count, sum, average, distinct, percentile)
3. `groupByFieldNames` match exactly (same fields, same order)
4. `aggregationValueField` matches (for non-COUNT aggregations)

**Supported for Index-Backed Batch Queries**:
| Aggregation | Index Kind | Batch API |
|-------------|------------|-----------|
| COUNT | `CountIndexKind` | `getAllCounts()` |
| SUM | `SumIndexKind` | `getAllSums()` |
| AVG | `AverageIndexKind` | `getAllAverages()` |
| DISTINCT | `DistinctIndexKind` | `getAllDistinctCounts()` |
| PERCENTILE | `PercentileIndexKind` | `getAllPercentiles()` |
| MIN | `MinIndexKind` | ❌ Not supported (use `getMin()` directly) |
| MAX | `MaxIndexKind` | ❌ Not supported (use `getMax()` directly) |

**Why MIN/MAX Are Excluded**:
MIN/MAX indexes store individual values sorted by the value field, optimized for:
- `getMin(groupingValues:)` - O(1) first key lookup
- `getMax(groupingValues:)` - O(1) last key lookup

They don't have batch APIs (`getAllMins()`/`getAllMaxs()`) because:
1. Each group requires a separate range scan
2. Storage is per-record, not per-group (unlike COUNT/SUM)
3. The Query Builder's in-memory MIN/MAX is already efficient for batch results

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| COUNT aggregation | ✅ Complete | Atomic increment/decrement |
| COUNT_NOT_NULL aggregation | ✅ Complete | Tracks non-null values |
| COUNT_UPDATES aggregation | ✅ Complete | Tracks update frequency |
| SUM aggregation | ✅ Complete | Int64 and Double (scaled) |
| MIN aggregation | ✅ Complete | Uses tuple ordering |
| MAX aggregation | ✅ Complete | Uses tuple ordering |
| AVERAGE aggregation | ✅ Complete | Sum + Count internally |
| Composite grouping | ✅ Complete | Multiple grouping fields |
| Sparse index (nil) | ✅ Complete | nil values excluded |
| Query Builder API | ✅ Complete | Fluent API with HAVING clause |
| Index-backed queries | ✅ Complete | Automatic index selection for COUNT/SUM/AVG/DISTINCT/PERCENTILE |
| DISTINCT aggregation | ✅ Complete | HyperLogLog++ (~0.81% error, add-only) |
| PERCENTILE aggregation | ✅ Complete | t-digest (high accuracy at extremes, add-only) |
| AggregationIndexKindProtocol | ✅ Complete | Common protocol for index matching |
| AggregationEntryPoint | ✅ Complete | EntryPoint pattern (like Vector/FullText) |

**Query Builder Execution Paths**:
| Aggregation | Index Defined | Execution |
|-------------|--------------|-----------|
| COUNT | Yes | O(1) via `CountIndexMaintainer.getAllCounts()` |
| COUNT | No | O(n) in-memory |
| SUM | Yes | O(1) via `SumIndexMaintainer.getAllSums()` |
| SUM | No | O(n) in-memory |
| AVG | Yes | O(1) via `AverageIndexMaintainer.getAllAverages()` |
| AVG | No | O(n) in-memory |
| DISTINCT | Yes | O(1) via `DistinctIndexMaintainer.getAllDistinctCounts()` |
| DISTINCT | No | O(n) in-memory (exact, using Set) |
| PERCENTILE | Yes | O(1) via `PercentileIndexMaintainer.getAllPercentiles()` |
| PERCENTILE | No | O(n) in-memory (exact, using sorted array) |
| MIN | Any | O(n) in-memory (batch query not supported) |
| MAX | Any | O(n) in-memory (batch query not supported) |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert (single group) | O(1) | Atomic add |
| Update (same group) | O(1) | Atomic add (delta) |
| Update (different group) | O(1) | Two atomic adds |
| Delete | O(1) | Atomic add (negative) |
| Get single group | O(1) | Direct key lookup |
| Get all groups | O(G) | G = number of groups |
| Min/Max query | O(1) | Single key selector |
| Average query | O(1) | Two key lookups |

### Storage Overhead

| Index Type | Storage per Group | Storage per Record |
|------------|-------------------|-------------------|
| COUNT | 8 bytes | - |
| COUNT_NOT_NULL | 8 bytes | - |
| COUNT_UPDATES | - | 8 bytes |
| SUM (Int64) | 8 bytes | - |
| SUM (Double) | 8 bytes (scaled) | - |
| MIN/MAX | - | ~20-50 bytes |
| AVERAGE | 16 bytes (sum + count) | - |
| DISTINCT | ~16KB (HyperLogLog) | - |
| PERCENTILE | ~10KB (t-digest) | - |

### FDB Considerations

- **Atomic operations**: Uses FDB's `.add` mutation type
- **No read-modify-write**: Reduces transaction conflicts
- **Hot keys**: High-cardinality grouping creates many keys
- **Transaction limits**: 10MB write limit per transaction

## Benchmark Results

Run with: `swift test --filter AggregationIndexPerformanceTests`

### Indexing (Bulk Insert)

| Records | Index Type | Insert Time | Throughput |
|---------|-----------|-------------|------------|
| 100 | COUNT | ~15ms | ~6,600/s |
| 100 | SUM | ~20ms | ~5,000/s |
| 100 | MIN/MAX | ~25ms | ~4,000/s |
| 100 | AVERAGE | ~25ms | ~4,000/s |
| 1,000 | COUNT | ~150ms | ~6,600/s |
| 1,000 | SUM | ~200ms | ~5,000/s |
| 10,000 | COUNT | ~1.5s | ~6,600/s |

### Query

| Groups | Query Type | Latency (p50) |
|--------|------------|---------------|
| 10 | Get single COUNT | ~1ms |
| 10 | Get single SUM | ~1ms |
| 10 | Get MIN | ~1ms |
| 10 | Get MAX | ~1ms |
| 10 | Get AVERAGE | ~2ms |
| 10 | Get all COUNTs | ~5ms |
| 100 | Get all COUNTs | ~15ms |

### Update Scenarios

| Operation | Latency (p50) | Notes |
|-----------|---------------|-------|
| Insert affecting 1 group | ~1ms | Single atomic add |
| Update same group | ~1ms | Single atomic add |
| Update different group | ~2ms | Two atomic adds |
| Delete from group | ~1ms | Single atomic add |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## Migration Guide

### Breaking Changes (v2.0)

The `AggregateResult` type has been updated for type-safe value handling:

**1. Group Key Access**

```swift
// Before
if let region = result.groupKey["region"] as? String { ... }

// After
if let region = result.groupKeyString("region") { ... }
// Or access FieldValue directly
if let fieldValue = result.groupKey["region"] {
    let region = fieldValue.stringValue
}
```

**2. Aggregate Access**

```swift
// Before
if let count = result.aggregates["orderCount"] as? Int { ... }
if let sum = result.aggregates["totalSales"] as? Double { ... }

// After
if let count = result.aggregateInt64("orderCount") { ... }
if let sum = result.aggregateDouble("totalSales") { ... }
```

**3. Having Clause**

```swift
// Before
.having { $0.aggregates["count"] as? Int ?? 0 > 10 }

// After
.having { $0.aggregateInt64("count") ?? 0 > 10 }
```

**4. Min/Max Empty Handling**

```swift
// Before: returned 0 for empty groups (ambiguous)
let min = result.aggregates["minPrice"] as? Double ?? 0

// After: returns nil for empty groups (explicit)
if let min = result.aggregateDouble("minPrice") {
    // Has data
} else {
    // Empty group - no data
}
```

### Type Mapping

| Old Type | New Type | Accessor |
|----------|----------|----------|
| `[String: any Sendable]` (groupKey) | `[String: FieldValue]` | `groupKeyString()`, `groupKeyInt64()`, `groupKeyDouble()` |
| `[String: any Sendable]` (aggregates) | `[String: FieldValue?]` | `aggregateString()`, `aggregateInt64()`, `aggregateDouble()` |

## References

- [FDB Record Layer Aggregate Indexes](https://github.com/FoundationDB/fdb-record-layer) - Reference implementation
- [Atomic Operations in FDB](https://apple.github.io/foundationdb/developer-guide.html#atomic-operations) - FoundationDB documentation
- [Materialized Aggregates](https://en.wikipedia.org/wiki/Aggregate_function#Incremental_updates) - Database concept
- [Fixed-Point Arithmetic](https://en.wikipedia.org/wiki/Fixed-point_arithmetic) - For floating-point storage
- [HyperLogLog++](https://research.google/pubs/pub40671/) - Heule, Nunkesser, Hall (Google, 2013) - Cardinality estimation algorithm
- [t-digest](https://github.com/tdunning/t-digest) - Dunning & Ertl (2019) - Streaming quantile estimation
