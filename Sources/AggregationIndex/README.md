# AggregationIndex

Materialized aggregations maintained incrementally in storage transactions.

## Overview

AggregationIndex provides pre-computed aggregation values that are maintained transactionally as data changes. Instead of computing aggregates on-the-fly (which requires scanning all records), AggregationIndex stores computed values that are updated incrementally with each insert, update, or delete operation.

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
  [indexSubspace][groupValue1]...["sum"] = Int64 / UInt64 / compensated Double state
  [indexSubspace][groupValue1]...["count"] = positive Int64

MIN/MAX:
  [indexSubspace][0][groupValue1]...[value][primaryKey] = covering value
  [indexSubspace][1][groupValue1]... = Tuple(value, primaryKey...)

AVERAGE:
  [indexSubspace][groupValue1]...["sum"] = Int128 / UInt128 / compensated Double state
  [indexSubspace][groupValue1]...["count"] = positive Int64

COUNT_UPDATES:
  [indexSubspace][primaryKey] = Int64 (update count)

DISTINCT (HyperLogLog++):
  [indexSubspace][0][groupValue1]...[canonicalValue] = positive Int64 refcount
  [indexSubspace][1][groupValue1]... = bounded six-bit HLL binary frame
  [indexSubspace][2][groupValue1]... = fixed 16-byte (unique count, scan bytes)

PERCENTILE (t-digest):
  [indexSubspace][0][groupValue1]...[canonicalDouble] = positive Int64 refcount
  [indexSubspace][1][groupValue1]... = strict bounded TDigest v1 frame
  [indexSubspace][2][groupValue1]... = fixed 16-byte (unique count, scan bytes)
```

For a global aggregation, the empty grouping tuple is represented by the
relevant subspace prefix itself. Because a subspace range excludes that exact
prefix, global COUNT, MIN/MAX summaries, DISTINCT summaries, and PERCENTILE
summaries use bounded direct reads; grouped queries continue to use bounded
range scans.

Nullable grouping fields are encoded as a canonical null tuple value and form
one real group. For sparse aggregates, only a null aggregate value is excluded;
the value is evaluated before grouping so `COUNT_NOT_NULL`, SUM, AVERAGE,
MIN/MAX, DISTINCT, and PERCENTILE never reject an otherwise excluded row merely
because its grouping field is null.

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
│       ├── .count() / .sum() / .distinct() → builder (global)           │
│       ├── .percentile() / .min() / .max() → builder (global)           │
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
│       │   (canonical descriptor metadata)       │                      │
│       ├─────────────────────────────────────────┤                      │
│       │ • Check matching indexes for each agg   │                      │
│       │ • All matched? → Index-backed [O(G)]    │                      │
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
| `AggregationEntryPoint` | `AggregationEntryPoint.swift` | Entry point from `DatabaseContext.aggregate()` |
| `AggregationQueryBuilder` | `AggregationQuery.swift` | Fluent API for building queries |
| `AggregationIndexMetadata` | `AggregationIndexMetadata.swift` | Strict canonical metadata validation and matching |
| `*IndexMaintainerProvider` | `*IndexMaintainerProvider.swift` | Explicit runtime maintainer construction |
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

**Performance**: O(1) for single group queries, maintained transactionally on each sale.

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

`getSum` returns a canonical `FieldValue`. `getAverage` returns a canonical
`FieldValue` average plus its positive membership count; the internal wide sum
is deliberately not exposed through the narrower value model. Code that
specifically requires `Double` can call `getSumAsDouble` or
`getAverageAsDouble`; those adapters reject integer results that would round.

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

The membership layer is exact. Deleting the final reference to a value rebuilds
the affected HLL summary in the same transaction, so the estimate always reflects
the current committed records. Every group stores a fixed 16-byte metadata frame
containing its exact unique-member count and the exact key/value bytes required
for a rebuild. A group whose metadata and summary are not both present is
rejected as corruption before membership mutation.

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

Deleting or changing a request rebuilds every affected digest from exact
reference-counted membership in the same transaction. A summary never includes a
deleted historical value.

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
    if let totalSales = try result.aggregateDouble("totalSales") {
        print("  Total Sales: \(totalSales)")
    }
    if let avgOrder = try result.aggregateDouble("avgOrderValue") {
        print("  Avg Order: \(avgOrder)")
    }
}
```

**Type-Safe Result Access**:

| Accessor Method | Return Type | Use For |
|-----------------|-------------|---------|
| `aggregateInt64(_:)` | `Int64?` | count, distinct |
| `aggregateDouble(_:)` | `throws -> Double?` | Exact sum, avg, percentile, numeric min/max conversion |
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
print("Count: \(result.count), Avg: \(result.average)")

// O(1) min/max lookup for one group
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
| MIN/MAX single-group lookup | Direct Maintainer (O(1)) |
| Ad-hoc analysis | Query Builder (flexible) |

**Note**: MIN/MAX indexes support both direct single-group lookup and bounded
batch scans used by the Query Builder.

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
| sum | exact `FieldValue.int64` / `uint64`, or `double` | `nil` |
| avg | exact integer when integral, otherwise `double` | `nil` |
| min | `FieldValue?` | `nil` |
| max | `FieldValue?` | `nil` |
| distinct | `FieldValue.int64` | `0` |
| percentile | `FieldValue.float64` | `nil` |

**Important**: `min`/`max` return `nil` for empty groups, not `0`. This distinguishes "no data" from "minimum is zero".

`AggregateResult` does not expose an implicit record-count property. A count is
part of the result only when the query explicitly requests `.count(as:)`, and
the returned `FieldValue.int64` is the canonical count. This keeps indexed and
in-memory execution semantically identical: an index that does not maintain a
record count never substitutes an invented zero.

```swift
// Correctly handle empty groups
if let minAmount = try result.aggregateDouble("minAmount") {
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

### Transactional Aggregate Mutations

SUM and AVERAGE replace the typed aggregate and membership count in one
transaction:

```
┌─────────────────────────────────────────────────────────────────┐
│              Transactional Update Pattern                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Insert Record                                                  │
│     ├── checked_add(sum, value)                                 │
│     └── checked_increment(count)                                │
│                                                                 │
│  Delete Record                                                  │
│     ├── checked_subtract(sum, value)                            │
│     ├── checked_decrement(count)                                │
│     └── clear sum/count when count reaches zero                 │
│                                                                 │
│  Update Record (same group)                                     │
│     └── checked_replace(sum, removing: old, adding: new)        │
│                                                                 │
│  Update Record (different group)                                │
│     ├── remove old contribution and membership                 │
│     └── add new contribution and membership                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

Integer overflow, unsigned underflow, negative counts, malformed companion
entries, and non-finite floating-point results are typed failures. The storage
transaction provides the concurrency and rollback boundary.

### Floating-Point Storage

Double values are stored as a fixed two-component Neumaier accumulator:

```swift
state.add(value)
let materialized = try state.total()
```

Both components and their materialized total must remain finite. This preserves
low-order contributions across materialized updates while rejecting NaN,
infinity, and overflow. Applications requiring decimal arithmetic should use an
exact integer unit or an exact decimal field rather than binary floating point.

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
| Sum of values | `SumIndexKind` | 16 bytes integer / 24 bytes floating per group | O(1) |
| Min value | `MinIndexKind` | ~20 bytes/record | O(1) |
| Max value | `MaxIndexKind` | ~20 bytes/record | O(1) |
| Average (sum/count) | `AverageIndexKind` | 24 bytes/group | O(1) |
| Unique count (approx) | `DistinctIndexKind` | membership + bounded HLL/group | O(1) read |
| Percentiles (approx) | `PercentileIndexKind` | membership + bounded digest/group | O(1) read |

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
| **IndexMaintainer** | Precomputed | Yes (`#Index`) | O(1) group read | High-frequency, large-scale |

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
│  Frequent deletes/updates in very large groups?                 │
│     └── Yes → Account for bounded O(D) summary rebuilds         │
│     └── No → Inserts and reads use incremental summaries        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Algorithms

| Aggregation | Algorithm | Accuracy | Memory/Group | Reference |
|-------------|-----------|----------|--------------|-----------|
| **DISTINCT** | HyperLogLog++ | ~0.81% error | ~16KB | Heule et al. (Google, 2013) |
| **PERCENTILE** | t-digest | High at extremes (p99.9) | ~10KB | Dunning & Ertl (2019) |

### Mutation Semantics (Precomputed Index)

| Operation | Behavior | Reason |
|-----------|----------|--------|
| Insert | Refcount update plus incremental summary update | Same transaction |
| Update | Old membership removed, new membership added, affected digest rebuilt | Same transaction |
| Delete | Refcount removed; affected summary rebuilt when required | Same transaction |

Membership scans, group scans, requested-percentile counts, decoded summaries, and
aggregate scanned bytes all have explicit limits. Exceeding a limit fails the
request; it never returns a partial aggregate.

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
│    1. Find matching IndexDescriptor:                             │
│       • canonical kind identifier matches?                       │
│       • aggregationType matches? (count, sum, avg, etc.)         │
│       • groupByFieldNames match exactly?                         │
│       • aggregationValueField matches? (if applicable)           │
│                                                                  │
│    2. Match found?                                               │
│       └── Yes → Use bounded IndexMaintainer scan [O(G)]          │
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
1. Descriptor kind identifier and canonical metadata operation agree
2. Operation matches (count, sum, average, min, max, distinct, percentile)
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
| MIN | `MinIndexKind` | `getAllMins()` |
| MAX | `MaxIndexKind` | `getAllMaxs()` |

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| COUNT aggregation | ✅ Complete | Checked transactional increment/decrement |
| COUNT_NOT_NULL aggregation | ✅ Complete | Tracks non-null values |
| COUNT_UPDATES aggregation | ✅ Complete | Tracks update frequency |
| SUM aggregation | ✅ Complete | Signed, unsigned, and compensated finite Double |
| MIN aggregation | ✅ Complete | Uses tuple ordering |
| MAX aggregation | ✅ Complete | Uses tuple ordering |
| AVERAGE aggregation | ✅ Complete | Sum + Count internally |
| Composite grouping | ✅ Complete | Multiple grouping fields |
| Sparse index (nil) | ✅ Complete | nil values excluded |
| Query Builder API | ✅ Complete | Fluent API with HAVING clause |
| Index-backed queries | ✅ Complete | Automatic index selection for all aggregation families |
| DISTINCT aggregation | ✅ Complete | Delete-capable exact membership + bounded HyperLogLog summary |
| PERCENTILE aggregation | ✅ Complete | Delete-capable exact membership + bounded t-digest summary |
| Canonical metadata matching | ✅ Complete | Strict descriptor metadata, no runtime kind downcast |
| AggregationEntryPoint | ✅ Complete | EntryPoint pattern (like Vector/FullText) |

**Query Builder Execution Paths**:
| Aggregation | Index Defined | Execution |
|-------------|--------------|-----------|
| COUNT | Yes | O(G) via `CountIndexMaintainer.getAllCounts()` |
| COUNT | No | O(n) in-memory |
| SUM | Yes | O(G) via `SumIndexMaintainer.getAllSums()` |
| SUM | No | O(n) in-memory |
| AVG | Yes | O(G) via `AverageIndexMaintainer.getAllAverages()` |
| AVG | No | O(n) in-memory |
| DISTINCT | Yes | O(G) via `DistinctIndexMaintainer.getAllDistinctCounts()` |
| DISTINCT | No | O(n) in-memory (exact, using Set) |
| PERCENTILE | Yes | O(G) via `PercentileIndexMaintainer.getAllPercentiles()` |
| PERCENTILE | No | O(n) in-memory (exact, using sorted array) |
| MIN | Yes | O(groups) via `MinIndexMaintainer.getAllMins()` |
| MIN | No | O(n) in-memory |
| MAX | Yes | O(groups) via `MaxIndexMaintainer.getAllMaxs()` |
| MAX | No | O(n) in-memory |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert (single group) | O(1) | Transactional checked add + count |
| Update (same group) | O(1), or O(D) for sketch rebuild | D = distinct members in the affected sketch group |
| Update (different group) | O(1), or O(D) for sketch rebuild | Transactional remove + add |
| Delete | O(1), or O(D) for sketch rebuild | Clears empty membership and summary groups |
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
| SUM | 16 bytes integer / 24 bytes floating (sum state + count) | - |
| MIN/MAX | one aggregate cache entry | one ordered individual entry |
| AVERAGE | 24 bytes (wide/compensated sum state + count) | - |
| DISTINCT | bounded six-bit HLL summary + 16-byte count/scan metadata | 8-byte refcount per canonical distinct value |
| PERCENTILE | bounded t-digest summary + 16-byte count/scan metadata | 8-byte refcount per canonical Double value |

### FDB Considerations

- **Transactional updates**: Aggregate and membership entries share one commit boundary
- **Conflict safety**: Read-modify-write keys participate in backend conflict detection
- **Hot keys**: High-cardinality grouping creates many keys
- **Transaction limits**: 10MB write limit per transaction

## Benchmark Results

Run with: `xcodebuild test -scheme AggregationIndexFocused -destination 'platform=macOS,arch=arm64' -only-testing:PerformanceBenchmarks/MinMaxBatchBenchmark`

### Latest Results (2026-04-11)

**Environment**: macOS 26.3, Apple M4 Max, local Docker FoundationDB cluster

### MIN/MAX Query Cost (Current Implementation)

**Test Configuration**:
- Groups: 50 regions
- Records per group: 50 sales
- Total records: 2,500
- Warmup: 3 iterations
- Measurement: 30 iterations

| Metric | Baseline | Optimized | Notes |
|--------|----------|-----------|-------|
| **Latency (p50)** | 2.42ms | 3.16ms | Same index-backed query path |
| **Latency (p95)** | 2.64ms | 3.45ms | Repeated run for variance check |
| **Latency (p99)** | 2.74ms | 3.47ms | Low jitter |
| **Throughput** | 377 ops/s | 358 ops/s | 10 grouped results |

**Note**: This benchmark currently runs the same index-backed query in both slots to capture current cost and run-to-run variance. It does not compare against a separate full-scan implementation.

### Aggregation Scalability (Number of Groups)

**Test Setup**: Query varying number of groups (MIN + MAX)

| Groups | Latency (p50) | Latency (p95) | Throughput |
|--------|---------------|---------------|------------|
| 5 groups | 2.45ms | 2.65ms | 360 ops/s |
| 10 groups | 2.40ms | 2.95ms | 422 ops/s |
| 25 groups | 2.19ms | 2.50ms | 445 ops/s |

**Observation**: At this scale, p95 stayed in a tight 2.50-2.95ms band.

### Multiple Aggregations Performance 🌟

**Test Setup**: 30 regions, 30 sales/region

**Single Aggregations** (2 separate queries: MIN then MAX):
- Latency (p50): 2.19ms
- Latency (p95): **2.44ms**
- Throughput: 364 ops/s

**Combined Aggregations** (1 query: MIN + MAX together):
- Latency (p50): 2.08ms
- Latency (p95): **2.33ms**
- Throughput: 470 ops/s

**💡 Key Finding**:
- **Latency reduction: 4.67%** (2.44ms → 2.33ms)
- **Throughput improvement: 29.05%** (364 ops/s → 470 ops/s)

**Recommendation**: Combine multiple aggregations in a single query to eliminate transaction overhead.

*Benchmarks run with Swift Testing `PerformanceBenchmarks` on Apple Silicon Mac and local Docker FoundationDB cluster.*

## References

- [FDB Record Layer Aggregate Indexes](https://github.com/FoundationDB/fdb-record-layer) - Reference implementation
- [Materialized Aggregates](https://en.wikipedia.org/wiki/Aggregate_function#Incremental_updates) - Database concept
- [HyperLogLog++](https://research.google/pubs/pub40671/) - Heule, Nunkesser, Hall (Google, 2013) - Cardinality estimation algorithm
- [t-digest](https://github.com/tdunning/t-digest) - Dunning & Ertl (2019) - Streaming quantile estimation
