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
```

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

### 4. Query Builder API

**Scenario**: SQL-like aggregation queries using fluent API.

```swift
// GROUP BY region with multiple aggregates
let stats = try await context.aggregate(Sale.self)
    .groupBy(\.region)
    .count(as: "orderCount")
    .sum(\.amount, as: "totalSales")
    .avg(\.amount, as: "avgOrderValue")
    .having { $0.aggregates["orderCount"] as? Int ?? 0 > 10 }
    .execute()

for result in stats {
    print("Region: \(result.groupKey["region"]!)")
    print("  Orders: \(result.aggregates["orderCount"]!)")
    print("  Total Sales: \(result.aggregates["totalSales"]!)")
    print("  Avg Order: \(result.aggregates["avgOrderValue"]!)")
}
```

**Note**: The query builder currently computes aggregates in-memory. Future optimization will use precomputed aggregation indexes for O(1) lookups when GROUP BY matches the index structure.

### 5. Sparse Aggregation (Optional Fields)

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
| Query Builder API | ✅ Complete | In-memory computation |
| Index-backed queries | ⚠️ Partial | Query builder uses O(n) scan |
| DISTINCT aggregation | ❌ Not implemented | Planned |
| PERCENTILE aggregation | ❌ Not implemented | Would need different algorithm |

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

## References

- [FDB Record Layer Aggregate Indexes](https://github.com/FoundationDB/fdb-record-layer) - Reference implementation
- [Atomic Operations in FDB](https://apple.github.io/foundationdb/developer-guide.html#atomic-operations) - FoundationDB documentation
- [Materialized Aggregates](https://en.wikipedia.org/wiki/Aggregate_function#Incremental_updates) - Database concept
- [Fixed-Point Arithmetic](https://en.wikipedia.org/wiki/Fixed-point_arithmetic) - For floating-point storage
