# ScalarIndex

Standard B-tree-like index for equality, range queries, and ordering.

## Overview

ScalarIndex is the most fundamental index type, providing efficient lookups and range scans on one or more fields. It's analogous to a traditional database B-tree index.

**Key Structure**:
```
[indexSubspace][field1Value][field2Value]...[primaryKey] = ''
```

## Use Cases

### 1. Single Field Equality Lookup

**Scenario**: Find user by email address.

```swift
@Persistable
struct User {
    var id: String = ULID().ulidString
    var email: String = ""

    #Index<User>(ScalarIndexKind(fields: [\.email]))
}

// Query: O(log N) lookup
let user = try await context.fetch(User.self)
    .where(\.email == "alice@example.com")
    .first()
```

**Performance**: O(log N) - Single key lookup.

### 2. Range Query

**Scenario**: Find products within price range.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var price: Int64 = 0

    #Index<Product>(ScalarIndexKind(fields: [\.price]))
}

// Query: O(log N + K) where K = result count
let products = try await context.fetch(Product.self)
    .where(\.price >= 1000)
    .where(\.price <= 5000)
    .execute()
```

**Performance**: O(log N + K) - Seek + scan K results.

### 3. Composite Index (Multi-Field)

**Scenario**: Find orders by customer and status, sorted by date.

```swift
@Persistable
struct Order {
    var id: String = ULID().ulidString
    var customerId: String = ""
    var status: String = ""
    var createdAt: Date = Date()

    // Composite index: customer + status + date
    #Index<Order>(ScalarIndexKind(fields: [\.customerId, \.status, \.createdAt]))
}

// Query: Efficient prefix scan
let orders = try await context.fetch(Order.self)
    .where(\.customerId == "C001")
    .where(\.status == "pending")
    .orderBy(\.createdAt, .descending)
    .execute()
```

**Performance**: O(log N + K) - Uses leftmost prefix efficiently.

### 4. Sorting / Pagination

**Scenario**: Paginated list sorted by creation date.

```swift
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var createdAt: Date = Date()

    #Index<Article>(ScalarIndexKind(fields: [\.createdAt]))
}

// Cursor-based pagination
let articles = try await context.fetch(Article.self)
    .orderBy(\.createdAt, .descending)
    .after(cursor)
    .limit(20)
    .execute()
```

**Performance**: O(log N + K) - Efficient cursor seek + scan.

### 5. Foreign Key Reverse Lookup (To-Many)

**Scenario**: Find all orders for a customer.

```swift
@Persistable
struct Order {
    var id: String = ULID().ulidString

    @Relationship(Customer.self)
    var customerId: String? = nil

    // Auto-generated index on FK field
}

// Reverse lookup
let orders = try await context.related(customer, \.orders)
```

**Performance**: O(log N + K) - Range scan on FK index.

## Design Patterns

### Composite Index Field Order

Order fields by:
1. **Equality conditions** (highest selectivity first)
2. **Range conditions**
3. **Sort fields**

```swift
// Good: equality → range → sort
#Index<Order>(ScalarIndexKind(fields: [\.customerId, \.status, \.createdAt]))

// Query matches index order
.where(\.customerId == "C001")  // Equality
.where(\.status == "pending")   // Equality
.orderBy(\.createdAt)           // Sort
```

### Sparse Index (Optional Fields)

ScalarIndex automatically handles `nil` values with sparse index behavior:

```swift
@Persistable
struct User {
    var id: String = ULID().ulidString
    var phoneNumber: String? = nil  // Optional

    #Index<User>(ScalarIndexKind(fields: [\.phoneNumber]))
}

// nil values are NOT indexed (sparse index)
// Only non-nil values appear in the index
```

### Array Field Index

For To-Many relationships, ScalarIndex creates one entry per array element:

```swift
@Persistable
struct Customer {
    var id: String = ULID().ulidString

    @Relationship(Order.self)
    var orderIDs: [String] = []

    // Creates index entry for each orderID
}

// Find customers who have order "O001"
// Uses array element index
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Single field index | ✅ Complete | |
| Composite index | ✅ Complete | Up to N fields |
| Range queries | ✅ Complete | Via FDB range scan |
| Prefix queries | ✅ Complete | Leftmost prefix |
| Sparse index (nil) | ✅ Complete | nil values not indexed |
| Array field index | ✅ Complete | One entry per element |
| Covering index | ❌ Not implemented | Store extra fields in value |
| Unique constraint | ⚠️ Partial | Via FDB conflict detection |
| Index-only scan | ❌ Not implemented | Avoid primary lookup |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Point lookup (equality) | O(log N) | Single key read |
| Range scan | O(log N + K) | K = result count |
| Insert | O(log N × M) | M = index count |
| Update | O(log N × M × 2) | Delete old + insert new |
| Delete | O(log N × M) | M = index count |

### FDB Considerations

- **Key size limit**: 10KB max per key
- **Transaction limit**: 5 second default, 10MB writes
- **Batch size**: ~100-1000 items per transaction recommended

## Benchmark Results

Run with: `swift test --filter "CoveringIndexBenchmark"`

### Fetch All Users (300 records)

**Test Configuration**:
- Warmup: 3 iterations
- Measurement: 30 iterations
- Throughput test: 3.0 seconds

| Metric | Baseline | Optimized | Notes |
|--------|----------|-----------|-------|
| **Latency (p50)** | 3.35ms | 3.40ms | Full record fetch |
| **Latency (p95)** | 4.00ms | 3.76ms | Consistent performance |
| **Latency (p99)** | 4.40ms | 4.17ms | Low variance |
| **Throughput** | 279 ops/s | 284 ops/s | 300 record scan |

**Expected Improvements with Covering Index**:
- 50-80% latency reduction (eliminates primary key lookup)
- Single index scan vs index scan + data fetch

### Index Scan Scalability

| Record Count | Latency (p50) | Latency (p95) | Throughput |
|--------------|---------------|---------------|------------|
| 10 records | 6.93ms | 7.24ms | 142 ops/s |
| 50 records | 6.95ms | 7.41ms | 140 ops/s |
| 100 records | 7.22ms | 7.72ms | 138 ops/s |
| 200 records | 7.47ms | 7.81ms | 134 ops/s |

**Scalability**: Nearly linear performance up to 200 records.

### Batch Fetch Performance (6 × 50 records)

| Metric | Baseline | Optimized | Notes |
|--------|----------|-----------|-------|
| **Latency (p50)** | 55.01ms | 54.53ms | Sequential batches |
| **Latency (p95)** | 56.31ms | 56.10ms | Transaction overhead |
| **Throughput** | 17 ops/s | 18 ops/s | 300 total records |

**Future Optimization**: Batch point queries to reduce transaction overhead.

*Benchmarks run on Apple Silicon Mac with local FoundationDB cluster.*

## References

- [FoundationDB Data Modeling](https://apple.github.io/foundationdb/data-modeling.html)
- [FDB Record Layer Indexes](https://foundationdb.github.io/fdb-record-layer/Indexes.html)
- [B-tree Index Design](https://use-the-index-luke.com/)
