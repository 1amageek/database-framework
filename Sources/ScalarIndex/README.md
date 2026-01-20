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

Run with: `swift test --filter ScalarIndexPerformanceTests`

| Scenario | Items | Operation | Throughput | Latency (p50) |
|----------|-------|-----------|------------|---------------|
| Point lookup | 10,000 | Read | ~50,000/s | <1ms |
| Range scan (100 items) | 10,000 | Read | ~10,000/s | <5ms |
| Bulk insert | 1,000 | Write | ~5,000/s | <10ms |
| Composite 3-field | 10,000 | Read | ~40,000/s | <2ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [FoundationDB Data Modeling](https://apple.github.io/foundationdb/data-modeling.html)
- [FDB Record Layer Indexes](https://foundationdb.github.io/fdb-record-layer/Indexes.html)
- [B-tree Index Design](https://use-the-index-luke.com/)
