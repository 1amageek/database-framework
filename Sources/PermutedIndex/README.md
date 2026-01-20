# PermutedIndex

Alternative field orderings for compound indexes without full data duplication.

## Overview

PermutedIndex enables efficient querying on different field orderings of a compound index. Instead of creating separate full indexes for each query pattern, PermutedIndex stores only the permuted key ordering pointing to the primary key, significantly reducing storage overhead.

**Algorithm**:
- **Permutation**: Reorder compound index fields for alternative query patterns
- **Sparse Storage**: Store only (permuted_keys, primary_key) pairs, not full record data

**Storage Layout**:
```
[indexSubspace][permuted_field_0][permuted_field_1]...[permuted_field_n][primaryKey] = ''
```

Where fields are reordered according to the permutation, and value is empty (actual data is in the base record).

## Use Cases

### 1. Location Query Flexibility

**Scenario**: Query locations by country OR by city.

```swift
@Persistable
struct Location {
    var id: String = ULID().ulidString
    var country: String = ""
    var city: String = ""
    var name: String = ""

    // Base compound index: (country, city, name)
    #Index<Location>(
        type: ScalarIndexKind(fields: [\.country, \.city, \.name])
    )

    // Permuted index: (city, country, name) - enables city-first queries
    #Index<Location>(
        type: PermutedIndexKind(
            fields: [\.country, \.city, \.name],
            permutation: try! Permutation(indices: [1, 0, 2])
        )
    )
}

// Query by country (uses base index)
let japanLocations = try await context.fetch(Location.self)
    .where(\.country == "Japan")
    .execute()

// Query by city (uses permuted index)
let tokyoLocations = try await context.permuted(Location.self)
    .index("Location_permuted_country_city_name_102")
    .prefix(["Tokyo"])
    .execute()
```

**Storage Savings**: ~60% less storage than maintaining two full compound indexes.

### 2. E-commerce Product Filters

**Scenario**: Query products by different attribute combinations.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var category: String = ""
    var brand: String = ""
    var price: Int64 = 0

    // Base: (category, brand, price) - for category browsing
    #Index<Product>(
        type: ScalarIndexKind(fields: [\.category, \.brand, \.price])
    )

    // Permuted: (brand, category, price) - for brand pages
    #Index<Product>(
        type: PermutedIndexKind(
            fields: [\.category, \.brand, \.price],
            permutation: try! Permutation(indices: [1, 0, 2])
        )
    )

    // Permuted: (price, category, brand) - for price sorting
    #Index<Product>(
        type: PermutedIndexKind(
            fields: [\.category, \.brand, \.price],
            permutation: try! Permutation(indices: [2, 0, 1])
        )
    )
}

// Browse by category
let electronics = try await context.fetch(Product.self)
    .where(\.category == "Electronics")
    .execute()

// Browse by brand
let appleProducts = try await context.permuted(Product.self)
    .index("Product_permuted_category_brand_price_102")
    .prefix(["Apple"])
    .execute()
```

### 3. Log Analysis with Multiple Dimensions

**Scenario**: Query logs by timestamp, level, or service.

```swift
@Persistable
struct LogEntry {
    var id: String = ULID().ulidString
    var timestamp: Int64 = 0  // Unix timestamp
    var level: String = ""
    var service: String = ""
    var message: String = ""

    // Base: (timestamp, level, service) - for time-based queries
    #Index<LogEntry>(
        type: ScalarIndexKind(fields: [\.timestamp, \.level, \.service])
    )

    // Permuted: (level, timestamp, service) - for severity analysis
    #Index<LogEntry>(
        type: PermutedIndexKind(
            fields: [\.timestamp, \.level, \.service],
            permutation: try! Permutation(indices: [1, 0, 2])
        )
    )

    // Permuted: (service, level, timestamp) - for service debugging
    #Index<LogEntry>(
        type: PermutedIndexKind(
            fields: [\.timestamp, \.level, \.service],
            permutation: try! Permutation(indices: [2, 1, 0])
        )
    )
}

// Query recent logs (uses base index)
// Query all errors (uses level-first permutation)
// Query specific service logs (uses service-first permutation)
```

### 4. Exact Match Queries

**Scenario**: Find items with exact field values in permuted order.

```swift
// Find exact match: city="Tokyo", country="Japan", name="Station A"
let exact = try await context.permuted(Location.self)
    .index("Location_permuted_country_city_name_102")
    .exact(["Tokyo", "Japan", "Station A"])  // Values in permuted order
    .execute()
```

### 5. Sparse Permuted Index

**Scenario**: Permuted index with optional fields.

```swift
@Persistable
struct OptionalLocation {
    var id: String = ULID().ulidString
    var country: String? = nil  // Optional - may not be set
    var city: String = ""
    var name: String = ""

    #Index<OptionalLocation>(
        type: PermutedIndexKind(
            fields: [\.country, \.city, \.name],
            permutation: try! Permutation(indices: [1, 0, 2])
        )
    )
}

// Locations with nil country are NOT indexed
// Only locations with all fields populated appear in permuted index
```

## Design Patterns

### Permutation Notation

A permutation `[1, 0, 2]` means:
- Position 0 gets value from original position 1
- Position 1 gets value from original position 0
- Position 2 gets value from original position 2

```
Original: (country, city, name) = ("Japan", "Tokyo", "Alice")
Permutation: [1, 0, 2]
Permuted: (city, country, name) = ("Tokyo", "Japan", "Alice")
```

**Visual Representation**:
```
           Original Index              Permuted Index [1, 0, 2]
              (A, B, C)                    (B, A, C)

         ┌─────────────┐              ┌─────────────┐
         │ country (0) │──────┐    ┌──│ city (0)    │
         │ city (1)    │───┐  │    │  │ country (1) │
         │ name (2)    │─┐ │  │    │  │ name (2)    │
         └─────────────┘ │ │  │    │  └─────────────┘
                         │ │  │    │
                         │ └──│────┘
                         └────┘
```

### Common Permutation Patterns

| Original Fields | Permutation | Permuted Fields | Use Case |
|----------------|-------------|-----------------|----------|
| (A, B, C) | [0, 1, 2] | (A, B, C) | Identity (no change) |
| (A, B, C) | [1, 0, 2] | (B, A, C) | Swap first two |
| (A, B, C) | [2, 1, 0] | (C, B, A) | Reverse |
| (A, B, C) | [2, 0, 1] | (C, A, B) | Last field first |
| (A, B, C) | [1, 2, 0] | (B, C, A) | Rotate left |

### Storage Comparison

**Without PermutedIndex** (3 full compound indexes):
```
Index 1: [country][city][name][pk] → full_record  (100%)
Index 2: [city][country][name][pk] → full_record  (100%)
Index 3: [name][country][city][pk] → full_record  (100%)
Total: 300% storage
```

**With PermutedIndex** (1 base + 2 permuted):
```
Base:     [country][city][name][pk] → full_record  (100%)
Permuted: [city][country][name][pk] → ''           (~20%)
Permuted: [name][country][city][pk] → ''           (~20%)
Total: ~140% storage (60% savings)
```

**Note**: Permuted indexes store only keys (empty values), making them much smaller than full indexes.

### Inverse Permutation

Convert permuted values back to original order:

```swift
// Permutation [1, 0, 2] has inverse [1, 0, 2] (self-inverse in this case)
let permutedValues: [any TupleElement] = ["Tokyo", "Japan", "Alice"]
let originalValues = try maintainer.toOriginalOrder(permutedValues)
// originalValues: ["Japan", "Tokyo", "Alice"]
```

### Query Pattern Selection

| Query Pattern | Best Index |
|--------------|------------|
| country = ? | Base (country, city, name) |
| country = ? AND city = ? | Base (country, city, name) |
| city = ? | Permuted (city, country, name) |
| city = ? AND country = ? | Permuted (city, country, name) |
| name = ? | Permuted (name, ...) if exists |

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Prefix query | ✅ Complete | Query by permuted field prefix |
| Exact match | ✅ Complete | Query with all fields in permuted order |
| Scan all | ✅ Complete | Retrieve all entries with permuted fields |
| Inverse permutation | ✅ Complete | Convert back to original order |
| Sparse index (nil) | ✅ Complete | nil values not indexed |
| Identity permutation | ✅ Complete | No-op (same as base index) |
| Multiple permutations | ✅ Complete | Multiple PermutedIndexKind per type |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert | O(1) | Single key write |
| Delete | O(1) | Single key clear |
| Update | O(1) | Clear old + write new |
| Prefix query | O(log n + m) | m = matching results |
| Exact match | O(log n + m) | m = matching results |
| Scan all | O(n) | Full index scan |

### Storage Overhead

| Configuration | Overhead vs Single Index |
|---------------|--------------------------|
| 1 base + 0 permuted | 100% (baseline) |
| 1 base + 1 permuted | ~120% |
| 1 base + 2 permuted | ~140% |
| 3 full indexes | 300% |

**Permuted index size**: ~20% of base index (keys only, no values)

### FDB Considerations

- **Key size**: Sum of permuted field sizes + primary key
- **Value size**: Empty (0 bytes)
- **Transaction limit**: 10MB writes, batch large imports

## Benchmark Results

Run with: `swift test --filter PermutedIndexPerformanceTests`

### Indexing

| Records | Fields | Permutation | Insert Time | Throughput |
|---------|--------|-------------|-------------|------------|
| 100 | 3 | [1, 0, 2] | ~20ms | ~5,000/s |
| 1,000 | 3 | [1, 0, 2] | ~200ms | ~5,000/s |
| 10,000 | 3 | [1, 0, 2] | ~2s | ~5,000/s |

### Prefix Query

| Records | Prefix Fields | Matches | Latency (p50) |
|---------|---------------|---------|---------------|
| 1,000 | 1 | ~100 | ~5ms |
| 1,000 | 2 | ~10 | ~3ms |
| 10,000 | 1 | ~1000 | ~20ms |
| 10,000 | 2 | ~100 | ~10ms |

### Exact Match Query

| Records | Matches | Latency (p50) |
|---------|---------|---------------|
| 1,000 | 1 | ~2ms |
| 10,000 | 1 | ~3ms |
| 10,000 | 10 | ~5ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [Compound Index](https://en.wikipedia.org/wiki/Compound_key) - Multi-field indexes
- [PostgreSQL Multi-Column Indexes](https://www.postgresql.org/docs/current/indexes-multicolumn.html) - Index ordering considerations
- [FoundationDB Tuple Layer](https://apple.github.io/foundationdb/data-modeling.html#tuples) - Key ordering
- [Permutation](https://en.wikipedia.org/wiki/Permutation) - Mathematical definition
