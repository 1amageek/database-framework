# BitmapIndex

Efficient set membership queries on low-cardinality fields using Roaring Bitmaps.

## Overview

BitmapIndex provides fast AND/OR/NOT operations across categorical fields by maintaining compressed bitmaps for each distinct value. Instead of scanning records, bitmap operations directly compute intersections and unions in memory, making complex filter combinations extremely efficient.

**Algorithm**:
- **Roaring Bitmaps**: Hybrid compressed bitmap using array, bitmap, and run-length containers
- **Container Selection**: Automatic switching between sparse (array) and dense (bitmap) storage
- **Reference**: Lemire et al., "Roaring Bitmaps: Implementation of an Optimized Software Library", 2016

**Storage Layout**:
```
// Bitmap data per field value
Key: [indexSubspace]["data"][fieldValue]
Value: Serialized RoaringBitmap

// Sequential ID assignment
Key: [indexSubspace]["meta"]["nextId"]
Value: Int64 (next sequential ID)

// ID-to-PrimaryKey mapping
Key: [indexSubspace]["ids"][sequentialId]
Value: primaryKey bytes

// PrimaryKey-to-ID mapping (reverse lookup)
Key: [indexSubspace]["pks"][primaryKey]
Value: sequentialId (Int64)

Example:
  [I]/User_bitmap_status/["data"]/["active"] = RoaringBitmap{0,1,3,5,...}
  [I]/User_bitmap_status/["data"]/["inactive"] = RoaringBitmap{2,4,6,...}
  [I]/User_bitmap_status/["ids"]/[0] = "user-abc"
  [I]/User_bitmap_status/["pks"]/["user-abc"] = 0
```

**Roaring Bitmap Structure**:
```
┌─────────────────────────────────────────────────────────────────┐
│                     Roaring Bitmap                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  32-bit value split into:                                        │
│    High 16 bits → Container index (up to 65536 containers)      │
│    Low 16 bits  → Value within container                        │
│                                                                  │
│  Container Types:                                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Array Container (sparse, <4096 values)                   │   │
│  │   - Sorted array of UInt16 values                        │   │
│  │   - Memory: 2 bytes per value                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Bitmap Container (dense, ≥4096 values)                   │   │
│  │   - 1024 × 64-bit words = 65536 bits                     │   │
│  │   - Memory: 8KB fixed                                    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Run Container (consecutive ranges)                       │   │
│  │   - Array of (start, length) pairs                       │   │
│  │   - Memory: 4 bytes per run                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Use Cases

### 1. User Status Filtering

**Scenario**: Filter users by status with fast cardinality counting.

```swift
@Persistable
struct User {
    var id: String = ULID().ulidString
    var name: String = ""
    var status: String = "active"  // active, inactive, pending, banned
    var department: String = ""

    // Bitmap index on status
    #Index<User>(type: BitmapIndexKind(field: \.status))
}

// Find all active users
let activeUsers = try await context.bitmap(User.self)
    .field(\.status)
    .equals("active")
    .execute()

// Count active users (O(1) - no item fetch)
let activeCount = try await context.bitmap(User.self)
    .field(\.status)
    .equals("active")
    .count()

// Find users with status "active" OR "pending"
let users = try await context.bitmap(User.self)
    .field(\.status)
    .in(["active", "pending"])
    .execute()
```

**Performance**: O(1) for count, O(n) for fetching n matching items.

### 2. Product Catalog Filtering

**Scenario**: E-commerce product filtering with multiple attributes.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var name: String = ""
    var category: String = ""
    var brand: String = ""
    var inStock: Bool = true
    var color: String = ""

    // Bitmap indexes for filterable attributes
    #Index<Product>(type: BitmapIndexKind(field: \.category))
    #Index<Product>(type: BitmapIndexKind(field: \.brand))
    #Index<Product>(type: BitmapIndexKind(field: \.inStock))
    #Index<Product>(type: BitmapIndexKind(field: \.color))
}

// Find electronics products
let electronics = try await context.bitmap(Product.self)
    .field(\.category)
    .equals("electronics")
    .execute()

// Get bitmap for advanced operations
let electronicsBitmap = try await context.bitmap(Product.self)
    .field(\.category)
    .equals("electronics")
    .getBitmap()

let sonyBitmap = try await context.bitmap(Product.self)
    .field(\.brand)
    .equals("Sony")
    .getBitmap()

// AND operation: Electronics AND Sony
let intersection = electronicsBitmap && sonyBitmap
print("Sony electronics: \(intersection.cardinality)")

// OR operation: Red OR Blue products
let redBitmap = ...
let blueBitmap = ...
let union = redBitmap || blueBitmap

// NOT operation: Non-Sony electronics
let nonSony = electronicsBitmap - sonyBitmap
```

### 3. Permission and Role Filtering

**Scenario**: Fast permission checks using bitmap operations.

```swift
@Persistable
struct UserRole {
    var id: String = ULID().ulidString
    var userId: String = ""
    var role: String = ""  // admin, editor, viewer

    #Index<UserRole>(type: BitmapIndexKind(field: \.role))
}

// Find all admins
let adminCount = try await context.bitmap(UserRole.self)
    .field(\.role)
    .equals("admin")
    .count()

// Find users with admin OR editor role
let privilegedUsers = try await context.bitmap(UserRole.self)
    .field(\.role)
    .in(["admin", "editor"])
    .execute()
```

### 4. Fusion Query with Bitmap Filter

**Scenario**: Combine bitmap filtering with vector search.

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var title: String = ""
    var status: String = "published"
    var embedding: [Float] = []

    #Index<Document>(type: BitmapIndexKind(field: \.status))
    #Index<Document>(type: HNSWIndexKind(
        field: \.embedding, dimensions: 384
    ))
}

// Vector search filtered to published documents only
let results = try await context.fuse(Document.self) {
    // Filter: only published documents
    Bitmap(\.status, equals: "published")

    // Then: vector similarity search
    Similar(\.embedding, dimensions: 384).nearest(to: queryVector, k: 100)
}
.execute()
```

**Performance**: Bitmap filter reduces candidate set before expensive vector operations.

### 5. Boolean Field Optimization

**Scenario**: Efficient filtering on boolean fields.

```swift
@Persistable
struct Task {
    var id: String = ULID().ulidString
    var title: String = ""
    var completed: Bool = false
    var archived: Bool = false

    #Index<Task>(type: BitmapIndexKind(field: \.completed))
    #Index<Task>(type: BitmapIndexKind(field: \.archived))
}

// Find incomplete, non-archived tasks
let incompleteBitmap = try await context.bitmap(Task.self)
    .field(\.completed)
    .equals(false)
    .getBitmap()

let nonArchivedBitmap = try await context.bitmap(Task.self)
    .field(\.archived)
    .equals(false)
    .getBitmap()

let activeTasks = incompleteBitmap && nonArchivedBitmap
print("Active tasks: \(activeTasks.cardinality)")
```

## Design Patterns

### Sequential ID Assignment

Roaring bitmaps use 32-bit integers. Since primary keys can be any type, we assign sequential IDs:

```
┌─────────────────────────────────────────────────────────────────┐
│                 Sequential ID Assignment                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Insert "user-abc":                                              │
│    1. Check pk→id mapping: not found                            │
│    2. Get nextId counter: 0                                      │
│    3. Assign seqId = 0                                          │
│    4. Store mappings:                                           │
│       - ["pks"]["user-abc"] = 0                                 │
│       - ["ids"][0] = "user-abc"                                 │
│    5. Increment nextId: 1                                       │
│    6. Add 0 to bitmap for field value                           │
│                                                                  │
│  Insert "user-def":                                              │
│    1. Get nextId: 1                                             │
│    2. Assign seqId = 1                                          │
│    3. Store mappings...                                         │
│    4. Add 1 to bitmap                                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Container Type Selection

The threshold of 4096 values optimizes memory:

| Values in Container | Container Type | Memory |
|---------------------|----------------|--------|
| < 4096 | Array | 2 bytes × n |
| ≥ 4096 | Bitmap | 8KB fixed |
| Consecutive runs | Run | 4 bytes × runs |

**Crossover Point**: At 4096 values, array uses 8KB (same as bitmap).

### Query Selection Guide

```
┌─────────────────────────────────────────────────────────────────┐
│              When to use BitmapIndex?                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Low cardinality field (< 1000 distinct values)?                │
│  Status, category, role, type, color?                           │
│  Boolean fields?                                                 │
│     └── Use BitmapIndex ✓                                       │
│                                                                  │
│  High cardinality field (> 10000 distinct values)?              │
│  Email, userId, timestamp?                                       │
│     └── Use ScalarIndex instead                                 │
│                                                                  │
│  Need complex AND/OR/NOT operations?                            │
│  Multi-attribute filtering?                                      │
│     └── Use BitmapIndex ✓                                       │
│                                                                  │
│  Need range queries (>, <, BETWEEN)?                            │
│     └── Use ScalarIndex instead                                 │
│                                                                  │
│  Need cardinality counting without fetch?                       │
│     └── Use BitmapIndex ✓                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Sparse Index Behavior

BitmapIndex supports sparse indexing - nil values are not indexed:

```swift
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var title: String = ""
    var category: String? = nil  // Optional field

    #Index<Article>(type: BitmapIndexKind(field: \.category))
}

// Articles without category are NOT in any bitmap
// Only articles with non-nil category are indexed
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Roaring Bitmap (Array container) | ✅ Complete | Sorted UInt16 array |
| Roaring Bitmap (Bitmap container) | ✅ Complete | 1024 × 64-bit words |
| Roaring Bitmap (Run container) | ✅ Complete | Run-length encoding |
| AND operation | ✅ Complete | Intersection |
| OR operation | ✅ Complete | Union |
| ANDNOT operation | ✅ Complete | Difference |
| Cardinality counting | ✅ Complete | O(containers) |
| Sequential ID assignment | ✅ Complete | Bidirectional mapping |
| Sparse index (nil values) | ✅ Complete | Nil values excluded |
| Query Builder API | ✅ Complete | equals, in, limit |
| Fusion integration | ✅ Complete | Filter query |
| XOR operation | ❌ Not implemented | Symmetric difference |
| Negation (NOT) | ⚠️ Partial | Via ANDNOT with universe |
| Parallel bitmap operations | ❌ Not implemented | Single-threaded |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Add value | O(log n) array, O(1) bitmap | Container-dependent |
| Remove value | O(n) array, O(1) bitmap | Container-dependent |
| Contains | O(log n) array, O(1) bitmap | Container-dependent |
| AND | O(min(n,m)) | Iterate smaller bitmap |
| OR | O(n + m) | Merge containers |
| Cardinality | O(containers) | Cached per container |
| Serialize | O(n) | JSON encoding |
| Deserialize | O(n) | JSON decoding |
| Get count | O(1) | Direct cardinality |
| Get items | O(n) | Fetch n primary keys |

### Storage Overhead

| Component | Storage per Value |
|-----------|-------------------|
| Sequential ID mapping | ~16 bytes (pk→id + id→pk) |
| Array container | 2 bytes per value |
| Bitmap container | 8KB fixed |
| Run container | 4 bytes per run |

**Per-Index Storage**:
- Overhead: ~24 bytes per record (ID mappings + nextId)
- Bitmap data: Depends on cardinality and distribution

### FDB Considerations

- **Value Size**: Roaring bitmaps can grow large; FDB has 100KB value limit
- **Serialization**: JSON encoding (can be optimized to binary)
- **Transaction Size**: Large bitmap updates may approach 10MB limit
- **Sequential IDs**: 32-bit limit (~4 billion records per index)

## Benchmark Results

Run with: `swift test --filter "SerializationBenchmark"`

### JSON Serialization Performance (Current Implementation)

**Test Configuration**:
- Warmup: 5 iterations
- Measurement: 100 iterations
- Throughput test: 5.0 seconds
- Test bitmaps: Sparse (100 values), Dense (8000 values), Mixed (1500 values)

| Metric | Baseline | Optimized | Notes |
|--------|----------|-----------|-------|
| **Latency (p50)** | 0.06ms | 0.06ms | JSON encoding |
| **Latency (p95)** | 0.07ms | 0.07ms | Consistent performance |
| **Latency (p99)** | 0.10ms | 0.07ms | Low variance |
| **Throughput** | 15,541 ops/s | 15,566 ops/s | 3 bitmap serializations |

**Serialization Sizes** (JSON Format):
- Sparse bitmap (100 values): **532 bytes**
- Dense bitmap (8000 values): **4,468 bytes**
- Mixed bitmap (1500 values): **6,434 bytes**

**Expected Improvements with Binary Format**:
- 50-70% faster serialization/deserialization
- 30-50% smaller storage footprint
- Reference: [Roaring Bitmap Format Spec](https://github.com/RoaringBitmap/RoaringFormatSpec)

### Round-trip Performance (Serialize + Deserialize)

| Data Size | Latency (p50) | Latency (p95) | Throughput |
|-----------|---------------|---------------|------------|
| 100 values | 0.57ms | 0.61ms | 1,702 ops/s |
| 500 values | 13.03ms | 13.48ms | 76 ops/s |
| 1,000 values | 50.82ms | 51.99ms | 19 ops/s |
| 5,000 values | 841.59ms | 864.15ms | 1 ops/s |
| 10,000 values | 841.30ms | 850.26ms | 1 ops/s |

### Bitmap Operations (In-Memory)

**Test Setup**: 5000 ∩ 3333 values (10,000 value range)

| Operation | Latency (p50) | Latency (p95) | Throughput |
|-----------|---------------|---------------|------------|
| **AND** | 0.20ms | 0.23ms | 4,884 ops/s |
| **OR** | 0.20ms | 0.23ms | 4,757 ops/s |

**Memory Efficiency**:
- Container auto-selection (Array vs Bitmap vs Run)
- Sparse bitmaps: 2 bytes per value
- Dense bitmaps: 8KB fixed per 65,536 value range

*Benchmarks run on Apple Silicon Mac with local FoundationDB cluster.*

## References

- [Roaring Bitmaps Paper](https://arxiv.org/abs/1603.06549) - Lemire et al., 2016
- [Roaring Bitmap Format](https://github.com/RoaringBitmap/RoaringFormatSpec) - Specification
- [CRoaring](https://github.com/RoaringBitmap/CRoaring) - Reference C implementation
- [Pilosa](https://www.pilosa.com/) - Distributed bitmap index database
