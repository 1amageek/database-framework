# VectorIndex

High-dimensional vector similarity search for embeddings (RAG, semantic search).

## Overview

VectorIndex provides K-nearest neighbor (KNN) search for vector embeddings. It supports explicit Flat, HNSW, IVF, and PQ index layouts and the cosine, Euclidean, and dot-product metrics.

**Algorithms**:
- **Flat Scan**: Exact brute-force search, O(n)
- **HNSW**: Approximate nearest neighbor, O(log n)
- **IVF**: Approximate search over trained inverted lists
- **PQ**: Compressed approximate search using product-quantized codes

The algorithm is fixed when the container constructs the index maintainer. A
read always uses the same layout that the mutation path maintains. The default
is exact Flat search; changing algorithms is an index migration, not a
query-time heuristic.

**Storage Layout (Flat)**:
```
[indexSubspace][primaryKey] = Float32 binary payload, little-endian
```

**Storage Layout (HNSW)**:
```
[indexSubspace]/vectors/[label] = Float32 binary payload, little-endian
[indexSubspace]/labels/[primaryKey] = Tuple-encoded label
[indexSubspace]/pks/[label] = Tuple-encoded primary key
[indexSubspace]/_graphMetadata = Tuple(version, byteCount, chunkSize, chunkCount, revision)
[indexSubspace]/_graphChunks/[chunk] = SwiftHNSW versioned binary graph snapshot chunk
```

HNSW graph snapshots are cached in-process by persisted metadata revision. Write paths
load from storage and persist a new revision; read paths reuse the cached graph only
when the transaction observes the same metadata bytes. This prevents stale graph reuse
after a committed update while avoiding per-query graph deserialization.

See [Vector Storage and HNSW](../../docs/storage/vector-storage-and-hnsw.md) for the physical storage contract and validation status.

## Use Cases

### 1. Semantic Search (RAG)

**Scenario**: Find documents similar to a query embedding for RAG pipeline.

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var content: String = ""
    var embedding: Vector

    #Index(
        .vector(dimensions: 384, metric: .cosine),
        embedding: \Document.embedding,
        name: "Document_embedding"
    )
}

// Query: Find top 10 similar documents
let queryEmbedding = try await embeddingModel.encode("What is machine learning?")

let results = try await context.findSimilar(Document.self)
    .vector(\.embedding, dimensions: 384)
    .query(queryEmbedding, k: 10)
    .metric(.cosine)
    .execute()

for (document, distance) in results {
    print("\(document.content): \(distance)")
}
```

**Performance (Flat)**: O(n × d) - Linear scan all vectors.
**Performance (HNSW)**: O(log n × d) - Graph traversal.

### 2. Product Recommendations

**Scenario**: Find products similar to a viewed product.

```swift
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var name: String = ""
    var embedding: Vector

    #Index(
        .vector(dimensions: 768, metric: .cosine),
        embedding: \Product.embedding,
        name: "Product_embedding"
    )
}

// Find similar products
let currentProduct = try await context.model(for: productId, as: Product.self)
let similar = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 768)
    .query(currentProduct.embedding, k: 20)
    .execute()
```

### 3. Post-Filtered HNSW Search

**Scenario**: Find similar products within a category by oversampling HNSW
candidates and applying an application predicate in distance order.

```swift
// HNSW candidate oversampling followed by predicate evaluation
let results = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 768)
    .query(queryEmbedding, k: 10)
    .filter { product in
        product.category == "electronics" && product.price < 1000
    }
    .postFilter(expansionFactor: 3)
    .execute()
```

The predicate is evaluated after HNSW traversal. This API does not implement
ACORN predicate-subgraph traversal and does not claim ACORN recall semantics.

### 4. Image Similarity

**Scenario**: Find visually similar images.

```swift
@Persistable
struct Image {
    var id: String = ULID().ulidString
    var url: String = ""
    var embedding: Vector

    #Index(
        .vector(dimensions: 512, metric: .cosine),
        embedding: \Image.embedding,
        name: "Image_embedding"
    )
}

// Find similar images
let queryImageEmbedding = try await clipModel.encode(image)
let similar = try await context.findSimilar(Image.self)
    .vector(\.embedding, dimensions: 512)
    .query(queryImageEmbedding, k: 50)
    .execute()
```

## Design Patterns

### Algorithm Selection

| Dataset Size | Recommended Algorithm | Recall | Latency |
|-------------|----------------------|--------|---------|
| < 10K | Flat Scan | 100% (exact) | O(n) |
| 10K - 1M | HNSW | ~95-99% | O(log n) |
| > 1M | IVF or PQ after workload measurement | Approximate | Data-dependent |

**Configuration via IndexRuntimeConfiguration**:
```swift
// At container initialization
let vectorConfig = VectorIndexConfiguration<Product>(
    field: Product.fields.embedding,
    algorithm: .hnsw(.default)
)

let container = try await DBContainer.open(
    for: schema,
    configuration: configuration,
    runtimeConfiguration: runtime,
    indexConfigurations: [vectorConfig]
)
```

### HNSW Parameters Tuning

| Parameter | Description | Trade-off | Typical Range |
|-----------|-------------|-----------|---------------|
| `m` | Max connections per node | Recall ↔ Build time/memory | 16-64 |
| `efConstruction` | Build-time candidate list | Recall ↔ Build time | 100-400 |
| `efSearch` | Search-time candidate list | Recall ↔ Search time | 50-200 |

**Tuning Guide**:
```
For ~95% recall: m=16, efConstruction=200, efSearch=k*2
For ~99% recall: m=32, efConstruction=400, efSearch=k*3
```

### Sparse Index (Optional Embedding)

VectorIndex supports sparse index behavior for optional embedding fields:

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var embedding: Vector?

    #Index(
        .vector(dimensions: 384, metric: .cosine),
        embedding: \Document.embedding,
        name: "Document_embedding"
    )
}

// Documents with nil embeddings are NOT indexed
// Only documents with embeddings appear in search results
```

### Polymorphic Vector Search

Polymorphic vector search is used when several concrete models share one
logical source and one vector space. The public interface stays KeyPath-based:
callers choose the shared vector field with `\.embedding`, not with a raw field
name string.

```swift
@Polymorphable(identifier: "Entity")
@PolymorphicDirectory("entities")
@PolymorphicIndex(
    .vector(dimensions: 256, metric: .cosine),
    embedding: "embedding",
    name: "Entity_embedding"
)
protocol Entity: Polymorphable<EntityPolymorphicGroup> {
    var id: String { get }
    var embedding: Vector { get }
}

let page = try await context.findPolymorphic(Person.self)
    .vector(\.embedding, dimensions: 256)
    .query(queryEmbedding, k: 10)
    .metric(.cosine)
    .executePage()
```

The schema keeps one descriptor per concrete member type. That preserves the
concrete `PartialKeyPath<Person>` and `PartialKeyPath<Organization>` values
needed by write-side extraction, while the shared descriptor name keeps the
read path pointed at the same logical index. The vector query builder converts
the KeyPath to the canonical read parameter internally after validating it
against the model metadata.

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Flat scan (exact) | ✅ Complete | O(n) brute force |
| HNSW API | ✅ Complete | Uses swift-hnsw 1.0.1 Swift production backend |
| Cosine distance | ✅ Complete | 1 - cosine_similarity |
| Euclidean distance | ✅ Complete | L2 distance |
| Dot product | ✅ Complete | Inner product |
| HNSW post-filtering | ✅ Complete | Expanded candidate search followed by predicate evaluation |
| Sparse index (nil) | ✅ Complete | nil vectors not indexed |
| Polymorphic vector search | ✅ Complete | KeyPath public API, shared logical index |
| IVF (inverted file) | ❌ Not implemented | Cluster-based ANN |
| Quantization (PQ) | ❌ Not implemented | Memory compression |
| Batch query | ⚠️ Partial | Multiple queries in one call |

## Performance Characteristics

swift-hnsw 1.0.1 uses a Swift-only production backend. The C++ hnswlib implementation is kept inside swift-hnsw's reference benchmark package and is not part of database-framework's package graph.

| Operation | Flat Scan | Swift HNSW Backend |
|-----------|-----------|--------------------|
| Insert | O(1) | O(log n × m) |
| Delete | O(1) | Soft delete in graph |
| Search (k=10) | O(n × d) | O(log n × ef × d) average |
| Memory | O(n × d) | O(n × d + n × m) |
| Recall | 100% | Approximate, tunable by efSearch |

Where:
- n = number of vectors
- d = dimensions
- m = HNSW connections per node
- ef = exploration factor

### FDB Considerations

- **Vector storage**: ~4 bytes × dimensions per vector
- **10KB key limit**: Max ~2500 dimensions in key (use value storage)
- **Transaction limit**: 10MB writes, batch large inserts

## Benchmark Results

Run with: `xcodebuild test -scheme DatabaseCoreFocused -destination 'platform=macOS,arch=arm64' -only-testing:VectorIndexTests/VectorIndexPerformanceTests`

### Flat Scan

| Vectors | Dimensions | k=10 | k=50 | Recall |
|---------|------------|------|------|--------|
| 1,000 | 384 | ~5ms | ~8ms | 100% |
| 10,000 | 384 | ~50ms | ~80ms | 100% |
| 100,000 | 384 | ~500ms | ~800ms | 100% |

### SwiftHNSW Production Backend

The production path uses the Swift backend from `swift-hnsw`. The C++ hnswlib backend is retained only in swift-hnsw's separate reference benchmark package and is not compiled by database-framework.

| Vectors | Dimensions | k=10 (ef=50) | k=10 (ef=100) | Recall |
|---------|------------|--------------|---------------|--------|
| 10,000 | 384 | ~2ms | ~4ms | ~95% |
| 100,000 | 384 | ~5ms | ~10ms | ~95% |
| 1,000,000 | 384 | ~10ms | ~20ms | ~95% |

*Historical benchmark shape. Refresh these numbers with the current SwiftHNSW benchmark before using them for a release decision.*

## References

- [HNSW Paper](https://arxiv.org/abs/1603.09320) - Malkov & Yashunin, 2016
- [swift-hnsw Library](https://github.com/1amageek/swift-hnsw)
- [Pinecone Vector DB Guide](https://www.pinecone.io/learn/vector-similarity/)
