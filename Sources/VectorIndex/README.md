# VectorIndex

High-dimensional vector similarity search for embeddings (RAG, semantic search).

## Overview

VectorIndex provides K-nearest neighbor (KNN) search for vector embeddings. It supports multiple algorithms (Flat scan, HNSW) and distance metrics (cosine, euclidean, dot product).

**Algorithms**:
- **Flat Scan**: Exact brute-force search, O(n)
- **HNSW**: Approximate nearest neighbor, O(log n)

**Storage Layout (Flat)**:
```
[indexSubspace][primaryKey] = Tuple(Float, Float, ..., Float)
```

**Storage Layout (HNSW)**:
```
[indexSubspace]/vectors/[label] = Tuple(Float...)    // Vector storage
[indexSubspace]/labels/[primaryKey] = UInt64         // PK to label mapping
[indexSubspace]/pks/[label] = primaryKey             // Label to PK mapping
[indexSubspace]/graph = Data                         // Serialized HNSW graph
```

## Use Cases

### 1. Semantic Search (RAG)

**Scenario**: Find documents similar to a query embedding for RAG pipeline.

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var content: String = ""
    var embedding: [Float] = []  // 384-dim from sentence-transformers

    #Index<Document>(
        type: VectorIndexKind(
            embedding: \.embedding,
            dimensions: 384,
            metric: .cosine
        )
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
    var embedding: [Float] = []  // 768-dim from BERT

    #Index<Product>(
        type: VectorIndexKind(
            embedding: \.embedding,
            dimensions: 768,
            metric: .cosine
        )
    )
}

// Find similar products
let currentProduct = try await context.model(for: productId, as: Product.self)
let similar = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 768)
    .query(currentProduct.embedding, k: 20)
    .execute()
```

### 3. Filtered Vector Search (ACORN)

**Scenario**: Find similar products within a category using ACORN algorithm.

```swift
// Filtered search with ACORN
let results = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 768)
    .query(queryEmbedding, k: 10)
    .filter { product in
        product.category == "electronics" && product.price < 1000
    }
    .acorn(expansionFactor: 3)
    .execute()
```

**Reference**: Patel et al., "ACORN: Performant and Predicate-Agnostic Search Over Vector Embeddings and Structured Data", SIGMOD 2024

### 4. Image Similarity

**Scenario**: Find visually similar images.

```swift
@Persistable
struct Image {
    var id: String = ULID().ulidString
    var url: String = ""
    var embedding: [Float] = []  // 512-dim from CLIP

    #Index<Image>(
        type: VectorIndexKind(
            embedding: \.embedding,
            dimensions: 512,
            metric: .cosine
        )
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
| > 1M | HNSW + Sharding | ~95-99% | O(log n) |

**Configuration via IndexConfiguration**:
```swift
// At container initialization
let container = try await FDBContainer(
    for: schema,
    indexConfigurations: [
        "Product_vector_embedding": [
            HNSWConfiguration(m: 16, efConstruction: 200, efSearch: 50)
        ]
    ]
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
    var embedding: [Float]? = nil  // Optional - not all docs have embeddings

    #Index<Document>(
        type: VectorIndexKind(
            embedding: \.embedding,
            dimensions: 384,
            metric: .cosine
        )
    )
}

// Documents with nil embeddings are NOT indexed
// Only documents with embeddings appear in search results
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Flat scan (exact) | ✅ Complete | O(n) brute force |
| HNSW (approximate) | ✅ Complete | Via swift-hnsw library |
| Cosine distance | ✅ Complete | 1 - cosine_similarity |
| Euclidean distance | ✅ Complete | L2 distance |
| Dot product | ✅ Complete | Inner product |
| ACORN filtering | ✅ Complete | Predicate-agnostic filtering |
| Sparse index (nil) | ✅ Complete | nil vectors not indexed |
| IVF (inverted file) | ❌ Not implemented | Cluster-based ANN |
| Quantization (PQ) | ❌ Not implemented | Memory compression |
| Batch query | ⚠️ Partial | Multiple queries in one call |

## Performance Characteristics

| Operation | Flat Scan | HNSW |
|-----------|-----------|------|
| Insert | O(1) | O(log n × m) |
| Delete | O(1) | O(log n × m) |
| Search (k=10) | O(n × d) | O(log n × ef × d) |
| Memory | O(n × d) | O(n × d + n × m) |
| Recall | 100% | ~95-99% |

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

Run with: `swift test --filter VectorIndexPerformanceTests`

### Flat Scan

| Vectors | Dimensions | k=10 | k=50 | Recall |
|---------|------------|------|------|--------|
| 1,000 | 384 | ~5ms | ~8ms | 100% |
| 10,000 | 384 | ~50ms | ~80ms | 100% |
| 100,000 | 384 | ~500ms | ~800ms | 100% |

### HNSW (m=16, efConstruction=200)

| Vectors | Dimensions | k=10 (ef=50) | k=10 (ef=100) | Recall |
|---------|------------|--------------|---------------|--------|
| 10,000 | 384 | ~2ms | ~4ms | ~95% |
| 100,000 | 384 | ~5ms | ~10ms | ~95% |
| 1,000,000 | 384 | ~10ms | ~20ms | ~95% |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [HNSW Paper](https://arxiv.org/abs/1603.09320) - Malkov & Yashunin, 2016
- [ACORN Paper](https://arxiv.org/abs/2403.04871) - Patel et al., SIGMOD 2024
- [swift-hnsw Library](https://github.com/1amageek/swift-hnsw)
- [Pinecone Vector DB Guide](https://www.pinecone.io/learn/vector-similarity/)
