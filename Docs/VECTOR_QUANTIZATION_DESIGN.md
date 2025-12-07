# Vector Quantization Design

## Overview

Vector quantization compresses high-dimensional vectors into compact codes for memory-efficient storage. This document describes the integration design following the project's FusionQuery pattern.

### Quantization Methods

| Method | Compression | Accuracy | Use Case |
|--------|-------------|----------|----------|
| **PQ** (Product Quantization) | 4-32x | High | General purpose |
| **SQ** (Scalar Quantization) | 4x | Very High | When accuracy is critical |
| **BQ** (Binary Quantization) | 32x | Medium | Extremely large datasets |

### References

- Product Quantization: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011
- Scalar Quantization: Guo et al., "Accelerating Large-Scale Inference with Anisotropic Vector Quantization", ICML 2020
- Binary Quantization: Norouzi et al., "Minimal Loss Hashing for Compact Binary Codes", ICML 2011

## Architecture

### Module Structure

```
VectorIndex/
├── QuantizationConfig.swift           # Runtime configuration (PQConfig, SQConfig, BQConfig)
├── VectorIndexConfiguration.swift     # Extended with quantization property
├── Quantization/
│   ├── VectorQuantizer.swift          # Protocol for quantization algorithms
│   ├── ProductQuantizer.swift         # PQ implementation (ADC, k-means++)
│   ├── ScalarQuantizer.swift          # SQ implementation (4-bit, 8-bit)
│   └── BinaryQuantizer.swift          # BQ implementation (Hamming distance)
└── Fusion/
    ├── Similar.swift                  # Existing FusionQuery (reference pattern)
    └── QuantizedSimilar.swift         # Quantized search FusionQuery (NEW)
```

### Design Principles

1. **Extension Pattern**: No modifications to core (`FDBContext`, `OnlineIndexer`, etc.)
2. **FusionQuery Pattern**: Follow existing `Similar.swift` pattern with `IndexQueryContext`
3. **Explicit User Control**: Training and codebook management via separate API

## API Design

### Configuration (QuantizationConfig)

```swift
// Runtime configuration at container initialization
let config = VectorIndexConfiguration<Product>(
    keyPath: \.embedding,
    algorithm: .flat,
    quantization: .pq(.default)  // Enable PQ compression
)

let container = try await FDBContainer(
    for: schema,
    indexConfigurations: [config]
)
```

### VectorQuantizer Protocol

```swift
public protocol VectorQuantizer: Sendable {
    associatedtype Code: Sendable

    var isTrained: Bool { get }
    var dimensions: Int { get }
    var codeSize: Int { get }

    // Lifecycle
    func train(vectors: [[Float]]) async throws
    func encode(_ vector: [Float]) throws -> Code
    func decode(_ code: Code) throws -> [Float]

    // Asymmetric Distance Computation (ADC)
    func prepareQuery(_ query: [Float]) throws -> PreparedQuery
    func distanceWithPrepared(_ prepared: PreparedQuery, code: Code) -> Float

    // Serialization
    func serialize() throws -> Data
    mutating func deserialize(from data: Data) throws
}
```

### QuantizedSimilar (FusionQuery)

```swift
/// Quantized vector similarity search query for Fusion
///
/// Uses pre-trained quantizer for memory-efficient search.
/// Follows the same pattern as Similar.swift.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     QuantizedSimilar(\.embedding, dimensions: 384, quantizer: pq)
///         .nearest(to: queryVector, k: 100)
/// }
/// .execute()
/// ```
public struct QuantizedSimilar<T: Persistable, Q: VectorQuantizer>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private let quantizer: Q
    private var queryVector: [Float]?
    private var k: Int = 10

    // MARK: - Initialization

    /// Create a QuantizedSimilar query for a vector field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    ///   - quantizer: Pre-trained quantizer instance
    public init(_ keyPath: KeyPath<T, [Float]>, dimensions: Int, quantizer: Q) {
        guard let context = FusionContext.current else {
            fatalError("QuantizedSimilar must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
        self.quantizer = quantizer
        self.queryContext = context
    }

    // MARK: - Configuration

    public func nearest(to vector: [Float], k: Int) -> Self {
        var copy = self
        copy.queryVector = vector
        copy.k = k
        return copy
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let vector = queryVector else { return [] }
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        // Prepare ADC distance tables (O(M*K) precomputation)
        let prepared = try quantizer.prepareQuery(vector)

        // Search using quantized codes
        // ...implementation details...
    }
}
```

### CodebookTrainer

```swift
/// Trains and manages vector quantizer codebooks
///
/// **Usage**:
/// ```swift
/// // Create quantizer
/// var pq = ProductQuantizer(config: .forDimensions(384), dimensions: 384)
///
/// // Train on sample data
/// let trainer = CodebookTrainer<Product, ProductQuantizer>(
///     keyPath: \.embedding,
///     quantizer: pq
/// )
/// try await trainer.train(sampleSize: 10000, context: ctx)
///
/// // Save codebook for persistence
/// try await trainer.saveCodebook(context: ctx)
/// ```
public struct CodebookTrainer<T: Persistable, Q: VectorQuantizer> {
    private let keyPath: KeyPath<T, [Float]>
    private var quantizer: Q

    public init(keyPath: KeyPath<T, [Float]>, quantizer: Q) {
        self.keyPath = keyPath
        self.quantizer = quantizer
    }

    /// Train quantizer on sample vectors from the database
    ///
    /// - Parameters:
    ///   - sampleSize: Number of vectors to sample for training
    ///   - context: IndexQueryContext for database access
    public mutating func train(sampleSize: Int, context: IndexQueryContext) async throws {
        // 1. Sample vectors from index
        let samples = try await sampleVectors(count: sampleSize, context: context)

        // 2. Train quantizer
        try await quantizer.train(vectors: samples)
    }

    /// Save trained codebook to database
    ///
    /// Storage key: [indexSubspace]/_codebook/[quantizerType]
    public func saveCodebook(context: IndexQueryContext) async throws {
        guard quantizer.isTrained else {
            throw QuantizerError.notTrained
        }

        let data = try quantizer.serialize()
        // Store in index metadata subspace
        // ...implementation details...
    }

    /// Load codebook from database
    public mutating func loadCodebook(context: IndexQueryContext) async throws {
        // Load from index metadata subspace
        // ...implementation details...
    }
}
```

## Storage Layout

### Codebook Storage

```
[indexSubspace]/_meta/codebook/pq    → Serialized ProductQuantizer state
[indexSubspace]/_meta/codebook/sq    → Serialized ScalarQuantizer state
[indexSubspace]/_meta/codebook/bq    → Serialized BinaryQuantizer state
```

### Quantized Vector Storage

```
# Full precision (existing)
[indexSubspace]/[primaryKey] → Tuple(Float, Float, ..., Float)

# Quantized (when quantization enabled)
[indexSubspace]/q/[primaryKey] → Tuple(UInt8, UInt8, ..., UInt8)  # PQ codes
```

## Usage Examples

### Complete Workflow

```swift
import VectorIndex

// 1. Define model with vector index
@Persistable
struct Product {
    var id: String = ULID().ulidString
    var name: String
    @Index(type: VectorIndexKind(dimensions: 384, metric: .cosine))
    var embedding: [Float]
}

// 2. Configure with quantization at container creation
let config = VectorIndexConfiguration<Product>(
    keyPath: \.embedding,
    algorithm: .flat,
    quantization: .pq(.forDimensions(384))
)

let container = try await FDBContainer(
    for: schema,
    indexConfigurations: [config]
)

// 3. Train codebook (one-time setup)
var pq = ProductQuantizer(config: .forDimensions(384), dimensions: 384)
var trainer = CodebookTrainer<Product, ProductQuantizer>(
    keyPath: \.embedding,
    quantizer: pq
)

let ctx = container.newContext()
try await trainer.train(sampleSize: 10000, context: ctx.queryContext)
try await trainer.saveCodebook(context: ctx.queryContext)

// 4. Search using quantized vectors
let queryVector: [Float] = generateEmbedding("search query")
let results = try await ctx.fuse(Product.self) {
    QuantizedSimilar(\.embedding, dimensions: 384, quantizer: pq)
        .nearest(to: queryVector, k: 10)
}
.execute()

for result in results {
    print("\(result.item.name): \(result.score)")
}
```

### Hybrid Search with Quantization

```swift
// Combine quantized vector search with text filtering
let results = try await ctx.fuse(Product.self) {
    QuantizedSimilar(\.embedding, dimensions: 384, quantizer: pq)
        .nearest(to: queryVector, k: 100)

    Search(\.description)
        .match("organic coffee")
        .scoreWeight(0.3)
}
.execute()
```

### Choosing Quantization Method

```swift
// PQ: General purpose (32x compression, high accuracy)
let pqConfig = VectorIndexConfiguration<Product>(
    keyPath: \.embedding,
    quantization: .pq(.forDimensions(384))
)

// SQ: When accuracy is critical (4x compression, very high accuracy)
let sqConfig = VectorIndexConfiguration<Product>(
    keyPath: \.embedding,
    quantization: .sq(.default)
)

// BQ: Extremely large datasets (32x compression, fast but lower accuracy)
let bqConfig = VectorIndexConfiguration<Product>(
    keyPath: \.embedding,
    quantization: .bq(.highRecall)  // rescoringFactor: 10
)
```

## Technical Details

### Product Quantization (PQ)

**Constraint**: PQ with ADC only supports Euclidean distance.

**Reason**: ADC (Asymmetric Distance Computation) decomposes distance calculation as:

```
d(x, y) ≈ Σᵢ d(xᵢ, cᵢ(yᵢ))
```

This decomposition is mathematically valid only for Euclidean distance due to:
1. Euclidean distance is additive across subspaces
2. Cosine/dot product cannot be decomposed this way

**Reference**: Jégou et al., "Product Quantization for Nearest Neighbor Search", IEEE TPAMI 2011, Section 3.2

### Scalar Quantization (SQ)

**Supports all metrics**: Euclidean, Cosine, Dot Product

SQ preserves the relative ordering of values within each dimension, allowing correct distance computation for any metric.

### Binary Quantization (BQ)

**Uses Hamming distance** with optional rescoring.

For high recall, BQ retrieves `k * rescoringFactor` candidates using fast Hamming distance, then rescores with original full-precision vectors.

## Extension Pattern Compliance

This design follows the project's extension pattern:

1. **No Core Modifications**:
   - `FDBContext.save()` unchanged
   - `OnlineIndexer` unchanged
   - No changes to existing index maintainers

2. **Explicit User Invocation**:
   - Training via `CodebookTrainer.train()`
   - Saving via `CodebookTrainer.saveCodebook()`
   - Search via `QuantizedSimilar` FusionQuery

3. **Optional Feature**:
   - Users who don't import quantization code have zero overhead
   - Quantization is opt-in via `VectorIndexConfiguration`

## Comparison with Similar.swift

| Aspect | Similar.swift | QuantizedSimilar |
|--------|---------------|------------------|
| Context | `FusionContext.current` | `FusionContext.current` |
| Query Context | `IndexQueryContext` | `IndexQueryContext` |
| Index Discovery | `findIndexDescriptor()` | Same pattern |
| Distance | Full precision | ADC with codes |
| Training | Not needed | Required (CodebookTrainer) |

## Future Enhancements

1. **Automatic Codebook Refresh**: Background task to retrain codebook as data distribution changes
2. **Hybrid PQ+HNSW**: Use quantized vectors in HNSW graph for memory reduction
3. **GPU Acceleration**: SIMD-optimized distance computation for batch queries
