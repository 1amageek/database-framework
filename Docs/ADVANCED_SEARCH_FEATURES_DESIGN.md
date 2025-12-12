# Advanced Search Features Design

This document describes the design for advanced search features:
1. **BlockMaxWAND** - Efficient top-k BM25 retrieval
2. **Multi-Vector Support** - Late interaction / ColBERT-style search
3. **Stopword Filtering** - Search quality improvement

## SPM Module Architecture

```
database-kit (client-safe, no FDB dependency)
├── Core                           # Persistable, IndexKind protocols
├── Vector                         # VectorIndexKind, VectorMetric
├── FullText                       # FullTextIndexKind, TokenizationStrategy
│   └── StopwordSet               # NEW: Stopword definitions
└── ...

database-framework (server-only, FDB dependency)
├── DatabaseEngine                 # Core engine, no index-specific knowledge
│   ├── IndexQueryContext
│   └── IndexMaintainer
│
├── VectorIndex                    # Depends on: DatabaseEngine
│   ├── MultiVector/              # NEW: Multi-vector support
│   │   └── MultiVectorSearcher.swift
│   ├── HNSWIndexMaintainer.swift
│   └── FlatVectorIndexMaintainer.swift
│
├── FullTextIndex                  # Depends on: DatabaseEngine
│   ├── BlockMaxWAND/             # NEW: Efficient BM25 retrieval
│   │   ├── BlockMaxWANDSearcher.swift
│   │   └── PostingListBlock.swift
│   ├── StopwordFilter.swift      # NEW: Stopword filtering
│   └── FullTextIndexMaintainer.swift
│
└── Database                       # Re-exports all modules
```

**Key Constraint**: `DatabaseEngine` cannot depend on any index module.
Configuration types (StopwordSet) are in `database-kit` for client access.

## References

### BlockMaxWAND
- **Original WAND**: Broder et al., "Efficient Query Evaluation using a Two-Level Retrieval Process", CIKM 2003
- **BlockMaxWAND**: Ding & Suel, "Faster Top-k Document Retrieval Using Block-Max Indexes", SIGIR 2011
- **BMW Implementation**: Mallia et al., "PISA: Performant Indexes and Search for Academia", OSIRRC 2019

### Multi-Vector / Late Interaction
- **ColBERT**: Khattab & Zaharia, "ColBERT: Efficient and Effective Passage Search via Contextualized Late Interaction over BERT", SIGIR 2020
- **ColBERTv2**: Santhanam et al., "ColBERTv2: Effective and Efficient Retrieval via Lightweight Late Interaction", NAACL 2022

### Stopwords
- **Information Retrieval**: Manning et al., "Introduction to Information Retrieval", Cambridge University Press, 2008

---

## 1. BlockMaxWAND

### Overview

BlockMaxWAND (BMW) is an optimization for top-k BM25 retrieval that skips document blocks that cannot contribute to the top-k results.

**Current Problem**: The existing `searchWithScores()` method retrieves ALL matching documents, then sorts by score. This is O(N) where N = number of matching documents.

**Solution**: BMW maintains per-block maximum scores, enabling early termination when a block's maximum possible score is below the current k-th best score.

### Algorithm

```
BlockMaxWAND Algorithm:

1. Initialize:
   - result_heap = MaxHeap(k)  // Track top-k results
   - threshold = 0              // Minimum score to enter top-k

2. For each term posting list, organize into blocks:
   Block structure: [docIDs...] + block_max_score

3. Multi-term iteration with pivot selection:
   - Sort terms by current document position
   - Calculate upper bound = sum of block_max_scores
   - If upper_bound <= threshold: skip to next block (PRUNING)
   - Else: score document, update threshold if enters top-k

4. Early termination:
   - When all remaining blocks have upper_bound <= threshold
```

### Storage Layout Changes

Current inverted index structure:
```
[indexSubspace]["terms"][term][docId] = Tuple(tf) or Tuple(positions...)
```

New structure with block metadata:
```
[indexSubspace]["terms"][term]["blocks"][blockId] = BlockMetadata
[indexSubspace]["terms"][term]["postings"][blockId][docId] = Tuple(tf)
```

Where `BlockMetadata`:
```swift
struct BlockMetadata: Codable {
    let minDocId: String      // First docId in block
    let maxDocId: String      // Last docId in block
    let docCount: Int         // Number of documents in block
    let maxTF: Int            // Maximum term frequency in block
    let maxImpact: Float      // Pre-computed max BM25 contribution
}
```

### Block Size Selection

**Reference**: Ding & Suel, "Faster Top-k Document Retrieval Using Block-Max Indexes"

```swift
/// Block size configuration
/// - Small blocks: More pruning opportunities, higher overhead
/// - Large blocks: Less overhead, fewer pruning opportunities
/// - Sweet spot: 64-128 documents per block
public struct BlockMaxConfig: Sendable {
    /// Target documents per block (default: 64)
    public let blockSize: Int

    /// Minimum documents to use BMW (default: 1000)
    /// Below this, simple sorting may be faster
    public let minDocsForBMW: Int

    public static let `default` = BlockMaxConfig(blockSize: 64, minDocsForBMW: 1000)
}
```

### Implementation

#### PostingListBlock

```swift
// Sources/FullTextIndex/BlockMaxWAND/PostingListBlock.swift

/// A block within a posting list
public struct PostingListBlock: Sendable {
    /// Block identifier (sequential within term)
    public let blockId: Int

    /// Document IDs in this block
    public let docIds: [String]

    /// Term frequencies for each document
    public let termFrequencies: [Int]

    /// Maximum term frequency in block
    public let maxTF: Int

    /// Pre-computed maximum BM25 impact score
    /// Using pessimistic document length (avgDL)
    public let maxImpact: Float

    /// Check if block can contribute to top-k
    public func canContribute(threshold: Float, idf: Float) -> Bool {
        return maxImpact * idf > threshold
    }
}
```

#### BlockMaxWANDSearcher

```swift
// Sources/FullTextIndex/BlockMaxWAND/BlockMaxWANDSearcher.swift

/// BlockMaxWAND searcher for efficient top-k BM25 retrieval
public struct BlockMaxWANDSearcher: Sendable {
    private let config: BlockMaxConfig
    private let bm25Params: BM25Parameters

    public init(config: BlockMaxConfig = .default, bm25Params: BM25Parameters = .default) {
        self.config = config
        self.bm25Params = bm25Params
    }

    /// Search with BlockMaxWAND optimization
    public func search(
        terms: [String],
        k: Int,
        maintainer: FullTextIndexMaintainer<some Persistable>,
        transaction: any TransactionProtocol
    ) async throws -> [(docId: Tuple, score: Float)] {
        // Get corpus statistics
        let stats = try await maintainer.getBM25Statistics(transaction: transaction)
        guard stats.totalDocuments > 0 else { return [] }

        let scorer = BM25Scorer(params: bm25Params, statistics: stats)

        // Load posting list iterators with block metadata
        var termIterators: [TermBlockIterator] = []
        var documentFrequencies: [String: Int64] = [:]

        for term in terms {
            let df = try await maintainer.getDocumentFrequency(term: term, transaction: transaction)
            documentFrequencies[term] = df

            let iterator = try await TermBlockIterator(
                term: term,
                idf: scorer.idf(documentFrequency: df),
                maintainer: maintainer,
                transaction: transaction
            )
            termIterators.append(iterator)
        }

        // Result heap (min-heap by score for efficient threshold updates)
        var resultHeap = BoundedHeap<ScoredDoc>(capacity: k)
        var threshold: Float = 0

        // Main WAND loop
        while termIterators.allSatisfy({ !$0.exhausted }) {
            // Sort iterators by current document
            termIterators.sort { $0.currentDocId < $1.currentDocId }

            // Find pivot: first position where cumulative upper bound > threshold
            var cumulativeUpperBound: Float = 0
            var pivotIdx = 0

            for (idx, iterator) in termIterators.enumerated() {
                cumulativeUpperBound += iterator.currentBlockMaxImpact
                if cumulativeUpperBound > threshold {
                    pivotIdx = idx
                    break
                }
            }

            // If no pivot found, we're done
            if cumulativeUpperBound <= threshold {
                break
            }

            let pivotDoc = termIterators[pivotIdx].currentDocId

            // Check if all terms before pivot are at pivotDoc
            let allAtPivot = termIterators[0..<pivotIdx].allSatisfy {
                $0.currentDocId == pivotDoc
            }

            if allAtPivot {
                // Score the document
                let score = try await scoreDocument(
                    docId: pivotDoc,
                    termIterators: termIterators,
                    scorer: scorer,
                    documentFrequencies: documentFrequencies,
                    maintainer: maintainer,
                    transaction: transaction
                )

                if score > threshold {
                    resultHeap.insert(ScoredDoc(docId: pivotDoc, score: score))
                    if resultHeap.isFull {
                        threshold = resultHeap.min!.score
                    }
                }

                // Advance all iterators past pivotDoc
                for i in 0...pivotIdx {
                    try await termIterators[i].advancePast(pivotDoc)
                }
            } else {
                // Advance first iterator to pivotDoc
                try await termIterators[0].advanceTo(pivotDoc)
            }
        }

        return resultHeap.sorted().map { ($0.docId, $0.score) }
    }

    private func scoreDocument(
        docId: String,
        termIterators: [TermBlockIterator],
        scorer: BM25Scorer,
        documentFrequencies: [String: Int64],
        maintainer: FullTextIndexMaintainer<some Persistable>,
        transaction: any TransactionProtocol
    ) async throws -> Float {
        // Get document length
        guard let metadata = try await maintainer.getDocumentMetadata(
            id: Tuple(docId),
            transaction: transaction
        ) else {
            return 0
        }

        // Sum BM25 contributions from each term
        var termFrequencies: [String: Int] = [:]
        for iterator in termIterators where iterator.currentDocId == docId {
            termFrequencies[iterator.term] = iterator.currentTF
        }

        return scorer.score(
            termFrequencies: termFrequencies,
            documentFrequencies: documentFrequencies,
            docLength: Int(metadata.docLength)
        )
    }
}

/// Iterator over blocks of a term's posting list
private class TermBlockIterator {
    let term: String
    let idf: Float

    var currentBlockIdx: Int = 0
    var currentDocIdx: Int = 0
    var currentDocId: String = ""
    var currentTF: Int = 0
    var currentBlockMaxImpact: Float = 0
    var exhausted: Bool = false

    private var blocks: [PostingListBlock] = []

    init(
        term: String,
        idf: Float,
        maintainer: FullTextIndexMaintainer<some Persistable>,
        transaction: any TransactionProtocol
    ) async throws {
        self.term = term
        self.idf = idf
        self.blocks = try await maintainer.loadBlocksForTerm(term, transaction: transaction)

        if !blocks.isEmpty {
            await loadCurrentBlock()
        } else {
            exhausted = true
        }
    }

    func advanceTo(_ targetDocId: String) async throws {
        while currentDocId < targetDocId && !exhausted {
            try await advanceOne()
        }
    }

    func advancePast(_ docId: String) async throws {
        while currentDocId <= docId && !exhausted {
            try await advanceOne()
        }
    }

    private func advanceOne() async throws {
        currentDocIdx += 1

        if currentDocIdx >= blocks[currentBlockIdx].docIds.count {
            // Move to next block
            currentBlockIdx += 1
            currentDocIdx = 0

            if currentBlockIdx >= blocks.count {
                exhausted = true
                return
            }
        }

        await loadCurrentBlock()
    }

    private func loadCurrentBlock() async {
        let block = blocks[currentBlockIdx]
        currentDocId = block.docIds[currentDocIdx]
        currentTF = block.termFrequencies[currentDocIdx]
        currentBlockMaxImpact = block.maxImpact * idf
    }
}
```

### Index Building with Blocks

```swift
extension FullTextIndexMaintainer {
    /// Build block structure during index maintenance
    func updateIndexWithBlocks(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Standard update logic...

        // After adding term entries, update block metadata
        if let newItem = newItem {
            let newId = try DataAccess.extractId(from: newItem, using: idExpression)
            let newTokens = tokenize(extractText(from: newItem))

            var termPositions: [String: [Int]] = [:]
            for token in newTokens {
                termPositions[token.term, default: []].append(token.position)
            }

            for (term, positions) in termPositions {
                let tf = positions.count

                // Determine block for this document
                let blockId = try await getOrCreateBlock(
                    term: term,
                    docId: newId,
                    transaction: transaction
                )

                // Update block metadata if this TF is higher
                try await updateBlockMaxTF(
                    term: term,
                    blockId: blockId,
                    tf: tf,
                    transaction: transaction
                )
            }
        }
    }
}
```

### Performance Characteristics

| Scenario | Current | With BMW |
|----------|---------|----------|
| k=10, 1M matches | O(1M) | O(k × log k) typical |
| k=100, 1M matches | O(1M) | O(10k) typical |
| Highly selective query | O(N) | O(N) worst case |
| Common terms | O(N) | 10-100x speedup |

**Reference**: Mallia et al. report 2-10x speedup on TREC datasets.

---

## 2. Multi-Vector Support

### Overview

Multi-vector indexing allows a single document to have multiple vector representations. This enables:

1. **Late Interaction (ColBERT)**: Each document token has its own embedding
2. **Multi-view representations**: Different aspects of the same item
3. **Hierarchical embeddings**: Summary + detail vectors

### Architecture

```
Document "How to cook pasta"
    │
    ├── Token embeddings (ColBERT-style)
    │   ├── "how"   → [0.1, 0.2, ...]
    │   ├── "to"    → [0.3, 0.1, ...]
    │   ├── "cook"  → [0.5, 0.4, ...]
    │   └── "pasta" → [0.2, 0.8, ...]
    │
    └── Query: "pasta recipe"
        ├── "pasta"  → [0.2, 0.7, ...]
        └── "recipe" → [0.4, 0.3, ...]

        Score = MaxSim("pasta", doc_tokens) + MaxSim("recipe", doc_tokens)
             = max(sim("pasta", "how"), sim("pasta", "to"), ...) + ...
```

### Storage Layout

```
[indexSubspace]["multivec"]["doc"][docId]["count"] = Int64
[indexSubspace]["multivec"]["doc"][docId]["vectors"][vecIdx] = [Float]
[indexSubspace]["multivec"]["flat"][docId][vecIdx] = [Float]  // For brute-force
[indexSubspace]["multivec"]["hnsw"]["nodes"][docId][vecIdx] = HNSWNodeMetadata
```

### Configuration (database-kit)

```swift
// Sources/Vector/MultiVectorConfig.swift

/// Multi-vector index configuration
public struct MultiVectorConfig: Sendable, Codable, Hashable {
    /// Maximum vectors per document
    public let maxVectorsPerDoc: Int

    /// Scoring method for combining vector similarities
    public let scoringMethod: MultiVectorScoring

    /// Whether to normalize scores by vector count
    public let normalizeByCount: Bool

    public static let `default` = MultiVectorConfig(
        maxVectorsPerDoc: 512,
        scoringMethod: .maxSim,
        normalizeByCount: true
    )
}

/// Scoring methods for multi-vector search
public enum MultiVectorScoring: String, Sendable, Codable, Hashable {
    /// MaxSim: For each query vector, take max similarity across doc vectors
    /// Final score = sum of maxSims
    /// Reference: ColBERT
    case maxSim

    /// Average: Average similarity of all query-doc vector pairs
    case average

    /// Chamfer: Bidirectional max-pooling
    /// score = avg(max_doc(sim)) + avg(max_query(sim))
    case chamfer
}
```

### VectorIndexKind Extension_CONNECT_

    public let rescoringFactor: Int

    public static let `default` = BQConfig(rescoringFactor: 4)

    public init(rescoringFactor: Int = 4) {
        precondition(rescoringFactor >= 1, "rescoringFactor must be >= 1")
        self.rescoringFactor = rescoringFactor
    }
}
```

### Unified Configuration

```swift
// Sources/Vector/QuantizationConfig.swift

/// Vector quantization configuration
public enum QuantizationConfig: Sendable, Codable, Hashable {
    /// No quantization (full precision)
    case none

    /// Product Quantization
    case pq(PQConfig)

    /// Scalar Quantization
    case sq(SQConfig)

    /// Binary Quantization
    case bq(BQConfig)

    public static let defaultPQ = QuantizationConfig.pq(.default)
    public static let defaultSQ = QuantizationConfig.sq(.default)
    public static let defaultBQ = QuantizationConfig.bq(.default)
}
```

### VectorIndexKind Extension (database-kit)

```swift
// Sources/Vector/VectorIndexKind.swift (modified)

public struct VectorIndexKind<Root: Persistable>: IndexKind {
    public let fieldNames: [String]
    public let dimensions: Int
    public let metric: VectorMetric

    /// Quantization configuration (NEW)
    public let quantization: QuantizationConfig

    public init(
        embedding: PartialKeyPath<Root>,
        dimensions: Int,
        metric: VectorMetric = .cosine,
        quantization: QuantizationConfig = .none  // NEW
    ) {
        precondition(dimensions > 0, "Vector dimensions must be positive")
        self.fieldNames = [Root.fieldName(for: embedding)]
        self.dimensions = dimensions
        self.metric = metric
        self.quantization = quantization
    }
}
```

### Implementation (database-framework)

#### VectorQuantizer Protocol

```swift
// Sources/VectorIndex/Quantization/VectorQuantizer.swift

/// Protocol for vector quantization algorithms
public protocol VectorQuantizer: Sendable {
    associatedtype Code: Sendable

    /// Whether the quantizer has been trained
    var isTrained: Bool { get }

    /// Train the quantizer on sample vectors
    func train(vectors: [[Float]]) async throws

    /// Encode a vector to compressed code
    func encode(_ vector: [Float]) -> Code

    /// Decode a code back to approximate vector
    func decode(_ code: Code) -> [Float]

    /// Compute distance between query vector and encoded vector
    /// Uses asymmetric distance computation when available
    func distance(query: [Float], code: Code) -> Float

    /// Prepare distance computation (e.g., build lookup tables)
    func prepareQuery(_ query: [Float]) -> Any

    /// Fast distance using prepared query state
    func distanceWithPrepared(prepared: Any, code: Code) -> Float

    /// Serialize quantizer state for storage
    func serialize() throws -> Data

    /// Deserialize quantizer state
    static func deserialize(from data: Data) throws -> Self
}
```

#### ProductQuantizer Implementation

```swift
// Sources/VectorIndex/Quantization/ProductQuantizer.swift

public final class ProductQuantizer: VectorQuantizer, @unchecked Sendable {
    public typealias Code = [UInt8]

    private let config: PQConfig
    private let dimensions: Int
    private let subDim: Int

    /// Codebook: [M][K][subDim] centroids
    private var codebook: [[[Float]]]
    private var _isTrained: Bool = false

    public var isTrained: Bool { _isTrained }

    public init(config: PQConfig, dimensions: Int) {
        precondition(dimensions % config.numSubquantizers == 0,
                     "dimensions must be divisible by numSubquantizers")
        self.config = config
        self.dimensions = dimensions
        self.subDim = dimensions / config.numSubquantizers
        self.codebook = []
    }

    public func train(vectors: [[Float]]) async throws {
        let M = config.numSubquantizers
        let K = config.numCentroids

        codebook = Array(repeating: [], count: M)

        // Train each subquantizer independently
        for m in 0..<M {
            // Extract subvectors for this subspace
            let subvectors = vectors.map { vector in
                Array(vector[m * subDim..<(m + 1) * subDim])
            }

            // Run k-means clustering
            codebook[m] = try await kmeans(
                vectors: subvectors,
                k: K,
                iterations: config.kmeansIterations
            )
        }

        _isTrained = true
    }

    public func encode(_ vector: [Float]) -> [UInt8] {
        precondition(isTrained, "Quantizer must be trained before encoding")

        var code = [UInt8](repeating: 0, count: config.numSubquantizers)

        for m in 0..<config.numSubquantizers {
            let subvector = Array(vector[m * subDim..<(m + 1) * subDim])

            // Find nearest centroid
            var minDist = Float.infinity
            var minIdx = 0

            for (idx, centroid) in codebook[m].enumerated() {
                let dist = squaredEuclidean(subvector, centroid)
                if dist < minDist {
                    minDist = dist
                    minIdx = idx
                }
            }

            code[m] = UInt8(minIdx)
        }

        return code
    }

    public func decode(_ code: [UInt8]) -> [Float] {
        var vector = [Float](repeating: 0, count: dimensions)

        for m in 0..<config.numSubquantizers {
            let centroid = codebook[m][Int(code[m])]
            for i in 0..<subDim {
                vector[m * subDim + i] = centroid[i]
            }
        }

        return vector
    }

    /// Prepare Asymmetric Distance Computation tables
    public func prepareQuery(_ query: [Float]) -> Any {
        var tables = [[Float]](repeating: [], count: config.numSubquantizers)

        for m in 0..<config.numSubquantizers {
            let subquery = Array(query[m * subDim..<(m + 1) * subDim])
            tables[m] = codebook[m].map { centroid in
                squaredEuclidean(subquery, centroid)
            }
        }

        return tables as [[Float]]
    }

    public func distanceWithPrepared(prepared: Any, code: [UInt8]) -> Float {
        let tables = prepared as! [[Float]]
        var dist: Float = 0

        for m in 0..<config.numSubquantizers {
            dist += tables[m][Int(code[m])]
        }

        return dist
    }

    public func distance(query: [Float], code: [UInt8]) -> Float {
        let prepared = prepareQuery(query)
        return distanceWithPrepared(prepared: prepared, code: code)
    }

    // MARK: - K-means Implementation

    private func kmeans(
        vectors: [[Float]],
        k: Int,
        iterations: Int
    ) async throws -> [[Float]] {
        let dim = vectors[0].count

        // Initialize centroids with k-means++
        var centroids = kmeansppInit(vectors: vectors, k: k)

        for _ in 0..<iterations {
            // Assignment step
            var assignments = [Int](repeating: 0, count: vectors.count)
            var counts = [Int](repeating: 0, count: k)
            var newCentroids = [[Float]](repeating: [Float](repeating: 0, count: dim), count: k)

            for (i, vector) in vectors.enumerated() {
                var minDist = Float.infinity
                var minIdx = 0

                for (j, centroid) in centroids.enumerated() {
                    let dist = squaredEuclidean(vector, centroid)
                    if dist < minDist {
                        minDist = dist
                        minIdx = j
                    }
                }

                assignments[i] = minIdx
                counts[minIdx] += 1

                for d in 0..<dim {
                    newCentroids[minIdx][d] += vector[d]
                }
            }

            // Update step
            for j in 0..<k {
                if counts[j] > 0 {
                    for d in 0..<dim {
                        centroids[j][d] = newCentroids[j][d] / Float(counts[j])
                    }
                }
            }
        }

        return centroids
    }

    private func kmeansppInit(vectors: [[Float]], k: Int) -> [[Float]] {
        var centroids: [[Float]] = []

        // First centroid: random
        centroids.append(vectors.randomElement()!)

        // Remaining centroids: proportional to squared distance
        for _ in 1..<k {
            var distances = [Float](repeating: 0, count: vectors.count)
            var totalDist: Float = 0

            for (i, vector) in vectors.enumerated() {
                var minDist = Float.infinity
                for centroid in centroids {
                    minDist = min(minDist, squaredEuclidean(vector, centroid))
                }
                distances[i] = minDist
                totalDist += minDist
            }

            // Sample proportional to distance
            let threshold = Float.random(in: 0..<totalDist)
            var cumulative: Float = 0
            var chosen = 0

            for (i, dist) in distances.enumerated() {
                cumulative += dist
                if cumulative >= threshold {
                    chosen = i
                    break
                }
            }

            centroids.append(vectors[chosen])
        }

        return centroids
    }

    private func squaredEuclidean(_ a: [Float], _ b: [Float]) -> Float {
        var sum: Float = 0
        for i in 0..<a.count {
            let diff = a[i] - b[i]
            sum += diff * diff
        }
        return sum
    }
}
```

### Integration with HNSW

```swift
// Sources/VectorIndex/HNSWIndexMaintainer.swift (modified)

public struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // ... existing properties ...

    /// Vector quantizer (optional)
    private var quantizer: (any VectorQuantizer)?

    /// Search with optional quantization
    public func search(
        queryVector: [Float],
        k: Int,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: Tuple, distance: Float)] {
        if let quantizer = quantizer, quantizer.isTrained {
            return try await searchQuantized(
                queryVector: queryVector,
                k: k,
                searchParams: searchParams,
                transaction: transaction
            )
        } else {
            return try await searchExact(
                queryVector: queryVector,
                k: k,
                searchParams: searchParams,
                transaction: transaction
            )
        }
    }

    /// Quantized search with optional rescoring
    private func searchQuantized(
        queryVector: [Float],
        k: Int,
        searchParams: HNSWSearchParameters,
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: Tuple, distance: Float)] {
        guard let quantizer = quantizer else {
            return try await searchExact(queryVector: queryVector, k: k,
                                         searchParams: searchParams, transaction: transaction)
        }

        // Retrieve more candidates for rescoring
        let rescoringK = k * 4  // Configurable

        // Search using quantized distances
        let prepared = quantizer.prepareQuery(queryVector)

        // ... HNSW traversal using quantizer.distanceWithPrepared() ...

        // Rescore top candidates with exact distances
        var results: [(primaryKey: Tuple, distance: Float)] = []
        for candidate in candidates.prefix(rescoringK) {
            let exactVector = try await loadVector(primaryKey: candidate.primaryKey, transaction: transaction)
            let exactDistance = calculateExactDistance(queryVector, exactVector)
            results.append((candidate.primaryKey, exactDistance))
        }

        // Sort by exact distance and return top k
        results.sort { $0.distance < $1.distance }
        return Array(results.prefix(k))
    }
}
```

### Training Workflow

```swift
// Training quantizer on existing data
let trainer = VectorQuantizerTrainer(container: container)
try await trainer.train(
    type: Product.self,
    indexName: "Product_vector_embedding",
    config: .pq(PQConfig(numSubquantizers: 48))
)

// Or during OnlineIndexer rebuild
let indexer = OnlineIndexer(container: container)
try await indexer.rebuild(
    for: Product.self,
    indexDescriptor: embeddingIndex,
    quantization: .pq(.default)  // Train during rebuild
)
```

---

## 2. BlockMaxWAND

### Overview

BlockMaxWAND (BMW) is an optimization for top-k BM25 retrieval that skips document blocks that cannot contribute to the top-k results.

**Current Problem**: The existing `searchWithScores()` method retrieves ALL matching documents, then sorts by score. This is O(N) where N = number of matching documents.

**Solution**: BMW maintains per-block maximum scores, enabling early termination when a block's maximum possible score is below the current k-th best score.

### Algorithm

```
BlockMaxWAND Algorithm:

1. Initialize:
   - result_heap = MaxHeap(k)  // Track top-k results
   - threshold = 0              // Minimum score to enter top-k

2. For each term posting list, organize into blocks:
   Block structure: [docIDs...] + block_max_score

3. Multi-term iteration with pivot selection:
   - Sort terms by current document position
   - Calculate upper bound = sum of block_max_scores
   - If upper_bound <= threshold: skip to next block (PRUNING)
   - Else: score document, update threshold if enters top-k

4. Early termination:
   - When all remaining blocks have upper_bound <= threshold
```

### Storage Layout Changes

Current inverted index structure:
```
[indexSubspace]["terms"][term][docId] = Tuple(tf) or Tuple(positions...)
```

New structure with block metadata:
```
[indexSubspace]["terms"][term]["blocks"][blockId] = BlockMetadata
[indexSubspace]["terms"][term]["postings"][blockId][docId] = Tuple(tf)
```

Where `BlockMetadata`:
```swift
struct BlockMetadata: Codable {
    let minDocId: String      // First docId in block
    let maxDocId: String      // Last docId in block
    let docCount: Int         // Number of documents in block
    let maxTF: Int            // Maximum term frequency in block
    let maxImpact: Float      // Pre-computed max BM25 contribution
}
```

### Block Size Selection

**Reference**: Ding & Suel, "Faster Top-k Document Retrieval Using Block-Max Indexes"

```swift
/// Block size configuration
/// - Small blocks: More pruning opportunities, higher overhead
/// - Large blocks: Less overhead, fewer pruning opportunities
/// - Sweet spot: 64-128 documents per block
public struct BlockMaxConfig: Sendable {
    /// Target documents per block (default: 64)
    public let blockSize: Int

    /// Minimum documents to use BMW (default: 1000)
    /// Below this, simple sorting may be faster
    public let minDocsForBMW: Int

    public static let `default` = BlockMaxConfig(blockSize: 64, minDocsForBMW: 1000)
}
```

### Implementation

#### PostingListBlock

```swift
// Sources/FullTextIndex/BlockMaxWAND/PostingListBlock.swift

/// A block within a posting list
public struct PostingListBlock: Sendable {
    /// Block identifier (sequential within term)
    public let blockId: Int

    /// Document IDs in this block
    public let docIds: [String]

    /// Term frequencies for each document
    public let termFrequencies: [Int]

    /// Maximum term frequency in block
    public let maxTF: Int

    /// Pre-computed maximum BM25 impact score
    /// Using pessimistic document length (avgDL)
    public let maxImpact: Float

    /// Check if block can contribute to top-k
    public func canContribute(threshold: Float, idf: Float) -> Bool {
        return maxImpact * idf > threshold
    }
}
```

#### BlockMaxWANDSearcher

```swift
// Sources/FullTextIndex/BlockMaxWAND/BlockMaxWANDSearcher.swift

/// BlockMaxWAND searcher for efficient top-k BM25 retrieval
public struct BlockMaxWANDSearcher: Sendable {
    private let config: BlockMaxConfig
    private let bm25Params: BM25Parameters

    public init(config: BlockMaxConfig = .default, bm25Params: BM25Parameters = .default) {
        self.config = config
        self.bm25Params = bm25Params
    }

    /// Search with BlockMaxWAND optimization
    public func search(
        terms: [String],
        k: Int,
        maintainer: FullTextIndexMaintainer<some Persistable>,
        transaction: any TransactionProtocol
    ) async throws -> [(docId: Tuple, score: Float)] {
        // Get corpus statistics
        let stats = try await maintainer.getBM25Statistics(transaction: transaction)
        guard stats.totalDocuments > 0 else { return [] }

        let scorer = BM25Scorer(params: bm25Params, statistics: stats)

        // Load posting list iterators with block metadata
        var termIterators: [TermBlockIterator] = []
        var documentFrequencies: [String: Int64] = [:]

        for term in terms {
            let df = try await maintainer.getDocumentFrequency(term: term, transaction: transaction)
            documentFrequencies[term] = df

            let iterator = try await TermBlockIterator(
                term: term,
                idf: scorer.idf(documentFrequency: df),
                maintainer: maintainer,
                transaction: transaction
            )
            termIterators.append(iterator)
        }

        // Result heap (min-heap by score for efficient threshold updates)
        var resultHeap = BoundedHeap<ScoredDoc>(capacity: k)
        var threshold: Float = 0

        // Main WAND loop
        while termIterators.allSatisfy({ !$0.exhausted }) {
            // Sort iterators by current document
            termIterators.sort { $0.currentDocId < $1.currentDocId }

            // Find pivot: first position where cumulative upper bound > threshold
            var cumulativeUpperBound: Float = 0
            var pivotIdx = 0

            for (idx, iterator) in termIterators.enumerated() {
                cumulativeUpperBound += iterator.currentBlockMaxImpact
                if cumulativeUpperBound > threshold {
                    pivotIdx = idx
                    break
                }
            }

            // If no pivot found, we're done
            if cumulativeUpperBound <= threshold {
                break
            }

            let pivotDoc = termIterators[pivotIdx].currentDocId

            // Check if all terms before pivot are at pivotDoc
            let allAtPivot = termIterators[0..<pivotIdx].allSatisfy {
                $0.currentDocId == pivotDoc
            }

            if allAtPivot {
                // Score the document
                let score = try await scoreDocument(
                    docId: pivotDoc,
                    termIterators: termIterators,
                    scorer: scorer,
                    documentFrequencies: documentFrequencies,
                    maintainer: maintainer,
                    transaction: transaction
                )

                if score > threshold {
                    resultHeap.insert(ScoredDoc(docId: pivotDoc, score: score))
                    if resultHeap.isFull {
                        threshold = resultHeap.min!.score
                    }
                }

                // Advance all iterators past pivotDoc
                for i in 0...pivotIdx {
                    try await termIterators[i].advancePast(pivotDoc)
                }
            } else {
                // Advance first iterator to pivotDoc
                try await termIterators[0].advanceTo(pivotDoc)
            }
        }

        return resultHeap.sorted().map { ($0.docId, $0.score) }
    }

    private func scoreDocument(
        docId: String,
        termIterators: [TermBlockIterator],
        scorer: BM25Scorer,
        documentFrequencies: [String: Int64],
        maintainer: FullTextIndexMaintainer<some Persistable>,
        transaction: any TransactionProtocol
    ) async throws -> Float {
        // Get document length
        guard let metadata = try await maintainer.getDocumentMetadata(
            id: Tuple(docId),
            transaction: transaction
        ) else {
            return 0
        }

        // Sum BM25 contributions from each term
        var termFrequencies: [String: Int] = [:]
        for iterator in termIterators where iterator.currentDocId == docId {
            termFrequencies[iterator.term] = iterator.currentTF
        }

        return scorer.score(
            termFrequencies: termFrequencies,
            documentFrequencies: documentFrequencies,
            docLength: Int(metadata.docLength)
        )
    }
}

/// Iterator over blocks of a term's posting list
private class TermBlockIterator {
    let term: String
    let idf: Float

    var currentBlockIdx: Int = 0
    var currentDocIdx: Int = 0
    var currentDocId: String = ""
    var currentTF: Int = 0
    var currentBlockMaxImpact: Float = 0
    var exhausted: Bool = false

    private var blocks: [PostingListBlock] = []

    init(
        term: String,
        idf: Float,
        maintainer: FullTextIndexMaintainer<some Persistable>,
        transaction: any TransactionProtocol
    ) async throws {
        self.term = term
        self.idf = idf
        self.blocks = try await maintainer.loadBlocksForTerm(term, transaction: transaction)

        if !blocks.isEmpty {
            await loadCurrentBlock()
        } else {
            exhausted = true
        }
    }

    func advanceTo(_ targetDocId: String) async throws {
        while currentDocId < targetDocId && !exhausted {
            try await advanceOne()
        }
    }

    func advancePast(_ docId: String) async throws {
        while currentDocId <= docId && !exhausted {
            try await advanceOne()
        }
    }

    private func advanceOne() async throws {
        currentDocIdx += 1

        if currentDocIdx >= blocks[currentBlockIdx].docIds.count {
            // Move to next block
            currentBlockIdx += 1
            currentDocIdx = 0

            if currentBlockIdx >= blocks.count {
                exhausted = true
                return
            }
        }

        await loadCurrentBlock()
    }

    private func loadCurrentBlock() async {
        let block = blocks[currentBlockIdx]
        currentDocId = block.docIds[currentDocIdx]
        currentTF = block.termFrequencies[currentDocIdx]
        currentBlockMaxImpact = block.maxImpact * idf
    }
}
```

### Index Building with Blocks

```swift
extension FullTextIndexMaintainer {
    /// Build block structure during index maintenance
    func updateIndexWithBlocks(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Standard update logic...

        // After adding term entries, update block metadata
        if let newItem = newItem {
            let newId = try DataAccess.extractId(from: newItem, using: idExpression)
            let newTokens = tokenize(extractText(from: newItem))

            var termPositions: [String: [Int]] = [:]
            for token in newTokens {
                termPositions[token.term, default: []].append(token.position)
            }

            for (term, positions) in termPositions {
                let tf = positions.count

                // Determine block for this document
                let blockId = try await getOrCreateBlock(
                    term: term,
                    docId: newId,
                    transaction: transaction
                )

                // Update block metadata if this TF is higher
                try await updateBlockMaxTF(
                    term: term,
                    blockId: blockId,
                    tf: tf,
                    transaction: transaction
                )
            }
        }
    }
}
```

### Performance Characteristics

| Scenario | Current | With BMW |
|----------|---------|----------|
| k=10, 1M matches | O(1M) | O(k × log k) typical |
| k=100, 1M matches | O(1M) | O(10k) typical |
| Highly selective query | O(N) | O(N) worst case |
| Common terms | O(N) | 10-100x speedup |

**Reference**: Mallia et al. report 2-10x speedup on TREC datasets.

---

## 3. Multi-Vector Support

### Overview

Multi-vector indexing allows a single document to have multiple vector representations. This enables:

1. **Late Interaction (ColBERT)**: Each document token has its own embedding
2. **Multi-view representations**: Different aspects of the same item
3. **Hierarchical embeddings**: Summary + detail vectors

### Architecture

```
Document "How to cook pasta"
    │
    ├── Token embeddings (ColBERT-style)
    │   ├── "how"   → [0.1, 0.2, ...]
    │   ├── "to"    → [0.3, 0.1, ...]
    │   ├── "cook"  → [0.5, 0.4, ...]
    │   └── "pasta" → [0.2, 0.8, ...]
    │
    └── Query: "pasta recipe"
        ├── "pasta"  → [0.2, 0.7, ...]
        └── "recipe" → [0.4, 0.3, ...]

        Score = MaxSim("pasta", doc_tokens) + MaxSim("recipe", doc_tokens)
             = max(sim("pasta", "how"), sim("pasta", "to"), ...) + ...
```

### Storage Layout

```
[indexSubspace]["multivec"]["doc"][docId]["count"] = Int64
[indexSubspace]["multivec"]["doc"][docId]["vectors"][vecIdx] = [Float]
[indexSubspace]["multivec"]["flat"][docId][vecIdx] = [Float]  // For brute-force
[indexSubspace]["multivec"]["hnsw"]["nodes"][docId][vecIdx] = HNSWNodeMetadata
```

### Configuration (database-kit)

```swift
// Sources/Vector/MultiVectorConfig.swift

/// Multi-vector index configuration
public struct MultiVectorConfig: Sendable, Codable, Hashable {
    /// Maximum vectors per document
    public let maxVectorsPerDoc: Int

    /// Scoring method for combining vector similarities
    public let scoringMethod: MultiVectorScoring

    /// Whether to normalize scores by vector count
    public let normalizeByCount: Bool

    public static let `default` = MultiVectorConfig(
        maxVectorsPerDoc: 512,
        scoringMethod: .maxSim,
        normalizeByCount: true
    )
}

/// Scoring methods for multi-vector search
public enum MultiVectorScoring: String, Sendable, Codable, Hashable {
    /// MaxSim: For each query vector, take max similarity across doc vectors
    /// Final score = sum of maxSims
    /// Reference: ColBERT
    case maxSim

    /// Average: Average similarity of all query-doc vector pairs
    case average

    /// Chamfer: Bidirectional max-pooling
    /// score = avg(max_doc(sim)) + avg(max_query(sim))
    case chamfer
}
```

### VectorIndexKind Extension

```swift
// Sources/Vector/VectorIndexKind.swift (modified)

public struct VectorIndexKind<Root: Persistable>: IndexKind {
    // ... existing properties ...

    /// Multi-vector configuration (NEW)
    /// When set, the field is expected to be [[Float]] instead of [Float]
    public let multiVector: MultiVectorConfig?

    public init(
        embedding: PartialKeyPath<Root>,
        dimensions: Int,
        metric: VectorMetric = .cosine,
        quantization: QuantizationConfig = .none,
        multiVector: MultiVectorConfig? = nil  // NEW
    ) {
        // ...
    }
}
```

### Implementation (database-framework)

#### MultiVectorSearcher

```swift
// Sources/VectorIndex/MultiVector/MultiVectorSearcher.swift

/// Searcher for multi-vector indexes
public struct MultiVectorSearcher<Item: Persistable>: Sendable {
    private let config: MultiVectorConfig
    private let dimensions: Int
    private let metric: VectorMetric

    /// Search with multiple query vectors (ColBERT-style)
    public func search(
        queryVectors: [[Float]],
        k: Int,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(docId: Tuple, score: Float)] {
        switch config.scoringMethod {
        case .maxSim:
            return try await searchMaxSim(
                queryVectors: queryVectors,
                k: k,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        case .average:
            return try await searchAverage(
                queryVectors: queryVectors,
                k: k,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        case .chamfer:
            return try await searchChamfer(
                queryVectors: queryVectors,
                k: k,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }
    }

    /// MaxSim scoring (ColBERT)
    private func searchMaxSim(
        queryVectors: [[Float]],
        k: Int,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(docId: Tuple, score: Float)] {
        // Step 1: For each query vector, find candidate documents
        // Use HNSW or approximate search to get top candidates per query vector
        var candidateScores: [String: [Float]] = [:]  // docId -> [maxSim per query vec]

        for (qIdx, queryVec) in queryVectors.enumerated() {
            // Search for this query vector
            let candidates = try await searchSingleVector(
                queryVector: queryVec,
                k: k * 10,  // Retrieve more for coverage
                indexSubspace: indexSubspace,
                transaction: transaction
            )

            for (docId, similarity) in candidates {
                let docIdStr = docId.description
                if candidateScores[docIdStr] == nil {
                    candidateScores[docIdStr] = [Float](repeating: 0, count: queryVectors.count)
                }
                // Update maxSim for this query vector
                candidateScores[docIdStr]![qIdx] = max(
                    candidateScores[docIdStr]![qIdx],
                    similarity
                )
            }
        }

        // Step 2: Compute final scores
        var results: [(docId: Tuple, score: Float)] = []

        for (docIdStr, maxSims) in candidateScores {
            let score = maxSims.reduce(0, +)
            let normalizedScore = config.normalizeByCount
                ? score / Float(queryVectors.count)
                : score

            // Parse docId back to Tuple
            if let docId = parseDocId(docIdStr) {
                results.append((docId, normalizedScore))
            }
        }

        // Sort and return top k
        results.sort { $0.score > $1.score }
        return Array(results.prefix(k))
    }

    /// Search returning similarity (not distance) for multi-vector scoring
    private func searchSingleVector(
        queryVector: [Float],
        k: Int,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [(docId: Tuple, similarity: Float)] {
        // Load all document vectors for this subspace
        // In production, use HNSW for efficiency
        let flatSubspace = indexSubspace.subspace("multivec").subspace("flat")
        let (begin, end) = flatSubspace.range()

        var results: [(docId: Tuple, similarity: Float)] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, value) in sequence {
            guard flatSubspace.contains(key) else { break }

            let keyTuple = try flatSubspace.unpack(key)
            // keyTuple = [docId, vecIdx]

            let vector = try decodeVector(value)
            let similarity = cosineSimilarity(queryVector, vector)

            // Extract docId (first element(s) of keyTuple)
            let docId = extractDocId(from: keyTuple)

            results.append((docId, similarity))
        }

        // Return best match per document
        var bestPerDoc: [String: (docId: Tuple, similarity: Float)] = [:]
        for result in results {
            let key = result.docId.description
            if bestPerDoc[key] == nil || result.similarity > bestPerDoc[key]!.similarity {
                bestPerDoc[key] = result
            }
        }

        return Array(bestPerDoc.values)
            .sorted { $0.similarity > $1.similarity }
            .prefix(k)
            .map { $0 }
    }
}
```

### Usage

```swift
// Model definition
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var title: String

    /// ColBERT-style token embeddings
    var tokenEmbeddings: [[Float]]

    #Index<Document>(
        [\.tokenEmbeddings],
        type: VectorIndexKind(
            embedding: \.tokenEmbeddings,
            dimensions: 128,
            metric: .cosine,
            multiVector: MultiVectorConfig(
                maxVectorsPerDoc: 256,
                scoringMethod: .maxSim
            )
        )
    )
}

// Query with multiple vectors
let queryTokenEmbeddings: [[Float]] = embedQuery("how to cook pasta")

let results = try await context.findSimilar(Document.self)
    .multiVector(\.tokenEmbeddings, dimensions: 128)
    .query(queryTokenEmbeddings, k: 10)
    .execute()
```

---

## 4. Stopword Filtering

### Overview

Stopwords are common words (e.g., "the", "a", "is") that carry little semantic meaning and can degrade search quality by:

1. Increasing index size unnecessarily
2. Reducing precision (matching too many documents)
3. Slowing down search

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     database-kit (FullText)                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     StopwordSet                           │  │
│  │  ├── .english     (175 words)                             │  │
│  │  ├── .german      (231 words)                             │  │
│  │  ├── .french      (164 words)                             │  │
│  │  ├── .spanish     (313 words)                             │  │
│  │  ├── .japanese    (hiragana particles)                    │  │
│  │  ├── .none        (no filtering)                          │  │
│  │  └── .custom([String])                                    │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                 database-framework (FullTextIndex)               │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   StopwordFilter                          │  │
│  │  ├── filter(tokens: [Token]) -> [Token]                   │  │
│  │  └── isStopword(_ term: String) -> Bool                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              FullTextIndexMaintainer                      │  │
│  │  tokenize() → filter stopwords → index                    │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Configuration (database-kit)

```swift
// Sources/FullText/StopwordSet.swift

/// Predefined stopword sets by language
public enum StopwordSet: Sendable, Codable, Hashable {
    /// No stopword filtering
    case none

    /// English stopwords (NLTK + Lucene combined, 175 words)
    case english

    /// German stopwords (231 words)
    case german

    /// French stopwords (164 words)
    case french

    /// Spanish stopwords (313 words)
    case spanish

    /// Japanese stopwords (particles, auxiliary verbs)
    case japanese

    /// Custom stopword list
    case custom(Set<String>)

    /// Get stopwords as Set
    public var words: Set<String> {
        switch self {
        case .none:
            return []
        case .english:
            return Self.englishStopwords
        case .german:
            return Self.germanStopwords
        case .french:
            return Self.frenchStopwords
        case .spanish:
            return Self.spanishStopwords
        case .japanese:
            return Self.japaneseStopwords
        case .custom(let words):
            return words
        }
    }

    // MARK: - Predefined Stopword Lists

    /// English stopwords
    /// Sources: NLTK, Lucene, Snowball
    /// Reference: Manning et al., "Introduction to Information Retrieval", Chapter 2
    private static let englishStopwords: Set<String> = [
        // Articles
        "a", "an", "the",

        // Pronouns
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
        "you", "your", "yours", "yourself", "yourselves",
        "he", "him", "his", "himself", "she", "her", "hers", "herself",
        "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
        "what", "which", "who", "whom", "this", "that", "these", "those",

        // Verbs (to be, to have, to do)
        "am", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "having",
        "do", "does", "did", "doing",

        // Prepositions
        "at", "by", "for", "from", "in", "into", "of", "on", "to", "with",
        "about", "above", "across", "after", "against", "along", "among",
        "around", "before", "behind", "below", "beneath", "beside", "between",
        "beyond", "during", "except", "inside", "near", "off", "onto",
        "outside", "over", "past", "since", "through", "throughout",
        "toward", "under", "underneath", "until", "upon", "within", "without",

        // Conjunctions
        "and", "but", "or", "nor", "so", "yet", "both", "either", "neither",
        "not", "only", "than", "when", "where", "while", "although", "because",
        "if", "unless", "whether",

        // Auxiliary/Modal
        "can", "could", "may", "might", "must", "shall", "should", "will", "would",

        // Other common words
        "all", "any", "each", "every", "few", "more", "most", "other", "some", "such",
        "no", "own", "same", "too", "very", "just", "also", "now", "here", "there",
        "how", "why", "again", "once", "then", "further", "still",

        // Contractions (without apostrophe, as tokenizer may strip them)
        "dont", "doesnt", "didnt", "isnt", "arent", "wasnt", "werent",
        "hasnt", "havent", "hadnt", "wont", "wouldnt", "cant", "couldnt",
        "shouldnt", "mustnt", "neednt"
    ]

    /// German stopwords
    private static let germanStopwords: Set<String> = [
        "der", "die", "das", "den", "dem", "des",
        "ein", "eine", "einer", "einem", "einen", "eines",
        "und", "oder", "aber", "denn", "weil", "wenn", "als", "ob",
        "ich", "du", "er", "sie", "es", "wir", "ihr",
        "mein", "dein", "sein", "unser", "euer",
        "ist", "sind", "war", "waren", "wird", "werden", "wurde", "wurden",
        "hat", "haben", "hatte", "hatten",
        "kann", "können", "konnte", "konnten",
        "muss", "müssen", "musste", "mussten",
        "soll", "sollen", "sollte", "sollten",
        "will", "wollen", "wollte", "wollten",
        "in", "an", "auf", "aus", "bei", "mit", "nach", "von", "zu", "über", "unter",
        "für", "gegen", "durch", "um", "ohne", "bis", "seit", "während",
        "nicht", "kein", "keine", "keiner", "keinem", "keinen", "keines",
        "auch", "noch", "schon", "nur", "sehr", "mehr", "viel", "wenig",
        "hier", "dort", "wo", "wann", "wie", "warum", "was", "wer",
        "dieser", "diese", "dieses", "diesem", "diesen",
        "jeder", "jede", "jedes", "jedem", "jeden",
        "alle", "alles", "allem", "allen"
        // ... more German stopwords
    ]

    /// French stopwords
    private static let frenchStopwords: Set<String> = [
        "le", "la", "les", "un", "une", "des", "du", "de", "d",
        "et", "ou", "mais", "donc", "car", "ni", "que", "qui", "quoi",
        "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles",
        "mon", "ton", "son", "notre", "votre", "leur",
        "ce", "cette", "ces", "cet",
        "est", "sont", "était", "étaient", "sera", "seront",
        "a", "ont", "avait", "avaient", "aura", "auront",
        "peut", "peuvent", "pouvait", "pouvaient",
        "dans", "sur", "sous", "avec", "sans", "pour", "par", "entre",
        "ne", "pas", "plus", "moins", "très", "bien", "mal",
        "ici", "là", "où", "quand", "comment", "pourquoi",
        "tout", "tous", "toute", "toutes", "autre", "autres"
        // ... more French stopwords
    ]

    /// Spanish stopwords
    private static let spanishStopwords: Set<String> = [
        "el", "la", "los", "las", "un", "una", "unos", "unas",
        "y", "o", "pero", "porque", "que", "quien", "cual",
        "yo", "tú", "él", "ella", "nosotros", "vosotros", "ellos", "ellas",
        "mi", "tu", "su", "nuestro", "vuestro",
        "este", "esta", "estos", "estas", "ese", "esa", "esos", "esas",
        "es", "son", "era", "eran", "será", "serán",
        "ha", "han", "había", "habían", "habrá", "habrán",
        "en", "de", "por", "para", "con", "sin", "sobre", "entre",
        "no", "sí", "muy", "más", "menos", "tan", "tanto",
        "aquí", "allí", "donde", "cuando", "como", "por qué",
        "todo", "todos", "toda", "todas", "otro", "otros", "otra", "otras"
        // ... more Spanish stopwords
    ]

    /// Japanese stopwords (particles and auxiliary verbs)
    private static let japaneseStopwords: Set<String> = [
        // Particles
        "は", "が", "を", "に", "で", "と", "も", "の", "へ", "から", "まで",
        "より", "ば", "て", "や", "か", "な", "ね", "よ", "わ",

        // Auxiliary verbs
        "です", "ます", "である", "だ", "った", "ない", "なかった",
        "れる", "られる", "せる", "させる",

        // Common words with low information content
        "こと", "もの", "ため", "ところ", "とき", "よう", "など"
    ]
}
```

### FullTextIndexKind Extension (database-kit)

```swift
// Sources/FullText/FullTextIndexKind.swift (modified)

public struct FullTextIndexKind<Root: Persistable>: IndexKind {
    public let fieldNames: [String]
    public let tokenizer: TokenizationStrategy
    public let storePositions: Bool
    public let ngramSize: Int
    public let minTermLength: Int

    /// Stopword configuration (NEW)
    public let stopwords: StopwordSet

    public init(
        fields: [PartialKeyPath<Root>],
        tokenizer: TokenizationStrategy = .simple,
        storePositions: Bool = true,
        ngramSize: Int = 3,
        minTermLength: Int = 2,
        stopwords: StopwordSet = .none  // NEW: default to none for backward compatibility
    ) {
        self.fieldNames = fields.map { Root.fieldName(for: $0) }
        self.tokenizer = tokenizer
        self.storePositions = storePositions
        self.ngramSize = ngramSize
        self.minTermLength = minTermLength
        self.stopwords = stopwords
    }
}
```

### Implementation (database-framework)

#### StopwordFilter

```swift
// Sources/FullTextIndex/StopwordFilter.swift

/// Filter for removing stopwords from token streams
public struct StopwordFilter: Sendable {
    private let stopwords: Set<String>

    public init(stopwordSet: StopwordSet) {
        self.stopwords = stopwordSet.words
    }

    /// Check if a term is a stopword
    public func isStopword(_ term: String) -> Bool {
        stopwords.contains(term.lowercased())
    }

    /// Filter stopwords from tokens
    public func filter(_ tokens: [(term: String, position: Int)]) -> [(term: String, position: Int)] {
        tokens.filter { !isStopword($0.term) }
    }
}
```

#### FullTextIndexMaintainer Integration

```swift
// Sources/FullTextIndex/FullTextIndexMaintainer.swift (modified)

public struct FullTextIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // ... existing properties ...

    /// Stopword filter (NEW)
    private let stopwordFilter: StopwordFilter

    public init(
        index: Index,
        tokenizer: TokenizationStrategy,
        storePositions: Bool,
        ngramSize: Int,
        minTermLength: Int,
        stopwords: StopwordSet,  // NEW
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        // ... existing initialization ...
        self.stopwordFilter = StopwordFilter(stopwordSet: stopwords)
    }

    /// Tokenize text with stopword filtering
    private func tokenize(_ text: String) -> [(term: String, position: Int)] {
        let rawTokens: [(term: String, position: Int)]

        switch tokenizer {
        case .simple:
            rawTokens = simpleTokenize(text)
        case .stem:
            rawTokens = stemTokenize(text)
        case .ngram:
            rawTokens = ngramTokenize(text)
        case .keyword:
            rawTokens = keywordTokenize(text)
        }

        // Apply stopword filtering (NEW)
        return stopwordFilter.filter(rawTokens)
    }

    /// Search with stopword filtering applied to query
    public func searchWithScores(
        terms: [String],
        matchMode: TextMatchMode = .all,
        bm25Params: BM25Parameters = .default,
        transaction: any TransactionProtocol,
        limit: Int? = nil
    ) async throws -> [(id: Tuple, score: Double)] {
        // Filter query terms (NEW)
        let filteredTerms = terms.filter { !stopwordFilter.isStopword($0) }

        guard !filteredTerms.isEmpty else {
            // All terms were stopwords - return empty or fall back to original terms
            return []
        }

        // Continue with existing search logic using filteredTerms
        // ...
    }
}
```

### Usage

```swift
// Model with English stopword filtering
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var title: String
    var body: String

    #Index<Article>(
        [\.title, \.body],
        type: FullTextIndexKind(
            fields: [\.title, \.body],
            tokenizer: .stem,
            storePositions: true,
            stopwords: .english  // Enable English stopwords
        )
    )
}

// Query "the quick brown fox" becomes "quick brown fox"
let results = try await context.search(Article.self)
    .fullText(\.body)
    .terms(["the", "quick", "brown", "fox"])  // "the" filtered out
    .executeWithScores()
```

### Position Adjustment

When stopwords are filtered during indexing, positions need adjustment to maintain phrase search accuracy:

```swift
/// Tokenize with position adjustment for removed stopwords
private func tokenizeWithAdjustedPositions(_ text: String) -> [(term: String, position: Int)] {
    let rawTokens = rawTokenize(text)

    var adjustedTokens: [(term: String, position: Int)] = []
    var adjustedPosition = 0

    for token in rawTokens {
        if !stopwordFilter.isStopword(token.term) {
            adjustedTokens.append((token.term, adjustedPosition))
            adjustedPosition += 1
        }
        // Stopwords don't increment position - maintains relative distances
    }

    return adjustedTokens
}
```

**Alternative**: Keep original positions but mark stopwords. This preserves exact phrase matching when needed:

```swift
/// Tokenize keeping original positions (recommended for phrase search)
private func tokenizeKeepingPositions(_ text: String) -> [(term: String, position: Int)] {
    let rawTokens = rawTokenize(text)
    return rawTokens.filter { !stopwordFilter.isStopword($0.term) }
    // Original positions preserved - phrase "quick brown" works even if "the" was between them
}
```

---

## Implementation Plan

### Phase 1: Stopword Filtering (Simplest, immediate value)

| Task | Description | Module | Estimate |
|------|-------------|--------|----------|
| 1.1 | Create `StopwordSet` enum with predefined lists | database-kit/FullText | S |
| 1.2 | Add `stopwords` to `FullTextIndexKind` | database-kit/FullText | S |
| 1.3 | Create `StopwordFilter` | database-framework/FullTextIndex | S |
| 1.4 | Integrate into `FullTextIndexMaintainer.tokenize()` | database-framework/FullTextIndex | S |
| 1.5 | Filter query terms in `searchWithScores()` | database-framework/FullTextIndex | S |
| 1.6 | Write unit tests | Tests/FullTextIndexTests | M |

### Phase 2: Vector Quantization (High impact, medium complexity)

| Task | Description | Module | Estimate |
|------|-------------|--------|----------|
| 2.1 | Create `QuantizationConfig` enum | database-kit/Vector | S |
| 2.2 | Create `VectorQuantizer` protocol | database-framework/VectorIndex | S |
| 2.3 | Implement `ProductQuantizer` | database-framework/VectorIndex | L |
| 2.4 | Implement `ScalarQuantizer` | database-framework/VectorIndex | M |
| 2.5 | Implement `BinaryQuantizer` | database-framework/VectorIndex | M |
| 2.6 | Integrate with `HNSWIndexMaintainer` | database-framework/VectorIndex | M |
| 2.7 | Create `VectorQuantizerTrainer` | database-framework/VectorIndex | M |
| 2.8 | Write unit tests | Tests/VectorIndexTests | L |

### Phase 3: BlockMaxWAND (High performance impact)

| Task | Description | Module | Estimate |
|------|-------------|--------|----------|
| 3.1 | Design block storage format | database-framework/FullTextIndex | M |
| 3.2 | Create `PostingListBlock` | database-framework/FullTextIndex | S |
| 3.3 | Implement `BlockMaxWANDSearcher` | database-framework/FullTextIndex | L |
| 3.4 | Modify index building for blocks | database-framework/FullTextIndex | M |
| 3.5 | Integrate with `FullTextQuery` | database-framework/FullTextIndex | M |
| 3.6 | Write unit tests and benchmarks | Tests/FullTextIndexTests | L |

### Phase 4: Multi-Vector Support (Advanced feature)

| Task | Description | Module | Estimate |
|------|-------------|--------|----------|
| 4.1 | Create `MultiVectorConfig` | database-kit/Vector | S |
| 4.2 | Extend `VectorIndexKind` | database-kit/Vector | S |
| 4.3 | Create `MultiVectorSearcher` | database-framework/VectorIndex | L |
| 4.4 | Implement MaxSim scoring | database-framework/VectorIndex | M |
| 4.5 | Modify `VectorIndexMaintainer` for multi-vec | database-framework/VectorIndex | M |
| 4.6 | Extend `VectorQuery` API | database-framework/VectorIndex | M |
| 4.7 | Write unit tests | Tests/VectorIndexTests | L |

**Size estimates**: S = Small (< 1 day), M = Medium (1-3 days), L = Large (3-5 days)

---

## Migration Considerations

### Stopword Filtering
- **Backward compatible**: Default is `StopwordSet.none`
- **Migration**: Rebuild index to apply stopword filtering to existing data

### Vector Quantization
- **Backward compatible**: Default is `QuantizationConfig.none`
- **Migration**: Train quantizer on existing vectors, then rebuild index with quantized codes
- **Incremental**: Can keep both full vectors and quantized codes during transition

### BlockMaxWAND
- **Not backward compatible**: Requires new storage format
- **Migration**: Full index rebuild with block structure

### Multi-Vector
- **Backward compatible**: Existing single-vector indexes unaffected
- **Migration**: N/A (new indexes only)

---

## Performance Expectations

### Stopword Filtering
| Metric | Before | After |
|--------|--------|-------|
| Index size | 100% | 70-85% |
| Query speed | Baseline | 1.1-1.3x faster |
| Precision | Baseline | Improved |

### Vector Quantization
| Method | Memory | Recall@10 | Search Speed |
|--------|--------|-----------|--------------|
| Full (baseline) | 100% | 100% | 1x |
| SQ (8-bit) | 25% | 98-99% | 1.2x |
| PQ (48 subspaces) | 3-6% | 90-95% | 2-4x |
| BQ | 3% | 80-90% | 10-20x |

### BlockMaxWAND
| Query Type | Before (sort all) | After (BMW) |
|------------|------------------|-------------|
| Common terms (1M matches) | O(N) | O(k log N) |
| Selective query (1K matches) | O(N) | ~O(N) |
| Multi-term OR | O(N) | 2-10x speedup |

### Multi-Vector (ColBERT-style)
| Metric | Single Vector | Multi-Vector |
|--------|---------------|--------------|
| Storage per doc | D × 4 bytes | T × D × 4 bytes |
| Search complexity | O(log N) | O(Q × log N) |
| Relevance (MRR) | Baseline | +5-15% |

Where D = dimensions, T = tokens/document, Q = query tokens, N = total documents.

---

## Operational Considerations

### 5.1 Quantizer Training and Lifecycle

#### Training Triggers

| Trigger | Action | Notes |
|---------|--------|-------|
| **Initial build** | Train on sample (10K vectors) | During `OnlineIndexer.rebuild()` |
| **Significant data drift** | Retrain codebook | Manual or scheduled |
| **Accuracy degradation** | Retrain with larger sample | Monitor Recall@k |
| **Schema migration** | Train new codebook | Keep old during transition |

#### Codebook Versioning

```swift
/// Quantizer metadata with versioning
public struct QuantizerMetadata: Codable {
    /// Monotonically increasing version
    let version: Int64

    /// Training timestamp
    let trainedAt: Date

    /// Number of training samples used
    let trainingSampleCount: Int

    /// Configuration used for training
    let config: QuantizationConfig

    /// Training metrics (optional)
    let metrics: TrainingMetrics?
}

public struct TrainingMetrics: Codable {
    /// Reconstruction error (lower is better)
    let reconstructionError: Float

    /// Distortion (quantization error)
    let distortion: Float
}
```

#### Storage Layout with Versioning

```
[indexSubspace]["quantizer"]["metadata"] = QuantizerMetadata
[indexSubspace]["quantizer"]["codebook"]["v1"] = Codebook (current)
[indexSubspace]["quantizer"]["codebook"]["v0"] = Codebook (previous, for rollback)
[indexSubspace]["quantizer"]["codes"]["v1"][primaryKey] = [UInt8]
```

#### Rollback Procedure

```swift
/// Rollback to previous quantizer version
public func rollbackQuantizer(
    to version: Int64,
    transaction: any TransactionProtocol
) async throws {
    // 1. Verify target version exists
    let targetCodebook = try await loadCodebook(version: version, transaction: transaction)

    // 2. Update metadata to point to target version
    let metadata = QuantizerMetadata(version: version, ...)
    try await saveMetadata(metadata, transaction: transaction)

    // 3. Re-encode vectors with target codebook (background job)
    // Old codes remain valid during transition
}
```

#### A/B Rollout for Quantization

```swift
/// Gradual rollout configuration
public struct QuantizerRollout: Sendable {
    /// Percentage of queries using new quantizer (0-100)
    let newQuantizerPercentage: Int

    /// Version to use for new queries
    let newVersion: Int64

    /// Version to use for remaining queries
    let oldVersion: Int64
}

// During search
func search(query: [Float], ...) async throws -> [...] {
    let useNew = rollout.newQuantizerPercentage > Int.random(in: 0..<100)
    let version = useNew ? rollout.newVersion : rollout.oldVersion
    let quantizer = try await loadQuantizer(version: version, ...)
    // ...
}
```

---

### 5.2 Field-Specific Stopword Configuration

#### Per-Field Stopword Settings

```swift
// Sources/FullText/StopwordConfig.swift (database-kit)

/// Stopword configuration with field-level granularity
public struct StopwordConfig: Sendable, Codable, Hashable {
    /// Default stopwords applied to all fields
    public let defaultStopwords: StopwordSet

    /// Field-specific overrides (field name -> stopword set)
    /// - `nil` value means inherit from default
    /// - `.none` value means disable stopwords for this field
    public let fieldOverrides: [String: StopwordSet]

    public static let `default` = StopwordConfig(
        defaultStopwords: .none,
        fieldOverrides: [:]
    )

    /// Get stopwords for a specific field
    public func stopwords(for fieldName: String) -> StopwordSet {
        fieldOverrides[fieldName] ?? defaultStopwords
    }
}

// Usage in FullTextIndexKind
public struct FullTextIndexKind<Root: Persistable>: IndexKind {
    // ...

    /// Stopword configuration (replaces single StopwordSet)
    public let stopwordConfig: StopwordConfig

    public init(
        fields: [PartialKeyPath<Root>],
        tokenizer: TokenizationStrategy = .simple,
        stopwords: StopwordSet = .none,           // Simple API
        stopwordConfig: StopwordConfig? = nil     // Advanced API
    ) {
        // If stopwordConfig provided, use it; otherwise wrap stopwords
        self.stopwordConfig = stopwordConfig ?? StopwordConfig(
            defaultStopwords: stopwords,
            fieldOverrides: [:]
        )
        // ...
    }
}
```

#### Example: Title vs Body Stopwords

```swift
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var title: String   // Keep stopwords (short, every word matters)
    var body: String    // Remove stopwords (long text)

    #Index<Article>(
        [\.title, \.body],
        type: FullTextIndexKind(
            fields: [\.title, \.body],
            tokenizer: .stem,
            stopwordConfig: StopwordConfig(
                defaultStopwords: .english,
                fieldOverrides: [
                    "title": .none  // No stopword removal for title
                ]
            )
        )
    )
}
```

#### Stopword Version Management

```swift
/// Stopword set with version for change tracking
public struct VersionedStopwords: Sendable, Codable {
    /// Version identifier (hash of stopword set)
    public let version: String

    /// Actual stopwords
    public let words: Set<String>

    /// Compute version hash
    public static func computeVersion(_ words: Set<String>) -> String {
        let sorted = words.sorted().joined(separator: ",")
        // Use SHA256 truncated to 8 chars
        return SHA256.hash(data: Data(sorted.utf8))
            .prefix(4)
            .map { String(format: "%02x", $0) }
            .joined()
    }
}

// Store version in index metadata
[indexSubspace]["metadata"]["stopwordVersion"] = "a1b2c3d4"
```

**Reindex Requirement**:
- Changing stopwords requires index rebuild for affected fields
- Version mismatch triggers warning/error on query
- Migration: `OnlineIndexer.rebuildWithStopwords(newConfig:)`

---

### 5.3 Multi-Vector Scoring Integration

#### Score Normalization Strategy

```swift
/// Score normalization for fusion
public enum ScoreNormalization: Sendable {
    /// Min-max normalization to [0, 1]
    case minMax

    /// Z-score normalization
    case zScore

    /// Percentile-based (robust to outliers)
    case percentile

    /// No normalization (raw scores)
    case none
}

/// Late interaction + BM25 hybrid configuration
public struct HybridMultiVectorConfig: Sendable {
    /// Multi-vector (ColBERT) weight
    public let multiVectorWeight: Float  // default: 0.5

    /// BM25 weight
    public let bm25Weight: Float  // default: 0.5

    /// Score normalization method
    public let normalization: ScoreNormalization  // default: .minMax

    /// Number of candidates from each source before fusion
    public let candidatesPerSource: Int  // default: 100

    public static let `default` = HybridMultiVectorConfig(
        multiVectorWeight: 0.5,
        bm25Weight: 0.5,
        normalization: .minMax,
        candidatesPerSource: 100
    )
}
```

#### Hybrid Search Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Hybrid Multi-Vector Search                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Query: "how to cook pasta"                                      │
│         │                                                        │
│         ├──────────────────┬──────────────────┐                 │
│         ▼                  ▼                  │                 │
│  ┌──────────────┐   ┌──────────────┐          │                 │
│  │ Tokenize &   │   │ Full-text    │          │                 │
│  │ Embed Query  │   │ Tokenize     │          │                 │
│  └──────────────┘   └──────────────┘          │                 │
│         │                  │                  │                 │
│         ▼                  ▼                  │                 │
│  ┌──────────────┐   ┌──────────────┐          │                 │
│  │ Multi-Vector │   │ BM25 Search  │          │                 │
│  │ MaxSim       │   │ (BlockMax)   │          │                 │
│  │ Search       │   │              │          │                 │
│  └──────────────┘   └──────────────┘          │                 │
│         │                  │                  │                 │
│         │ top 100          │ top 100          │                 │
│         ▼                  ▼                  │                 │
│  ┌──────────────────────────────────┐         │                 │
│  │         Score Normalization       │         │                 │
│  │  MaxSim: [0.2, 0.8] → [0.0, 1.0] │         │                 │
│  │  BM25:   [1.5, 8.2] → [0.0, 1.0] │         │                 │
│  └──────────────────────────────────┘         │                 │
│                    │                          │                 │
│                    ▼                          │                 │
│  ┌──────────────────────────────────┐         │                 │
│  │     Weighted Score Fusion         │         │                 │
│  │  score = 0.5 × maxsim + 0.5 × bm25│         │                 │
│  └──────────────────────────────────┘         │                 │
│                    │                          │                 │
│                    ▼                          │                 │
│              ┌──────────┐                     │                 │
│              │  Top-k   │                     │                 │
│              │ Results  │                     │                 │
│              └──────────┘                     │                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Caching Strategy for Multi-Vector

```swift
/// Multi-vector search cache
public final class MultiVectorCache: @unchecked Sendable {
    private let cache: LRUCache<CacheKey, CachedResult>

    struct CacheKey: Hashable {
        let queryHash: UInt64      // Hash of query vectors
        let indexVersion: Int64    // Index version for invalidation
        let k: Int
    }

    struct CachedResult {
        let results: [(docId: String, score: Float)]
        let timestamp: Date
        let ttl: TimeInterval
    }

    /// Cache per-query-vector results (intermediate)
    /// Key: single query vector hash
    /// Value: top candidates for that vector
    private let perVectorCache: LRUCache<UInt64, [(docId: String, similarity: Float)]>

    /// Cache configuration
    public struct Config {
        /// Maximum entries in final result cache
        let maxEntries: Int  // default: 1000

        /// TTL for cached results
        let ttl: TimeInterval  // default: 60 seconds

        /// Enable per-vector caching (helps repeated query terms)
        let enablePerVectorCache: Bool  // default: true
    }
}
```

---

### 5.4 BlockMaxWAND Safety and Correctness

#### Upper Bound Selection

```swift
/// Upper bound computation strategy
public enum UpperBoundStrategy: Sendable {
    /// Block-level max (tighter bound, more pruning)
    /// Uses max TF within block + pessimistic doc length
    case blockMax

    /// Document-level max (looser bound, safer)
    /// Uses actual document scores (requires more I/O)
    case docMax

    /// Hybrid: use blockMax with safety margin
    case blockMaxWithMargin(margin: Float)  // default margin: 1.001
}
```

#### Floating-Point Safety

```swift
/// Safe floating-point comparison for threshold checks
extension Float {
    /// Compare with epsilon tolerance
    func isGreaterThan(_ other: Float, epsilon: Float = 1e-6) -> Bool {
        return self > other + epsilon
    }

    /// Safe upper bound comparison (conservative)
    func upperBoundExceeds(_ threshold: Float) -> Bool {
        // Add small margin to avoid floating-point edge cases
        return self > threshold * 0.9999
    }
}

/// BlockMaxWAND threshold update
func updateThreshold(
    heap: inout BoundedMinHeap<ScoredDoc>,
    newScore: Float
) {
    heap.insert(ScoredDoc(score: newScore, ...))

    if heap.isFull {
        // Use slightly lower threshold to be conservative
        // Ensures we don't miss documents due to FP errors
        threshold = heap.min!.score * 0.9999
    }
}
```

#### Heap Type Definition

```swift
/// Bounded min-heap for top-k tracking
/// - Maintains k highest scores
/// - Min element = threshold (lowest score in top-k)
public struct BoundedMinHeap<Element: Comparable>: Sendable {
    private var heap: [Element]
    public let capacity: Int

    public var isFull: Bool { heap.count >= capacity }
    public var min: Element? { heap.first }

    public mutating func insert(_ element: Element) {
        if heap.count < capacity {
            heap.append(element)
            siftUp(heap.count - 1)
        } else if element > heap[0] {
            heap[0] = element
            siftDown(0)
        }
    }

    /// Extract all elements sorted descending
    public func sortedDescending() -> [Element] {
        heap.sorted(by: >)
    }
}

/// Scored document for heap
public struct ScoredDoc: Comparable, Sendable {
    public let docId: Tuple
    public let score: Float

    public static func < (lhs: ScoredDoc, rhs: ScoredDoc) -> Bool {
        lhs.score < rhs.score  // Min-heap by score
    }
}
```

#### Correctness Test Plan

```swift
// Tests/FullTextIndexTests/BlockMaxWANDTests.swift

@Suite("BlockMaxWAND Correctness")
struct BlockMaxWANDCorrectnessTests {

    /// Verify BMW returns same results as exhaustive search
    @Test func testExactnessVsExhaustive() async throws {
        let corpus = generateTestCorpus(docs: 10000, avgTerms: 100)
        let queries = generateQueries(count: 100)

        for query in queries {
            let bmwResults = try await bmwSearch(query, k: 10)
            let exhaustiveResults = try await exhaustiveSearch(query, k: 10)

            // Must return identical top-k (order may differ for ties)
            #expect(Set(bmwResults.map(\.docId)) == Set(exhaustiveResults.map(\.docId)))

            // Scores must match within epsilon
            for (bmw, exh) in zip(bmwResults, exhaustiveResults) {
                #expect(abs(bmw.score - exh.score) < 1e-5)
            }
        }
    }

    /// Test with adversarial score distributions
    @Test func testAdversarialScores() async throws {
        // Case 1: All documents have same score
        // Case 2: One document has much higher score
        // Case 3: Scores clustered near threshold
        // ...
    }

    /// Test floating-point edge cases
    @Test func testFloatingPointEdgeCases() async throws {
        // Very small scores
        // Very large scores
        // Scores differing by epsilon
        // Subnormal numbers
    }

    /// Test block boundary conditions
    @Test func testBlockBoundaries() async throws {
        // Document at block boundary
        // Block with single document
        // Empty blocks (all docs deleted)
    }
}
```

---

### 5.5 FDB Storage Layout and Size Limits

#### Size Limits and Chunking Strategy

| Data Type | Typical Size | FDB Limit | Strategy |
|-----------|--------------|-----------|----------|
| PQ Code | 48-96 bytes | 10KB (key) | Single value |
| PQ Codebook | 48×256×8×4 = 384KB | 100KB (value) | Chunked |
| Posting Block | 64 docs × ~20 bytes = 1.3KB | 100KB (value) | Single value |
| Multi-Vector | 256 vecs × 128 dims × 4 = 128KB | 100KB (value) | Chunked per doc |

#### Codebook Chunking

```swift
/// Chunk large codebook for FDB storage
public struct ChunkedCodebook {
    static let maxChunkSize = 90_000  // Leave margin under 100KB

    /// Save codebook with chunking
    func save(
        codebook: [[[Float]]],  // [M][K][subDim]
        subspace: Subspace,
        transaction: any TransactionProtocol
    ) throws {
        let encoded = try JSONEncoder().encode(codebook)

        if encoded.count <= Self.maxChunkSize {
            // Single chunk
            let key = subspace.pack(Tuple("codebook", Int64(0)))
            transaction.setValue(encoded, for: key)
            transaction.setValue(
                Tuple(Int64(1)).pack(),
                for: subspace.pack(Tuple("codebook_chunks"))
            )
        } else {
            // Multiple chunks
            let chunks = encoded.chunked(size: Self.maxChunkSize)
            for (i, chunk) in chunks.enumerated() {
                let key = subspace.pack(Tuple("codebook", Int64(i)))
                transaction.setValue(Data(chunk), for: key)
            }
            transaction.setValue(
                Tuple(Int64(chunks.count)).pack(),
                for: subspace.pack(Tuple("codebook_chunks"))
            )
        }
    }

    /// Load codebook with dechunking
    func load(
        subspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [[[Float]]] {
        let chunkCountKey = subspace.pack(Tuple("codebook_chunks"))
        guard let countData = try await transaction.getValue(for: chunkCountKey, snapshot: true),
              let count = try Tuple.unpack(from: countData).first as? Int64 else {
            throw QuantizerError.codebookNotFound
        }

        var data = Data()
        for i in 0..<count {
            let key = subspace.pack(Tuple("codebook", Int64(i)))
            guard let chunk = try await transaction.getValue(for: key, snapshot: true) else {
                throw QuantizerError.codebookCorrupted
            }
            data.append(chunk)
        }

        return try JSONDecoder().decode([[[Float]]].self, from: data)
    }
}
```

#### Subspace Layout

```
[typeSubspace]/
├── [indexName]/
│   ├── R/[id]                              # Raw items (existing)
│   │
│   ├── vector/                             # Vector index
│   │   ├── hnsw/                           # HNSW graph
│   │   │   ├── nodes/[id]                  # Node metadata
│   │   │   ├── neighbors/[id]/[level]      # Neighbor lists
│   │   │   └── entry                       # Entry point
│   │   │
│   │   ├── quantizer/                      # Quantization (NEW)
│   │   │   ├── metadata                    # QuantizerMetadata
│   │   │   ├── codebook/[chunk_id]         # Codebook chunks
│   │   │   ├── codebook_chunks             # Chunk count
│   │   │   └── codes/[id]                  # Compressed codes
│   │   │
│   │   └── multivec/                       # Multi-vector (NEW)
│   │       ├── doc/[id]/count              # Vector count per doc
│   │       ├── doc/[id]/vectors/[chunk]    # Vectors (chunked if >100KB)
│   │       └── flat/[id]/[vecIdx]          # Flattened for search
│   │
│   └── fulltext/                           # Full-text index
│       ├── terms/[term]/                   # Current structure
│       │   └── [docId]
│       │
│       ├── blocks/[term]/                  # BlockMaxWAND (NEW)
│       │   ├── meta                        # Block count, config
│       │   └── [blockId]                   # BlockMetadata + postings
│       │
│       ├── docs/[docId]                    # Document metadata
│       ├── stats/                          # BM25 statistics
│       │   ├── N
│       │   ├── totalLength
│       │   └── stopwordVersion             # (NEW)
│       └── df/[term]                       # Document frequencies
```

#### Write Batching and Transaction Retry

```swift
/// Batch writer for quantized codes
public struct QuantizedCodeBatchWriter {
    private let batchSize: Int  // default: 500 (to stay under 10MB transaction limit)
    private let retryConfig: RetryConfig

    struct RetryConfig {
        let maxRetries: Int  // default: 5
        let initialBackoff: Duration  // default: 10ms
        let maxBackoff: Duration  // default: 1s
        let backoffMultiplier: Double  // default: 2.0
    }

    /// Write codes in batches with retry
    func writeCodes(
        codes: [(id: Tuple, code: [UInt8])],
        subspace: Subspace,
        database: any DatabaseProtocol
    ) async throws {
        for batch in codes.chunked(size: batchSize) {
            try await withRetry(config: retryConfig) {
                try await database.withTransaction { transaction in
                    for (id, code) in batch {
                        let key = subspace.subspace("codes").pack(id)
                        transaction.setValue(Data(code), for: key)
                    }
                }
            }
        }
    }

    /// Retry with exponential backoff
    private func withRetry<T>(
        config: RetryConfig,
        operation: () async throws -> T
    ) async throws -> T {
        var lastError: Error?
        var backoff = config.initialBackoff

        for attempt in 0..<config.maxRetries {
            do {
                return try await operation()
            } catch let error as FDBError where error.isRetryable {
                lastError = error
                try await Task.sleep(for: backoff)
                backoff = min(backoff * config.backoffMultiplier, config.maxBackoff)
            }
        }

        throw lastError ?? QuantizerError.writeFailed
    }
}
```

---

### 5.6 Evaluation Plan

#### Quality Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| **Recall@k** | Fraction of true top-k in retrieved top-k | >95% for SQ, >90% for PQ |
| **MRR** | Mean Reciprocal Rank | Minimal degradation (<5%) |
| **NDCG@k** | Normalized Discounted Cumulative Gain | >0.9 relative to baseline |

#### Performance Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| **QPS** | Queries per second | 2x baseline (with PQ) |
| **p50 Latency** | Median query latency | <50ms |
| **p99 Latency** | 99th percentile latency | <200ms |
| **Memory** | Index memory footprint | <25% baseline (with PQ) |

#### Evaluation Framework

```swift
/// Benchmark configuration
public struct BenchmarkConfig {
    /// Dataset size
    let documentCount: Int  // e.g., 100K, 1M

    /// Vector dimensions
    let dimensions: Int  // e.g., 384, 768

    /// Query count for evaluation
    let queryCount: Int  // e.g., 1000

    /// k values to evaluate
    let kValues: [Int]  // e.g., [1, 10, 100]

    /// Baseline to compare against
    let baseline: BaselineConfig
}

public enum BaselineConfig {
    case exactSearch           // Brute-force flat search
    case unquantizedHNSW       // HNSW without quantization
    case standardBM25          // BM25 without BMW
}

/// Benchmark results
public struct BenchmarkResults {
    /// Quality metrics per k
    let recallAtK: [Int: Double]
    let mrrAtK: [Int: Double]
    let ndcgAtK: [Int: Double]

    /// Performance metrics
    let qps: Double
    let latencyP50Ms: Double
    let latencyP99Ms: Double
    let memoryMB: Double

    /// Comparison to baseline
    let relativeRecall: Double      // recall / baseline_recall
    let relativeQPS: Double         // qps / baseline_qps
    let relativeMemory: Double      // memory / baseline_memory
}
```

#### Benchmark Test Suite

```swift
// Tests/BenchmarkTests/QuantizationBenchmarks.swift

@Suite("Quantization Benchmarks")
struct QuantizationBenchmarks {

    @Test func benchmarkPQVsBaseline() async throws {
        let config = BenchmarkConfig(
            documentCount: 100_000,
            dimensions: 384,
            queryCount: 1000,
            kValues: [1, 10, 100],
            baseline: .unquantizedHNSW
        )

        let baseline = try await runBaseline(config)
        let pqResults = try await runWithPQ(config, pqConfig: .default)

        // Quality assertions
        for k in config.kValues {
            #expect(pqResults.recallAtK[k]! > 0.90,
                    "PQ Recall@\(k) should be >90%")
        }

        // Performance assertions
        #expect(pqResults.relativeMemory < 0.10,
                "PQ should use <10% memory")
        #expect(pqResults.relativeQPS > 2.0,
                "PQ should be >2x faster")

        // Log detailed results
        print("""
        PQ Benchmark Results:
        - Recall@10: \(pqResults.recallAtK[10]!)
        - Memory: \(pqResults.memoryMB)MB (\(pqResults.relativeMemory)x baseline)
        - QPS: \(pqResults.qps) (\(pqResults.relativeQPS)x baseline)
        - p99 Latency: \(pqResults.latencyP99Ms)ms
        """)
    }

    @Test func benchmarkBlockMaxWAND() async throws {
        let config = BenchmarkConfig(
            documentCount: 1_000_000,
            dimensions: 0,  // N/A for text
            queryCount: 1000,
            kValues: [10, 100],
            baseline: .standardBM25
        )

        let baseline = try await runBM25Baseline(config)
        let bmwResults = try await runWithBMW(config)

        // Exactness assertion
        for k in config.kValues {
            #expect(bmwResults.recallAtK[k]! == 1.0,
                    "BMW must return exact same results as baseline")
        }

        // Performance assertion
        #expect(bmwResults.relativeQPS > 2.0,
                "BMW should be >2x faster for common queries")
    }
}
```

---

### 5.7 Selection Guidelines

#### When to Use Each Quantization Method

```
                    ┌─────────────────────────────────────────┐
                    │          Decision Tree                   │
                    └─────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    │     Memory constrained?           │
                    └─────────────────┬─────────────────┘
                           │                    │
                          No                   Yes
                           │                    │
                           ▼                    ▼
                    ┌──────────┐     ┌─────────────────────┐
                    │   None   │     │  Accuracy critical? │
                    │(baseline)│     └─────────┬───────────┘
                    └──────────┘          │           │
                                         Yes          No
                                          │           │
                                          ▼           ▼
                                    ┌─────────┐ ┌─────────────┐
                                    │   SQ    │ │ Dataset >10M?│
                                    │ (4x)    │ └──────┬──────┘
                                    └─────────┘    │        │
                                                  Yes       No
                                                   │        │
                                                   ▼        ▼
                                             ┌─────────┐ ┌─────────┐
                                             │   BQ    │ │   PQ    │
                                             │ (32x)   │ │ (4-32x) │
                                             └─────────┘ └─────────┘
```

#### Recommended Configurations

| Scenario | Method | Config | Expected Quality |
|----------|--------|--------|------------------|
| **General (< 1M docs)** | None | - | 100% |
| **Memory-sensitive (< 1M)** | SQ | 8-bit | 98-99% Recall |
| **Large scale (1-10M)** | PQ | M=48, K=256 | 92-95% Recall |
| **Very large (> 10M)** | BQ + rescore | factor=4 | 90-95% Recall |
| **Real-time + quality** | SQ | 8-bit | 98-99% Recall |

#### Dimension-Based PQ Configuration

| Dimensions | Recommended M | subDim | Code Size |
|------------|---------------|--------|-----------|
| 128 | 16 | 8 | 16 bytes |
| 256 | 32 | 8 | 32 bytes |
| 384 | 48 | 8 | 48 bytes |
| 512 | 64 | 8 | 64 bytes |
| 768 | 96 | 8 | 96 bytes |
| 1024 | 128 | 8 | 128 bytes |
| 1536 | 192 | 8 | 192 bytes |

**Rule**: `M = dimensions / 8` is a good starting point (8-dim subvectors)

---

## 6. Additional Specifications (Review Feedback)

This section addresses additional specifications requested during design review.

### 6.1 Multi-Vector Storage Format Diagram

The following diagram illustrates the storage layout for multi-vector documents (e.g., ColBERT-style token embeddings):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Multi-Vector Storage Format                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Document: "How to cook pasta" (4 token embeddings, 128 dims each)          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  [indexSubspace]/multivec/doc/[docId]/                               │    │
│  │  ├── count                        → Int64(4)                         │    │
│  │  └── vectors/                                                        │    │
│  │      ├── [chunk_0]  (if total size ≤ 100KB)                         │    │
│  │      │   └── [[128 floats], [128 floats], [128 floats], [128 floats]]│    │
│  │      │                                                               │    │
│  │      └── [chunk_0], [chunk_1], ... (if total size > 100KB)          │    │
│  │          Each chunk ≤ 90KB                                           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  [indexSubspace]/multivec/flat/[docId]/[vecIdx]                      │    │
│  │  For brute-force / HNSW search                                       │    │
│  │                                                                       │    │
│  │  ├── [docId]/0  → [128 floats] ("how")                               │    │
│  │  ├── [docId]/1  → [128 floats] ("to")                                │    │
│  │  ├── [docId]/2  → [128 floats] ("cook")                              │    │
│  │  └── [docId]/3  → [128 floats] ("pasta")                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Constraints:                                                                │
│  ├── maxVectorsPerDoc: 512 (configurable)                                   │
│  ├── Single vector size: dimensions × 4 bytes (e.g., 128 × 4 = 512 bytes)  │
│  ├── Max doc vectors size: 512 × 512 = 256KB → chunked into 3 parts        │
│  └── FDB value limit: 100KB → chunk threshold: 90KB (with margin)           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Key Encoding Format

```swift
/// Multi-vector key structure
enum MultiVectorKey {
    /// Document-level metadata
    /// Key: [subspace]["multivec"]["doc"][docId]["count"]
    /// Value: Int64 (number of vectors)
    case count(docId: Tuple)

    /// Chunked vector storage (for documents with many vectors)
    /// Key: [subspace]["multivec"]["doc"][docId]["vectors"][chunkId]
    /// Value: [[Float]] encoded as Protobuf or MessagePack
    case vectorChunk(docId: Tuple, chunkId: Int64)

    /// Flattened index for search
    /// Key: [subspace]["multivec"]["flat"][docId][vecIdx]
    /// Value: [Float] (single vector)
    case flatVector(docId: Tuple, vecIdx: Int64)
}
```

### 6.2 Reciprocal Rank Fusion (RRF)

Add RRF as an additional fusion method for hybrid search:

```swift
/// Extended score normalization with RRF
public enum ScoreNormalization: Sendable {
    /// Min-max normalization to [0, 1]
    case minMax

    /// Z-score normalization
    case zScore

    /// Percentile-based (robust to outliers)
    case percentile

    /// Reciprocal Rank Fusion (RRF)
    /// Reference: Cormack et al., "Reciprocal Rank Fusion outperforms
    ///            Condorcet and individual Rank Learning Methods", SIGIR 2009
    /// Formula: RRF(d) = Σ 1/(k + rank_i(d))
    /// - k: smoothing constant (default: 60)
    case reciprocalRankFusion(k: Int = 60)

    /// No normalization (raw scores)
    case none
}

/// RRF implementation
public struct ReciprocalRankFusion {
    private let k: Int

    public init(k: Int = 60) {
        self.k = k
    }

    /// Fuse multiple ranked lists using RRF
    /// - Parameter rankedLists: Array of ranked document lists (docId, score)
    /// - Returns: Fused ranking sorted by RRF score descending
    public func fuse(
        rankedLists: [[(docId: String, score: Float)]]
    ) -> [(docId: String, rrfScore: Float)] {
        var rrfScores: [String: Float] = [:]

        for rankedList in rankedLists {
            for (rank, (docId, _)) in rankedList.enumerated() {
                // RRF formula: 1 / (k + rank)
                // rank is 0-indexed, so add 1 for 1-indexed rank
                let contribution = 1.0 / Float(k + rank + 1)
                rrfScores[docId, default: 0] += contribution
            }
        }

        return rrfScores
            .map { (docId: $0.key, rrfScore: $0.value) }
            .sorted { $0.rrfScore > $1.rrfScore }
    }
}
```

#### When to Use RRF vs Weighted Fusion

| Method | Best For | Pros | Cons |
|--------|----------|------|------|
| **Weighted Fusion** | Known score distributions | Tunable weights | Requires score calibration |
| **RRF (k=60)** | Unknown/incomparable scores | Score-agnostic, robust | Ignores score magnitudes |
| **Min-Max** | Bounded score ranges | Simple, intuitive | Sensitive to outliers |

### 6.3 Stopword Change Impact Matrix

Clarify when reindexing is required after stopword configuration changes:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                   Stopword Change Impact Matrix                             │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Change Type              │ Reindex Required? │ Workaround                 │
│  ─────────────────────────┼───────────────────┼────────────────────────────│
│  Add stopwords            │ ❌ No             │ Filter at query time       │
│  (e.g., add "the")        │                   │ Index still works          │
│  ─────────────────────────┼───────────────────┼────────────────────────────│
│  Remove stopwords         │ ✅ Yes            │ No workaround              │
│  (e.g., remove "the")     │                   │ Terms not in index         │
│  ─────────────────────────┼───────────────────┼────────────────────────────│
│  Change default set       │ ⚠️ Partial        │ Depends on direction       │
│  (english → german)       │                   │ (add-only: no, else: yes)  │
│  ─────────────────────────┼───────────────────┼────────────────────────────│
│  Add field override       │ ❌ No             │ Query-time filter          │
│  (body: .english)         │                   │                            │
│  ─────────────────────────┼───────────────────┼────────────────────────────│
│  Remove field override    │ ⚠️ Maybe          │ If more restrictive: no    │
│  (body: .none → default)  │                   │ If less restrictive: yes   │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

#### Automatic Reindex Detection

```swift
/// Detect if reindex is required for stopword change
public struct StopwordChangeAnalyzer {
    /// Analyze change impact
    public func analyzeChange(
        oldConfig: StopwordConfig,
        newConfig: StopwordConfig,
        fields: [String]
    ) -> ChangeImpact {
        var requiresReindex: [String] = []
        var queryTimeOnly: [String] = []

        for field in fields {
            let oldStopwords = oldConfig.stopwords(for: field).words
            let newStopwords = newConfig.stopwords(for: field).words

            let removed = oldStopwords.subtracting(newStopwords)
            let added = newStopwords.subtracting(oldStopwords)

            if !removed.isEmpty {
                // Removed stopwords = terms missing from index
                requiresReindex.append(field)
            } else if !added.isEmpty {
                // Added stopwords = can filter at query time
                queryTimeOnly.append(field)
            }
        }

        return ChangeImpact(
            requiresReindex: requiresReindex,
            queryTimeOnly: queryTimeOnly
        )
    }
}

public struct ChangeImpact {
    /// Fields that require full reindex
    public let requiresReindex: [String]

    /// Fields that can be handled at query time
    public let queryTimeOnly: [String]

    /// Whether any reindex is needed
    public var needsReindex: Bool { !requiresReindex.isEmpty }
}
```

### 6.4 Incremental Quantizer Re-training

Define workflow for updating quantizers without full rebuild:

```swift
/// Incremental re-training strategy
public enum RetrainingStrategy: Sendable {
    /// Full retrain on all data
    case full

    /// Incremental update with new samples
    /// Reference: Based on online k-means concepts
    case incremental(
        /// New samples to incorporate
        newSampleWeight: Float,  // 0.0-1.0, how much weight for new samples
        /// Minimum samples before triggering retrain
        minNewSamples: Int
    )

    /// Decay-based update (older samples less important)
    case decayBased(
        /// Decay factor per time window
        decayFactor: Float  // e.g., 0.9 = 10% decay per window
    )
}

/// Quantizer re-training workflow
public struct QuantizerRetrainer {
    let strategy: RetrainingStrategy

    /// Check if retraining is needed
    public func shouldRetrain(
        currentMetrics: TrainingMetrics,
        recentQueryPerformance: QueryPerformance
    ) -> RetrainDecision {
        // Trigger conditions:
        // 1. Recall degradation > 5%
        // 2. Reconstruction error increased > 20%
        // 3. Manual trigger
        // 4. Scheduled (e.g., weekly)

        if recentQueryPerformance.recallAt10 < 0.90 {
            return .required(reason: .recallDegradation)
        }

        if currentMetrics.reconstructionError > baselineError * 1.2 {
            return .required(reason: .errorIncrease)
        }

        return .notNeeded
    }

    /// Execute retraining with zero-downtime
    public func retrain(
        container: FDBContainer,
        indexName: String,
        sampleSize: Int
    ) async throws {
        // 1. Sample vectors from current index
        let samples = try await sampleVectors(count: sampleSize)

        // 2. Train new quantizer version
        let newQuantizer = try await trainQuantizer(samples: samples)

        // 3. Store new codebook with version bump
        let newVersion = try await storeNewCodebook(newQuantizer)

        // 4. Begin dual-write period (old + new codes)
        try await enableDualWrite(oldVersion: currentVersion, newVersion: newVersion)

        // 5. Background re-encode existing vectors
        let reencoder = BackgroundReencoder(
            batchSize: 1000,
            throttle: .adaptive
        )
        try await reencoder.reencode(
            from: currentVersion,
            to: newVersion
        )

        // 6. Switch to new version
        try await switchActiveVersion(to: newVersion)

        // 7. Cleanup old version (optional, keep for rollback)
        // try await deleteOldCodebook(version: oldVersion)
    }
}

public enum RetrainDecision {
    case required(reason: RetrainReason)
    case notNeeded
    case scheduled(date: Date)
}

public enum RetrainReason {
    case recallDegradation
    case errorIncrease
    case manual
    case scheduled
    case dataDrift
}
```

#### Monitoring Dashboard Metrics

```swift
/// Quantizer health metrics for monitoring
public struct QuantizerHealthMetrics: Codable {
    /// Current codebook version
    let version: Int64

    /// Training timestamp
    let trainedAt: Date

    /// Days since last training
    var daysSinceTraining: Int {
        Calendar.current.dateComponents([.day], from: trainedAt, to: Date()).day ?? 0
    }

    /// Recent query performance
    let recallAt10: Double
    let recallAt100: Double

    /// Reconstruction error trend
    let reconstructionError: Double
    let reconstructionErrorBaseline: Double
    let reconstructionErrorTrend: Trend  // .stable, .increasing, .decreasing

    /// Alert thresholds
    public func alerts() -> [Alert] {
        var alerts: [Alert] = []

        if recallAt10 < 0.90 {
            alerts.append(.critical("Recall@10 below 90%: \(recallAt10)"))
        }

        if daysSinceTraining > 30 {
            alerts.append(.warning("Codebook not retrained in \(daysSinceTraining) days"))
        }

        if reconstructionErrorTrend == .increasing {
            alerts.append(.warning("Reconstruction error trending up"))
        }

        return alerts
    }
}
```

---

## Summary of Review Feedback Addressed

| Review Point | Section | Status |
|--------------|---------|--------|
| Codebook versioning and rollout | 5.1 | ✅ Complete |
| A/B rollout for quantization | 5.1 | ✅ Complete |
| Field-specific stopwords | 5.2 | ✅ Complete |
| Stopword version management | 5.2 + 6.3 | ✅ Enhanced |
| Multi-vector + BM25 hybrid scoring | 5.3 | ✅ Complete |
| RRF scoring method | 6.2 | ✅ Added |
| BlockMaxWAND safety | 5.4 | ✅ Complete |
| Floating-point handling | 5.4 | ✅ Complete |
| Test plan for correctness | 5.4 | ✅ Complete |
| FDB size limits and chunking | 5.5 | ✅ Complete |
| Subspace layout diagram | 5.5 | ✅ Complete |
| Write batching and retry | 5.5 | ✅ Complete |
| Evaluation metrics and benchmarks | 5.6 | ✅ Complete |
| PQ/SQ/BQ selection guidelines | 5.7 | ✅ Complete |
| Multi-vector storage format diagram | 6.1 | ✅ Added |
| Stopword reindex requirements | 6.3 | ✅ Added |
| Incremental re-training workflow | 6.4 | ✅ Added |
