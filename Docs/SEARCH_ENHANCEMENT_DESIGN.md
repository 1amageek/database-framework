# Search Enhancement Design

This document describes the design for advanced search features:
1. **BM25 Scoring** - Full-text search ranking
2. **ACORN Filtered Search** - Predicate-aware vector search
3. **Fusion Query** - Multi-source result combination (future)

## SPM Module Architecture

```
database-kit (client-safe, no FDB dependency)
└── Core, Vector, FullText, etc.  (IndexKind definitions)

database-framework (server-only, FDB dependency)
├── DatabaseEngine                 # Core engine, no index-specific knowledge
│   ├── IndexQueryContext          # Generic query operations
│   └── IndexMaintainer            # Protocol only
│
├── FullTextIndex                  # Depends on: DatabaseEngine
│   ├── FullTextIndexMaintainer    # + BM25 statistics, scored search
│   ├── FullTextQuery              # + executeWithScores(), bm25()
│   ├── BM25Parameters             # NEW
│   └── BM25Scorer                 # NEW
│
├── VectorIndex                    # Depends on: DatabaseEngine
│   ├── HNSWIndexMaintainer        # + searchWithFilter()
│   ├── VectorQuery                # + filter(), acorn()
│   └── ACORNParameters            # NEW
│
└── Database                       # Re-exports all modules
```

**Key Constraint**: `DatabaseEngine` cannot depend on any index module.
All feature-specific code (BM25, ACORN) must reside in the respective index module.

## References

- **BM25**: Robertson & Zaragoza, "The Probabilistic Relevance Framework: BM25 and Beyond", Foundations and Trends in Information Retrieval, 2009
- **ACORN**: Patel et al., "ACORN: Performant and Predicate-Agnostic Search Over Vector Embeddings and Structured Data", SIGMOD 2024 ([arXiv:2403.04871](https://arxiv.org/abs/2403.04871))
- **RRF**: Cormack et al., "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods", SIGIR 2009

## Quick Start

### BM25 Scored Search

```swift
import FullTextIndex

// Search with BM25 ranking
let ranked = try await context.search(Article.self)
    .fullText(\.content)
    .terms(["swift", "concurrency"])
    .bm25(k1: 1.2, b: 0.75)  // Optional: custom parameters
    .executeWithScores()
// Returns: [(item: Article, score: Double)] sorted by BM25 score
```

### ACORN Filtered Vector Search

```swift
import VectorIndex

// Filtered similarity search using ACORN algorithm
let results = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 384)
    .query(queryVector, k: 10)
    .filter { product in
        product.category == "electronics" && product.price < 1000
    }
    .acorn(expansionFactor: 3)  // Optional: higher = better recall
    .execute()
// Returns: [(item: Product, distance: Double)] - only matching items

// KeyPath equality filter (convenience)
let results = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 384)
    .query(queryVector, k: 10)
    .filter(\.category, equals: "electronics")
    .execute()
```

**Note**: ACORN filtering requires HNSW index. Configure with `VectorIndexConfiguration`:

```swift
let container = try FDBContainer(
    for: schema,
    configuration: FDBConfiguration(
        indexConfigurations: [
            VectorIndexConfiguration<Product>(
                keyPath: \.embedding,
                algorithm: .hnsw(.default)
            )
        ]
    )
)
```

---

## 1. BM25 Scoring

### Overview

BM25 (Best Matching 25) is a probabilistic ranking function for full-text search. It improves upon TF-IDF by incorporating document length normalization.

### Formula

```
BM25(D, Q) = Σ IDF(qi) × (tf(qi, D) × (k1 + 1)) / (tf(qi, D) + k1 × (1 - b + b × |D|/avgDL))
```

Where:
- `tf(qi, D)`: Term frequency of query term qi in document D
- `IDF(qi)`: `log((N - df(qi) + 0.5) / (df(qi) + 0.5))`
- `N`: Total number of documents
- `df(qi)`: Number of documents containing term qi
- `|D|`: Document length (token count)
- `avgDL`: Average document length
- `k1`: Term frequency saturation parameter (default: 1.2)
- `b`: Document length normalization parameter (default: 0.75)

**Note on IDF**: The standard BM25 IDF formula can produce **negative values** when
`df > N/2` (term appears in more than half of documents). This is intentional:
very common terms carry less information and should not boost scores. Some
implementations add `+1` inside the log to force non-negative IDF, but this
over-ranks common terms. We use the standard formula for accurate ranking.

### Storage Layout

#### Current Structure
```
[indexSubspace]["terms"][term][docId] = Tuple(positions...)
[indexSubspace]["docs"][docId] = Tuple(termCount)
```

#### Extended Structure (BM25)
```
[indexSubspace]["terms"][term][docId] = Tuple(positions...)
[indexSubspace]["docs"][docId] = Tuple(termCount, docLength)    # docLength added
[indexSubspace]["stats"]["N"] = Int64                            # total document count
[indexSubspace]["stats"]["totalLength"] = Int64                  # sum of all doc lengths
[indexSubspace]["df"][term] = Int64                              # document frequency per term
```

#### Atomic Operations

All statistics use FDB atomic operations for concurrent safety:
- `N`: `atomicOp(.add, ±1)`
- `totalLength`: `atomicOp(.add, ±docLength)`
- `df[term]`: `atomicOp(.add, ±1)`

### File Structure

**Important**: All BM25-related code resides in the `FullTextIndex` module.
`DatabaseEngine` has no knowledge of BM25 (SPM dependency constraint).

```
Sources/FullTextIndex/              # Depends on: DatabaseEngine
├── FullTextIndexMaintainer.swift   # Modified: maintain BM25 statistics, scored search
├── FullTextQuery.swift             # Modified: add executeWithScores(), bm25()
├── FullTextIndexKind+Maintainable.swift  # Unchanged
├── BM25Parameters.swift            # New: k1, b parameters
└── BM25Scorer.swift                # New: BM25 calculation logic

Sources/DatabaseEngine/             # No changes for BM25
└── QueryPlanner/
    └── IndexQueryContext.swift     # Provides generic methods only:
                                    # - withTransaction()
                                    # - indexSubspace(for:)
                                    # - fetchItems()
```

### API Design

#### BM25Parameters

```swift
/// BM25 scoring parameters
public struct BM25Parameters: Sendable, Codable {
    /// Term frequency saturation (1.2 - 2.0, default: 1.2)
    public let k1: Double

    /// Document length normalization (0.0 - 1.0, default: 0.75)
    /// - 0.0: No normalization (longer docs may rank higher)
    /// - 1.0: Full normalization (strongly penalizes long docs)
    public let b: Double

    public static let `default` = BM25Parameters(k1: 1.2, b: 0.75)

    public init(k1: Double = 1.2, b: Double = 0.75) {
        precondition(k1 > 0, "k1 must be positive")
        precondition(b >= 0 && b <= 1, "b must be in [0, 1]")
        self.k1 = k1
        self.b = b
    }
}
```

#### BM25Scorer

```swift
/// BM25 scoring calculator
public struct BM25Scorer: Sendable {
    private let params: BM25Parameters
    private let N: Int64
    private let avgDL: Double

    public init(params: BM25Parameters, totalDocs: Int64, avgDocLength: Double) {
        self.params = params
        self.N = totalDocs
        self.avgDL = avgDocLength
    }

    /// Calculate IDF for a term
    ///
    /// Uses standard BM25 IDF formula: log((N - df + 0.5) / (df + 0.5))
    /// Note: Returns negative value when df > N/2 (term in majority of docs)
    public func idf(documentFrequency df: Int64) -> Double {
        let numerator = Double(N) - Double(df) + 0.5
        let denominator = Double(df) + 0.5
        return log(numerator / denominator)
    }

    /// Calculate BM25 score for a document
    public func score(
        termFrequencies: [String: Int],
        documentFrequencies: [String: Int64],
        docLength: Int
    ) -> Double {
        var totalScore = 0.0

        for (term, tf) in termFrequencies {
            guard let df = documentFrequencies[term] else { continue }

            let idfValue = idf(documentFrequency: df)
            let tfNorm = (Double(tf) * (params.k1 + 1)) /
                (Double(tf) + params.k1 * (1 - params.b + params.b * Double(docLength) / avgDL))

            totalScore += idfValue * tfNorm
        }

        return totalScore
    }
}
```

#### Query API

```swift
// Existing API (unchanged)
let articles = try await context.search(Article.self)
    .fullText(\.content)
    .terms(["swift", "concurrency"])
    .execute()  // Returns [Article]

// New: With BM25 scores
let rankedArticles = try await context.search(Article.self)
    .fullText(\.content)
    .terms(["swift", "concurrency"])
    .executeWithScores()  // Returns [(item: Article, score: Double)]

// With custom parameters
let rankedArticles = try await context.search(Article.self)
    .fullText(\.content)
    .terms(["swift", "concurrency"])
    .bm25(k1: 1.5, b: 0.8)
    .executeWithScores()
```

#### Query Builder Implementation

The query builder uses `IndexQueryContext` for generic operations, but calls
`FullTextIndexMaintainer` directly for BM25-specific logic (within the module).

```swift
// FullTextIndex/FullTextQuery.swift
public struct FullTextQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var searchTerms: [String] = []
    private var bm25Params: BM25Parameters = .default

    /// Set BM25 parameters
    public func bm25(k1: Double = 1.2, b: Double = 0.75) -> Self {
        var copy = self
        copy.bm25Params = BM25Parameters(k1: k1, b: b)
        return copy
    }

    /// Execute with BM25 scores
    public func executeWithScores() async throws -> [(item: T, score: Double)] {
        guard !searchTerms.isEmpty else { return [] }

        let indexName = buildIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Use generic withTransaction from IndexQueryContext
        return try await queryContext.withTransaction { transaction in
            // Create maintainer (FullTextIndex module internal)
            let maintainer = FullTextIndexMaintainer<T>(
                index: ...,
                subspace: indexSubspace,
                ...
            )

            // Call BM25-specific search (within FullTextIndex module)
            let scoredResults = try await maintainer.searchWithScores(
                terms: self.searchTerms,
                bm25Params: self.bm25Params,
                transaction: transaction
            )

            // Fetch items
            let items = try await self.queryContext.fetchItems(
                ids: scoredResults.map(\.id),
                type: T.self
            )

            // Combine items with scores
            return self.combineWithScores(items: items, scoredResults: scoredResults)
        }
    }
}
```

### Implementation Notes

#### Index Maintenance Changes

```swift
extension FullTextIndexMaintainer {
    func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionProtocol) async throws {
        // Remove old item statistics
        if let oldItem = oldItem {
            let oldTokens = tokenize(extractText(from: oldItem))
            let oldDocLength = oldTokens.count
            let oldTerms = Set(oldTokens.map(\.term))

            // Decrement N
            transaction.atomicOp(key: statsNKey, param: int64Bytes(-1), mutationType: .add)

            // Decrement totalLength
            transaction.atomicOp(key: statsTotalLengthKey, param: int64Bytes(-Int64(oldDocLength)), mutationType: .add)

            // Decrement df for each unique term
            for term in oldTerms {
                let dfKey = dfSubspace.pack(Tuple(term))
                transaction.atomicOp(key: dfKey, param: int64Bytes(-1), mutationType: .add)
            }

            // Remove term entries and doc metadata (existing logic)
            // ...
        }

        // Add new item statistics
        if let newItem = newItem {
            let newId = try DataAccess.extractId(from: newItem, using: idExpression)
            let newTokens = tokenize(extractText(from: newItem))
            let newDocLength = newTokens.count  // Total token count (for avgDL)

            // Group tokens by term to get unique terms and term frequencies
            var termPositions: [String: [Int]] = [:]
            for token in newTokens {
                termPositions[token.term, default: []].append(token.position)
            }
            let uniqueTermCount = termPositions.count  // Unique term count
            let uniqueTerms = Set(termPositions.keys)

            // Increment N (total document count)
            transaction.atomicOp(key: statsNKey, param: int64Bytes(1), mutationType: .add)

            // Increment totalLength (sum of all document lengths)
            transaction.atomicOp(key: statsTotalLengthKey, param: int64Bytes(Int64(newDocLength)), mutationType: .add)

            // Increment df for each unique term
            for term in uniqueTerms {
                let dfKey = dfSubspace.pack(Tuple(term))
                transaction.atomicOp(key: dfKey, param: int64Bytes(1), mutationType: .add)
            }

            // Add term entries (existing logic)
            // ...

            // Store doc metadata: (uniqueTermCount, docLength)
            // - uniqueTermCount: number of distinct terms in this document
            // - docLength: total number of tokens (for length normalization)
            let docKey = docsSubspace.pack(newId)
            let docValue = Tuple(Int64(uniqueTermCount), Int64(newDocLength)).pack()
            transaction.setValue(docValue, for: docKey)
        }
    }
}
```

---

## 2. ACORN Filtered Search

### Overview

ACORN (Approximate Containment Queries Over Real-Value Navigable Networks) enables efficient filtered vector search over HNSW graphs without requiring predicate-specific index structures.

### Key Concepts

#### Traditional Approaches (Problems)

1. **Pre-filtering**: Filter first, then vector search
   - Problem: May eliminate relevant vectors, poor recall when filter is selective

2. **Post-filtering**: Vector search first, then filter
   - Problem: May need to retrieve many more than k results to find k matches

#### ACORN Solution: Predicate Subgraph Traversal

During HNSW graph traversal, dynamically filter neighbors that don't match the predicate. This emulates searching on a predicate-specific "oracle partition" without actually building one.

### Strategies

| Strategy | Construction | Search | Use Case |
|----------|--------------|--------|----------|
| **ACORN-γ** | Store M×γ neighbors | Filter during search | High QPS, larger index |
| **ACORN-1** | Standard HNSW | Expand ef during search | Lower TTI, flexible |

### Implementation: ACORN-1

We implement ACORN-1 because:
- No changes to existing HNSW index structure
- Works with any predicate at query time
- Simpler migration path

#### Algorithm

```
function ACORN_SEARCH(query_vector, k, predicate, ef_base, expansion_factor):
    ef = ef_base * expansion_factor  // Expand search space
    entry_point = get_entry_point()

    // Top-down traversal (unchanged, no predicate filtering at upper layers)
    for level in max_level..1:
        entry_point = greedy_search(query_vector, entry_point, level)

    // Bottom layer search with predicate
    candidates = MinHeap()       // For graph traversal (all nodes)
    results = MaxHeap(capacity: ef)  // Only predicate-matching items
    visited = Set()

    // Initialize with entry point
    visited.add(entry_point)
    candidates.push(entry_point, distance(query_vector, entry_point.vector))

    // Evaluate predicate on entry point too
    if predicate(entry_point):
        results.push(entry_point, distance(query_vector, entry_point.vector))

    while not candidates.empty():
        current = candidates.pop_min()

        if results.size == k and current.distance > results.max_distance:
            break

        for neighbor in get_neighbors(current, level=0):
            if neighbor in visited:
                continue
            visited.add(neighbor)

            // ACORN: Evaluate predicate
            passes_predicate = predicate(neighbor)

            dist = distance(query_vector, neighbor.vector)

            // Always add to candidates for graph connectivity
            // (even non-matching nodes help reach matching nodes)
            if results.size < k or dist < results.max_distance:
                candidates.push(neighbor, dist)

                // Only add to results if predicate passes
                if passes_predicate:
                    results.push(neighbor, dist)

    return results.to_sorted_list()[:k]
```

**Key Design Decision**: Non-matching nodes are still added to the candidate queue
to maintain graph connectivity. This allows the algorithm to traverse through
non-matching regions to reach matching nodes that may be nearby in vector space
but not directly connected through matching nodes.

### File Structure

**Important**: All ACORN-related code resides in the `VectorIndex` module.
`DatabaseEngine` has no knowledge of ACORN (SPM dependency constraint).

```
Sources/VectorIndex/                    # Depends on: DatabaseEngine
├── HNSWIndexMaintainer.swift           # Modified: add searchWithFilter(), searchLayerWithFilter()
├── VectorQuery.swift                   # Modified: add .filter(), .acorn() API
├── VectorIndexKind+Maintainable.swift  # Unchanged
├── ACORNParameters.swift               # New: expansion factor, max evaluations
└── FlatVectorIndexMaintainer.swift     # Unchanged (ACORN is HNSW-specific)

Sources/DatabaseEngine/                 # No changes for ACORN
└── QueryPlanner/
    └── IndexQueryContext.swift         # Provides generic methods only
```

### API Design

#### Basic Filter (Closure)

```swift
let results = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 384)
    .query(queryVector, k: 10)
    .filter { product in
        product.category == "electronics" && product.price < 1000
    }
    .execute()
```

#### Type-Safe Filter (KeyPath)

```swift
let results = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 384)
    .query(queryVector, k: 10)
    .filter(\.category, equals: "electronics")
    .filter(\.inStock, equals: true)
    .execute()
```

#### ACORN Parameters

```swift
/// ACORN search parameters
public struct ACORNParameters: Sendable {
    /// ef expansion factor (default: 2)
    /// Higher values improve recall but increase latency
    public let expansionFactor: Int

    /// Maximum items to evaluate predicates on (default: unlimited)
    /// Useful for expensive predicates
    public let maxPredicateEvaluations: Int?

    public static let `default` = ACORNParameters(expansionFactor: 2)

    public init(expansionFactor: Int = 2, maxPredicateEvaluations: Int? = nil) {
        precondition(expansionFactor >= 1, "expansionFactor must be >= 1")
        self.expansionFactor = expansionFactor
        self.maxPredicateEvaluations = maxPredicateEvaluations
    }
}
```

### Predicate Evaluation Strategies

```swift
/// Strategy for evaluating predicates during ACORN search
public enum PredicateStrategy<Item: Persistable>: Sendable {
    /// Fetch item and evaluate closure (most flexible, higher latency)
    case fetchAndEvaluate(@Sendable (Item) async throws -> Bool)

    /// Use ScalarIndex for equality check (fast, limited expressiveness)
    case scalarIndex(indexName: String, fieldName: String, value: any TupleElement & Sendable)

    /// Use BitmapIndex for set membership (fast for high-cardinality)
    case bitmapIndex(indexName: String, value: any TupleElement & Sendable)

    /// Combine multiple strategies with AND
    case and([PredicateStrategy<Item>])

    /// Combine multiple strategies with OR
    case or([PredicateStrategy<Item>])
}
```

#### Query Builder Implementation

The query builder uses `IndexQueryContext` for generic operations, but calls
`HNSWIndexMaintainer` directly for ACORN-specific logic (within the module).

```swift
// VectorIndex/VectorQuery.swift
public struct VectorQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private var queryVector: [Float]?
    private var k: Int = 10
    private var filterPredicate: (@Sendable (T) async throws -> Bool)?
    private var acornParams: ACORNParameters = .default

    /// Add filter predicate (enables ACORN search)
    public func filter(_ predicate: @escaping @Sendable (T) -> Bool) -> Self {
        var copy = self
        copy.filterPredicate = { item in predicate(item) }
        return copy
    }

    /// Add KeyPath equality filter (convenience)
    public func filter<V: Equatable & Sendable>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> Self {
        filter { item in item[keyPath: keyPath] == value }
    }

    /// Set ACORN parameters
    public func acorn(expansionFactor: Int = 2) -> Self {
        var copy = self
        copy.acornParams = ACORNParameters(expansionFactor: expansionFactor)
        return copy
    }

    /// Execute search
    public func execute() async throws -> [(item: T, distance: Double)] {
        guard let vector = queryVector else {
            throw VectorQueryError.noQueryVector
        }

        let indexName = buildIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            // Create HNSW maintainer (VectorIndex module internal)
            let maintainer = HNSWIndexMaintainer<T>(
                index: ...,
                subspace: indexSubspace,
                ...
            )

            let results: [(primaryKey: [any TupleElement], distance: Double)]

            if let predicate = self.filterPredicate {
                // ACORN filtered search
                results = try await maintainer.searchWithFilter(
                    queryVector: vector,
                    k: self.k,
                    predicate: predicate,
                    acornParams: self.acornParams,
                    transaction: transaction
                )
            } else {
                // Standard HNSW search
                results = try await maintainer.search(
                    queryVector: vector,
                    k: self.k,
                    transaction: transaction
                )
            }

            // Fetch items and combine with distances
            return try await self.fetchAndCombine(results: results)
        }
    }
}
```

### Implementation Notes

#### HNSWIndexMaintainer Extension

```swift
extension HNSWIndexMaintainer {
    /// Search with predicate filter (ACORN-1)
    public func searchWithFilter(
        queryVector: [Float],
        k: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        acornParams: ACORNParameters = .default,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        // Expand ef based on ACORN parameters
        let expandedEf = max(k, searchParams.ef) * acornParams.expansionFactor

        guard let entryPointPK = try await getEntryPoint(transaction: transaction) else {
            return []
        }

        let entryMetadata = try await getNodeMetadata(primaryKey: entryPointPK, transaction: transaction)!
        let currentLevel = entryMetadata.level
        let vectorCache = VectorCache()

        // Phase 1: Greedy descent (unchanged)
        var entryPoints = [entryPointPK]
        for level in stride(from: currentLevel, through: 1, by: -1) {
            let nearest = try await searchLayer(
                queryVector: queryVector,
                entryPoints: entryPoints,
                ef: 1,
                level: level,
                transaction: transaction,
                cache: vectorCache
            )
            entryPoints = [nearest[0].primaryKey]
        }

        // Phase 2: Filtered search at layer 0
        let candidates = try await searchLayerWithFilter(
            queryVector: queryVector,
            entryPoints: entryPoints,
            ef: expandedEf,
            level: 0,
            predicate: predicate,
            acornParams: acornParams,
            transaction: transaction,
            cache: vectorCache
        )

        return candidates.prefix(k).compactMap { candidate in
            guard let elements = try? Tuple.unpack(from: candidate.primaryKey.pack()) else {
                return nil
            }
            return (primaryKey: elements, distance: candidate.distance)
        }
    }

    /// Search layer with predicate evaluation
    private func searchLayerWithFilter(
        queryVector: [Float],
        entryPoints: [Tuple],
        ef: Int,
        level: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        acornParams: ACORNParameters,
        transaction: any TransactionProtocol,
        cache: VectorCache?
    ) async throws -> [(primaryKey: Tuple, distance: Double)] {
        var visited = Set<Tuple>()
        var candidates = CandidateHeap<Tuple>()
        var result = ResultHeap<Tuple>(k: ef)
        var predicateEvaluations = 0

        // Initialize with entry points
        // Entry points are evaluated for predicate to:
        // 1. Include matching entry points in results
        // 2. Still use all entry points to seed traversal (for graph connectivity)
        for entryPK in entryPoints {
            visited.insert(entryPK)

            let entryVector = try await loadVectorCached(primaryKey: entryPK, transaction: transaction, cache: cache)
            let distance = calculateDistance(queryVector, entryVector)

            // Always add to candidates (to seed graph traversal)
            candidates.insert((primaryKey: entryPK, distance: distance))

            // Evaluate predicate for entry points too
            if let item = try await fetchItemForPredicate(entryPK, transaction: transaction) {
                predicateEvaluations += 1
                let passes = try await predicate(item)

                // Only add to results if predicate passes
                if passes {
                    result.insert((primaryKey: entryPK, distance: distance))
                }
            }
        }

        while !candidates.isEmpty {
            guard let current = candidates.pop() else { break }

            if result.isFull, let worst = result.worst, current.distance > worst.distance {
                break
            }

            let neighbors = try await getNeighbors(
                primaryKey: current.primaryKey,
                level: level,
                transaction: transaction
            )

            for neighborPK in neighbors {
                if visited.contains(neighborPK) { continue }
                visited.insert(neighborPK)

                guard let neighborVector = try? await loadVectorCached(
                    primaryKey: neighborPK,
                    transaction: transaction,
                    cache: cache
                ) else { continue }

                let distance = calculateDistance(queryVector, neighborVector)

                // Always add to candidates for graph connectivity
                // (even non-matching nodes help reach matching nodes)
                let shouldExplore = !result.isFull || (result.worst.map { distance < $0.distance } ?? true)
                if shouldExplore {
                    candidates.insert((primaryKey: neighborPK, distance: distance))
                }

                // ACORN: Predicate evaluation for results
                if let maxEvals = acornParams.maxPredicateEvaluations,
                   predicateEvaluations >= maxEvals {
                    continue  // Skip predicate eval if budget exhausted (but still explored)
                }

                // Fetch item and evaluate predicate
                if let item = try await fetchItemForPredicate(neighborPK, transaction: transaction) {
                    predicateEvaluations += 1
                    let passes = try await predicate(item)

                    // Only add to results if predicate passes
                    if passes && shouldExplore {
                        result.insert((primaryKey: neighborPK, distance: distance))
                    }
                }
            }
        }

        return result.toSortedArray()
    }
}
```

---

## 3. Fusion Query (Future)

### Overview

Fusion combines results from multiple search sources (Vector, FullText, Spatial, etc.) into a unified ranked list. This is deferred but designed to integrate with the existing architecture.

### Architecture

```
DatabaseEngine (core)
├── Fusion/
│   ├── FusionProtocol.swift      # Protocol for fusionable queries
│   ├── FusionAlgorithm.swift     # RRF, RSF algorithms
│   ├── FusionQueryBuilder.swift  # Query builder
│   └── FusionResult.swift        # Result types
```

### Protocol Design

```swift
/// Result entry for fusion
public struct FusionEntry: Sendable {
    public let id: Tuple
    public let score: Double  // Normalized [0, 1] preferred
}

/// Protocol for queries that can participate in fusion
public protocol FusionCompatibleQuery<Item>: Sendable {
    associatedtype Item: Persistable

    /// Execute and return scored results for fusion
    func executeForFusion() async throws -> [FusionEntry]

    /// Source identifier for debugging/logging
    var sourceIdentifier: String { get }

    /// Query context for item fetching
    var queryContext: IndexQueryContext { get }
}
```

### Algorithms

#### Reciprocal Rank Fusion (RRF)

```swift
/// RRF: rank-based fusion (score-agnostic)
/// score(d) = Σ 1/(k + rank_i(d)) for each source i
public static func rrf(sources: [[FusionEntry]], k: Int = 60) -> [FusionEntry]
```

#### Relative Score Fusion (RSF)

```swift
/// RSF: score-based fusion with normalization
/// score(d) = Σ weight_i × normalize(score_i(d))
public static func rsf(sources: [[FusionEntry]], weights: [Double]? = nil) -> [FusionEntry]
```

### Usage (Future)

```swift
// Combine any FusionCompatibleQuery sources
let results = try await context.fuse(Product.self)
    .add(
        context.findSimilar(Product.self)
            .vector(\.embedding, dimensions: 384)
            .query(queryVector, k: 100)
    )
    .add(
        context.search(Product.self)
            .fullText(\.description)
            .terms(["organic", "coffee"])
    )
    .add(
        context.nearby(Product.self)
            .spatial(\.location)
            .within(radius: 10.km, of: userLocation)
    )
    .algorithm(.rrf(k: 60))
    .limit(10)
    .execute()
```

### Index Conformance (Future)

Each index module will conform to `FusionCompatibleQuery`:

```swift
// VectorIndex
extension VectorQueryBuilder: FusionCompatibleQuery {
    public var sourceIdentifier: String { "vector" }

    public func executeForFusion() async throws -> [FusionEntry] {
        let results = try await execute()
        // Convert distance to score (smaller distance = higher score)
        guard let maxDist = results.map(\.distance).max(), maxDist > 0 else {
            return results.map { FusionEntry(id: Tuple($0.item.id), score: 1.0) }
        }
        return results.map { result in
            FusionEntry(id: Tuple(result.item.id), score: 1.0 - result.distance / maxDist)
        }
    }
}

// FullTextIndex
extension FullTextQueryBuilder: FusionCompatibleQuery {
    public var sourceIdentifier: String { "fulltext" }

    public func executeForFusion() async throws -> [FusionEntry] {
        let results = try await executeWithScores()
        return results.map { FusionEntry(id: Tuple($0.item.id), score: $0.score) }
    }
}
```

---

## Implementation Plan

### SPM Module Constraints

- `DatabaseEngine` cannot depend on `FullTextIndex` or `VectorIndex`
- All feature-specific code must reside in the respective index module
- `IndexQueryContext` provides only generic operations (no BM25/ACORN knowledge)

### Phase 1: BM25 (FullTextIndex module only) - ✅ COMPLETED

| Task | Description | File | Status |
|------|-------------|------|--------|
| 1.1 | Create BM25 parameters struct | `HighlightConfig.swift` | ✅ |
| 1.2 | Create BM25 scorer | `BM25Scorer.swift` | ✅ |
| 1.3 | Add BM25 subspaces (stats, df) | `FullTextIndexMaintainer.swift` | ✅ |
| 1.4 | Update `updateIndex()` for BM25 statistics | `FullTextIndexMaintainer.swift` | ✅ |
| 1.5 | Add `searchWithScores()` method | `FullTextIndexMaintainer.swift` | ✅ |
| 1.6 | Add `getBM25Statistics()` method | `FullTextIndexMaintainer.swift` | ✅ |
| 1.7 | Add `.bm25()` builder method | `FullTextQuery.swift` | ✅ |
| 1.8 | Add `executeWithScores()` method | `FullTextQuery.swift` | ✅ |
| 1.9 | Write unit tests | `FullTextIndexTests/` | ⬜ |

### Phase 2: ACORN (VectorIndex module only) - ✅ COMPLETED

| Task | Description | File | Status |
|------|-------------|------|--------|
| 2.1 | Create ACORN parameters struct | `ACORNParameters.swift` | ✅ |
| 2.2 | Add `searchWithFilter()` public method | `HNSWIndexMaintainer.swift` | ✅ |
| 2.3 | Add `searchLayerWithFilter()` private method | `HNSWIndexMaintainer.swift` | ✅ |
| 2.4 | Use fetchItem callback (not internal helper) | `HNSWIndexMaintainer.swift` | ✅ |
| 2.5 | Add `.filter()` builder method | `VectorQuery.swift` | ✅ |
| 2.6 | Add `.acorn()` builder method | `VectorQuery.swift` | ✅ |
| 2.7 | Update `execute()` for filtered search | `VectorQuery.swift` | ✅ |
| 2.8 | Write unit tests | `VectorIndexTests/` | ⬜ |

### Phase 3: Fusion (Future)

| Task | Description |
|------|-------------|
| 3.1 | Create `FusionProtocol.swift` in DatabaseEngine |
| 3.2 | Create `FusionAlgorithm.swift` (RRF, RSF) |
| 3.3 | Create `FusionQueryBuilder.swift` |
| 3.4 | Add `fuse()` to FDBContext |
| 3.5 | Add `FusionCompatibleQuery` conformance to VectorQueryBuilder |
| 3.6 | Add `FusionCompatibleQuery` conformance to FullTextQueryBuilder |
| 3.7 | Add conformance to other indexes (Spatial, Rank, etc.) |
| 3.8 | Write unit tests |

---

## Migration Considerations

### BM25 Storage Migration

For existing FullTextIndex data, BM25 statistics need to be computed:

```swift
// OnlineIndexer can rebuild BM25 statistics
let indexer = OnlineIndexer(container: container)
try await indexer.rebuildBM25Statistics(for: Article.self, indexName: "Article_fulltext_content")
```

The rebuilder will:
1. Scan all documents in the index
2. Compute N, totalLength, df for each term
3. Update doc metadata with docLength

### ACORN Compatibility

ACORN-1 requires no index structure changes. It works with existing HNSW indexes immediately.

---

## Performance Characteristics

### BM25

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Score calculation | O(q × t) | q = query terms, t = terms in doc |
| Statistics lookup | O(q) | One df lookup per query term |
| Additional storage | O(V) | V = vocabulary size (df entries) |

### ACORN

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Search (unfiltered) | O(log n × M × ef) | Standard HNSW |
| Search (filtered) | O(log n × M × ef × γ) | γ = expansion factor |
| Predicate evaluation | O(1) per candidate | May involve item fetch |

### Fusion

| Operation | Complexity | Notes |
|-----------|------------|-------|
| RRF | O(s × n) | s = sources, n = results per source |
| RSF | O(s × n) | Same, with normalization |
| Item fetch | O(k) | k = final result count |
