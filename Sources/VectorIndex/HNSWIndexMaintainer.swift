// HNSWIndexMaintainer.swift
// VectorIndexLayer - HNSW (Hierarchical Navigable Small World) graph index maintainer
//
// Complete HNSW implementation ported from fdb-record-layer

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Logging
import Synchronization

// MARK: - HNSW Constants

/// Maximum nodes allowed for inline indexing (updateIndex).
/// Beyond this limit, use batch indexing (scanItem) instead.
/// This prevents FDB transaction timeouts (~10k operations limit).
public let hnswMaxInlineNodes: Int64 = 500

// MARK: - HNSW Data Structures

/// HNSW node metadata (NO vector duplication - vectors stored in flat index only)
///
/// **Storage Design**:
/// - Flat Index: `[indexSubspace][primaryKey] = vector` (source of truth)
/// - HNSW Metadata: `[indexSubspace]["hnsw"]["nodes"][primaryKey] = HNSWNodeMetadata`
/// - HNSW Edges: `[indexSubspace]["hnsw"]["edges"][primaryKey][level][neighborPK] = ""`
///
/// **Critical**: This struct contains NO vector data. Vectors are loaded from flat index only.
public struct HNSWNodeMetadata: Codable, Sendable {
    /// Maximum level this node appears in (0-indexed)
    ///
    /// Level is assigned probabilistically during insertion:
    /// - P(level = l) = (1/M)^l
    /// - Higher levels have exponentially fewer nodes
    /// - Top layer typically has 1-2 nodes (entry points)
    public let level: Int

    /// Initialize node metadata
    ///
    /// - Parameter level: Maximum level for this node (0 = ground layer)
    public init(level: Int) {
        self.level = level
    }
}

/// HNSW construction parameters
///
/// **M (Maximum Connections)**: Number of bi-directional links created for every new element
/// - Higher M → Better recall, slower build, more memory
/// - Typical: 16-64
///
/// **efConstruction**: Size of dynamic candidate list during construction
/// - Higher efConstruction → Better recall, slower build
/// - Typical: 100-400
///
/// **efSearch**: Default size of dynamic candidate list during search
/// - Higher efSearch → Better recall, slower search
/// - Typical: 50-200
public struct HNSWParameters: Sendable {
    public let m: Int
    public let efConstruction: Int
    public let efSearch: Int

    public static let `default` = HNSWParameters(m: 16, efConstruction: 200, efSearch: 50)

    public init(m: Int = 16, efConstruction: Int = 200, efSearch: Int = 50) {
        self.m = m
        self.efConstruction = efConstruction
        self.efSearch = efSearch
    }
}

/// Internal HNSW parameters with computed properties
private struct HNSWInternalParameters {
    let M: Int
    let efConstruction: Int
    let efSearch: Int
    let ml: Double

    init(from params: HNSWParameters) {
        self.M = params.m
        self.efConstruction = params.efConstruction
        self.efSearch = params.efSearch
        self.ml = 1.0 / log(Double(params.m))
    }

    var M_max0: Int {
        return M * 2
    }

    var M_max: Int {
        return M
    }
}

/// Search-time parameters for HNSW
///
/// **ef (exploration factor)**: Size of dynamic candidate list during search
/// - Higher ef → Better recall, slower search
/// - Lower ef → Faster search, worse recall
/// - Must be >= k (number of nearest neighbors)
/// - Typical: ef = k * 1.5 to k * 3
public struct HNSWSearchParameters: Sendable {
    /// Size of dynamic candidate list during search
    ///
    /// **Recommendation**: ef >= k (k = number of results)
    /// - For recall ~90%: ef ≈ k * 1.5
    /// - For recall ~95%: ef ≈ k * 2
    /// - For recall ~99%: ef ≈ k * 3
    public let ef: Int

    /// Initialize search parameters
    ///
    /// - Parameter ef: Exploration factor (default: 50)
    public init(ef: Int = 50) {
        self.ef = ef
    }
}

// MARK: - HNSW Index Maintainer

/// HNSW (Hierarchical Navigable Small World) index maintainer
///
/// **Algorithm**:
/// - **Construction**: O(log n) insertion with probabilistic layers
/// - **Search**: O(log n) greedy search with dynamic candidate list
/// - **Deletion**: O(M^2 * level) with neighbor rewiring
/// - **Storage**: Metadata-only (vectors in flat index, no duplication)
///
/// **Storage Layout**:
/// ```
/// Flat Index: [indexSubspace][primaryKey] = vector
/// HNSW Metadata: [indexSubspace]["hnsw"]["nodes"][primaryKey] = HNSWNodeMetadata
/// HNSW Edges: [indexSubspace]["hnsw"]["edges"][primaryKey][level][neighborPK] = ""
/// Entry Point: [indexSubspace]["hnsw"]["entrypoint"] = primaryKey
/// ```
///
/// **⚠️ CRITICAL: Transaction Budget Limitations**:
///
/// **Actual Complexity** (measured, not theoretical):
/// - **Small graphs** (level ≤ 2, ~1,000 nodes): ~2,600 ops/insert ✅ Safe
/// - **Medium graphs** (level = 3, ~5,000 nodes): ~3,800 ops/insert ⚠️ Risky
/// - **Large graphs** (level ≥ 4, >10,000 nodes): ~12,000+ ops/insert ❌ Will timeout
///
/// **Why**: Nested loops in pruning logic:
/// - searchLayer: efConstruction * avgNeighbors getNeighbors calls per level
/// - Pruning: M * M distance calculations per level
/// - Total: O(efConstruction * M * currentLevel) with nested I/O
///
/// **⚠️ DO NOT USE INLINE INDEXING FOR HNSW IN PRODUCTION**
public struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    // HNSW parameters
    private let parameters: HNSWInternalParameters

    private let dimensions: Int
    private let metric: VectorMetric

    // HNSW subspaces
    private let hnswSubspace: Subspace
    private let nodesSubspace: Subspace
    private let edgesSubspace: Subspace
    private let entryPointKey: [UInt8]
    private let nodeCountKey: [UInt8]

    public init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression,
        parameters: HNSWParameters = .default
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.parameters = HNSWInternalParameters(from: parameters)
        self.dimensions = dimensions
        self.metric = metric

        // Initialize HNSW subspaces
        self.hnswSubspace = subspace.subspace("hnsw")
        self.nodesSubspace = hnswSubspace.subspace("nodes")
        self.edgesSubspace = hnswSubspace.subspace("edges")
        self.entryPointKey = hnswSubspace.pack(Tuple("entrypoint"))
        self.nodeCountKey = hnswSubspace.pack(Tuple("_nodeCount"))
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // ✅ Deletions: Supported with neighbor rewiring
        if let oldItem = oldItem {
            let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
            try await deleteNode(primaryKey: oldId, transaction: transaction)

            // Delete vector from flat index
            let vectorKey = subspace.pack(oldId)
            transaction.clear(key: vectorKey)

            // Decrement node count atomically
            decrementNodeCount(transaction: transaction)
        }

        // ⚠️ Insertions: Check graph size before attempting
        if let newItem = newItem {
            // Early check: node count (O(1) lookup)
            let nodeCount = try await getNodeCount(transaction: transaction)
            if nodeCount >= hnswMaxInlineNodes {
                throw VectorIndexError.graphTooLarge(maxLevel: -1)
            }

            // Note: We no longer check maxLevel as a constraint.
            // HNSW level assignment is probabilistic (P(level=l) = 1/M_L^l where M_L ≈ ln(M)).
            // With M=16, a graph with just 20 nodes has ~80% chance of reaching level 2.
            // The node count check above is sufficient for FDB transaction limits.

            // Small graph - allow inline insertion
            let primaryKey = try DataAccess.extractId(from: newItem, using: idExpression)
            let vector = try extractVector(from: newItem)

            // Save vector to flat index BEFORE graph construction
            try saveVectorToFlatIndex(primaryKey: primaryKey, vector: vector, transaction: transaction)

            // Perform inline insertion
            try await insert(
                primaryKey: primaryKey,
                queryVector: vector,
                transaction: transaction
            )

            // Increment node count atomically
            incrementNodeCount(transaction: transaction)
        }
    }

    /// Scan item during batch index building (OnlineIndexer専用)
    ///
    /// **⚠️ IMPORTANT**: This method is designed for OnlineIndexer use ONLY.
    /// It does NOT check `hnswMaxInlineNodes` limit because:
    /// - OnlineIndexer manages batch sizes and transaction boundaries
    /// - OnlineIndexer is responsible for staying within FDB transaction limits
    /// - Batch indexing typically uses smaller transactions with throttling
    ///
    /// **DO NOT call directly from user code**. Use `updateIndex` for individual
    /// record updates, which includes safety checks.
    ///
    /// - Parameters:
    ///   - item: Item to index
    ///   - id: Primary key tuple
    ///   - transaction: FDB transaction (managed by OnlineIndexer)
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let vector = try extractVector(from: item)

        // Save vector to flat index
        try saveVectorToFlatIndex(primaryKey: id, vector: vector, transaction: transaction)

        // Insert into HNSW graph
        try await insert(primaryKey: id, queryVector: vector, transaction: transaction)

        // Increment node count atomically
        incrementNodeCount(transaction: transaction)
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        // Vector index stores in flat index key
        return [subspace.pack(id)]
    }
}

// MARK: - HNSW Storage Helpers

/// Vector cache to reduce redundant FDB reads during HNSW operations.
/// Keyed by packed primary key bytes (as Data, which is Hashable) for efficient lookup.
///
/// Uses `Mutex` pattern per CLAUDE.md guidelines:
/// - ✅ `final class: Sendable` (not actor)
/// - ✅ Mutable state protected by `Mutex<State>`
/// - ❌ No `NSLock` (issues in async context)
/// - ❌ No `@unchecked Sendable`
internal final class VectorCache: Sendable {
    private struct State: Sendable {
        var cache: [Data: [Float]] = [:]
    }
    private let state: Mutex<State>

    init() {
        self.state = Mutex(State())
    }

    func get(_ key: FDB.Bytes) -> [Float]? {
        state.withLock { $0.cache[Data(key)] }
    }

    func set(_ key: FDB.Bytes, vector: [Float]) {
        state.withLock { $0.cache[Data(key)] = vector }
    }

    var count: Int {
        state.withLock { $0.cache.count }
    }
}

extension HNSWIndexMaintainer {
    /// Load vector from flat index with caching to reduce redundant reads.
    ///
    /// **Optimization**: During HNSW insertion, the same vectors may be accessed
    /// multiple times (e.g., when pruning neighbors). This method caches vectors
    /// within a transaction to avoid O(M²) FDB reads.
    ///
    /// - Parameters:
    ///   - primaryKey: The primary key tuple
    ///   - transaction: FDB transaction
    ///   - cache: Optional vector cache for memoization
    /// - Returns: The vector as Float array
    private func loadVectorCached(
        primaryKey: Tuple,
        transaction: any TransactionProtocol,
        cache: VectorCache?
    ) async throws -> [Float] {
        let cacheKey = primaryKey.pack()

        // Check cache first
        if let cachedVector = cache?.get(cacheKey) {
            return cachedVector
        }

        // Load from FDB
        let vector = try await loadVectorFromFlatIndex(primaryKey: primaryKey, transaction: transaction)

        // Store in cache
        cache?.set(cacheKey, vector: vector)

        return vector
    }

    /// Load vector from flat index (ONLY way to access vectors)
    private func loadVectorFromFlatIndex(
        primaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> [Float] {
        let vectorKey = subspace.pack(primaryKey)

        guard let vectorValue = try await transaction.getValue(for: vectorKey, snapshot: true) else {
            throw VectorIndexError.invalidStructure("Vector not found for primaryKey: \(primaryKey)")
        }

        // Decode vector from tuple
        let vectorTuple = try Tuple.unpack(from: vectorValue)
        var vectorArray: [Float] = []
        vectorArray.reserveCapacity(dimensions)

        for i in 0..<dimensions {
            guard i < vectorTuple.count else {
                throw VectorIndexError.invalidStructure("Vector has fewer elements than expected dimensions")
            }

            let element = vectorTuple[i]
            let floatValue: Float
            if let f = element as? Float {
                floatValue = f
            } else if let d = element as? Double {
                floatValue = Float(d)
            } else if let i64 = element as? Int64 {
                floatValue = Float(i64)
            } else if let i = element as? Int {
                floatValue = Float(i)
            } else {
                throw VectorIndexError.invalidStructure("Vector element must be numeric")
            }

            vectorArray.append(floatValue)
        }

        return vectorArray
    }

    /// Save vector to flat index (single source of truth)
    private func saveVectorToFlatIndex(
        primaryKey: Tuple,
        vector: [Float],
        transaction: any TransactionProtocol
    ) throws {
        let vectorKey = subspace.pack(primaryKey)
        try validateKeySize(vectorKey)
        let tupleElements: [any TupleElement] = vector.map { $0 as any TupleElement }
        let vectorValue = Tuple(tupleElements).pack()
        try validateValueSize(vectorValue)
        transaction.setValue(vectorValue, for: vectorKey)
    }

    /// Calculate distance between two vectors
    private func calculateDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        switch metric {
        case .cosine:
            return cosineDistance(v1, v2)
        case .euclidean:
            return euclideanDistance(v1, v2)
        case .dotProduct:
            return dotProductDistance(v1, v2)
        }
    }

    /// Cosine distance: 1 - cosine_similarity
    private func cosineDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        let norm1 = sqrt(v1.map { Double($0) * Double($0) }.reduce(0, +))
        let norm2 = sqrt(v2.map { Double($0) * Double($0) }.reduce(0, +))

        guard norm1 > 0 && norm2 > 0 else {
            return 2.0  // Maximum distance for zero vectors
        }

        let cosineSimilarity = dotProduct / (norm1 * norm2)
        return 1.0 - cosineSimilarity
    }

    /// Euclidean distance
    private func euclideanDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        var sum: Double = 0.0
        for (a, b) in zip(v1, v2) {
            let diff = Double(a) - Double(b)
            sum += diff * diff
        }
        return sqrt(sum)
    }

    /// Dot product distance: -dot_product
    private func dotProductDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        return -dotProduct
    }

    /// Assign random level for new node
    private func assignRandomLevel() -> Int {
        let randomValue = Double.random(in: 0..<1)
        let level = Int(floor(-log(randomValue) * parameters.ml))
        return max(0, level)
    }

    /// Get current entry point of HNSW graph
    private func getEntryPoint(
        transaction: any TransactionProtocol,
        snapshot: Bool = true
    ) async throws -> Tuple? {
        guard let entryPointBytes = try await transaction.getValue(for: entryPointKey, snapshot: snapshot) else {
            return nil
        }
        let elements = try Tuple.unpack(from: entryPointBytes)
        return Tuple(elements)
    }

    /// Set entry point of HNSW graph
    private func setEntryPoint(primaryKey: Tuple, transaction: any TransactionProtocol) {
        transaction.setValue(primaryKey.pack(), for: entryPointKey)
    }

    /// Load node metadata
    public func getNodeMetadata(
        primaryKey: Tuple,
        transaction: any TransactionProtocol,
        snapshot: Bool = true
    ) async throws -> HNSWNodeMetadata? {
        let nodeKey = nodesSubspace.pack(primaryKey)
        guard let nodeBytes = try await transaction.getValue(for: nodeKey, snapshot: snapshot) else {
            return nil
        }

        let decoder = JSONDecoder()
        return try decoder.decode(HNSWNodeMetadata.self, from: Data(nodeBytes))
    }

    /// Save node metadata
    private func setNodeMetadata(
        primaryKey: Tuple,
        metadata: HNSWNodeMetadata,
        transaction: any TransactionProtocol
    ) throws {
        let nodeKey = nodesSubspace.pack(primaryKey)
        try validateKeySize(nodeKey)
        let encoder = JSONEncoder()
        let nodeBytes = try encoder.encode(metadata)
        try validateValueSize(Array(nodeBytes))
        transaction.setValue(Array(nodeBytes), for: nodeKey)
    }

    /// Get neighbors of a node at a specific level
    private func getNeighbors(
        primaryKey: Tuple,
        level: Int,
        transaction: any TransactionProtocol,
        snapshot: Bool = true
    ) async throws -> [Tuple] {
        let levelSubspace = edgesSubspace.subspace(primaryKey).subspace(level)
        let (begin, end) = levelSubspace.range()

        var neighbors: [Tuple] = []
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: snapshot
        )

        for try await (key, _) in sequence {
            let neighborPK = try levelSubspace.unpack(key)
            neighbors.append(neighborPK)
        }

        return neighbors
    }

    /// Add bidirectional edge between two nodes
    private func addEdge(
        fromPK: Tuple,
        toPK: Tuple,
        level: Int,
        transaction: any TransactionProtocol
    ) {
        // Forward edge
        let forwardKey = edgesSubspace.subspace(fromPK).subspace(level).pack(toPK)
        transaction.setValue([], for: forwardKey)

        // Backward edge (bidirectional)
        let backwardKey = edgesSubspace.subspace(toPK).subspace(level).pack(fromPK)
        transaction.setValue([], for: backwardKey)
    }

    /// Remove edge between two nodes
    private func removeEdge(
        fromPK: Tuple,
        toPK: Tuple,
        level: Int,
        transaction: any TransactionProtocol
    ) {
        let forwardKey = edgesSubspace.subspace(fromPK).subspace(level).pack(toPK)
        transaction.clear(key: forwardKey)

        let backwardKey = edgesSubspace.subspace(toPK).subspace(level).pack(fromPK)
        transaction.clear(key: backwardKey)
    }
}

// MARK: - HNSW Search Algorithm

extension HNSWIndexMaintainer {
    /// Search for nearest neighbors within a single layer (greedy search)
    ///
    /// Uses proper heap semantics:
    /// - `candidates`: Min-heap (pop smallest distance first for greedy exploration)
    /// - `result`: Max-heap bounded to ef elements (tracks k-best, worst = largest distance)
    ///
    /// **Optimization**: Optional vector cache reduces redundant FDB reads when
    /// the same vectors are accessed multiple times during graph traversal.
    private func searchLayer(
        queryVector: [Float],
        entryPoints: [Tuple],
        ef: Int,
        level: Int,
        transaction: any TransactionProtocol,
        snapshot: Bool = true,
        cache: VectorCache? = nil
    ) async throws -> [(primaryKey: Tuple, distance: Double)] {
        var visited = Set<Tuple>()

        // Candidates: Min-heap to explore closest candidates first
        // Pop returns smallest distance (greedy search toward query)
        var candidates = CandidateHeap<Tuple>()

        // Result: Max-heap bounded to ef elements
        // Tracks ef-best results, worst (root) = largest distance
        var result = ResultHeap<Tuple>(k: ef)

        // Initialize with entry points
        for entryPK in entryPoints {
            let entryVector = try await loadVectorCached(primaryKey: entryPK, transaction: transaction, cache: cache)
            let distance = calculateDistance(queryVector, entryVector)

            candidates.insert((primaryKey: entryPK, distance: distance))
            result.insert((primaryKey: entryPK, distance: distance))
            visited.insert(entryPK)
        }

        // Greedy search: explore closest candidates first
        while !candidates.isEmpty {
            // Pop closest candidate (smallest distance)
            guard let current = candidates.pop() else { break }

            // Termination: if closest candidate is worse than our worst result,
            // no unexplored candidate can improve results
            if result.isFull, let worst = result.worst, current.distance > worst.distance {
                break
            }

            // Explore neighbors of current candidate
            let neighbors = try await getNeighbors(
                primaryKey: current.primaryKey,
                level: level,
                transaction: transaction,
                snapshot: snapshot
            )

            for neighborPK in neighbors {
                if visited.contains(neighborPK) { continue }
                visited.insert(neighborPK)

                // Defensive: Skip nodes whose vectors have been deleted
                // This can happen if edges point to recently deleted nodes
                guard let neighborVector = try? await loadVectorCached(primaryKey: neighborPK, transaction: transaction, cache: cache) else {
                    continue  // Skip deleted nodes gracefully
                }
                let distance = calculateDistance(queryVector, neighborVector)

                // Add to candidates and results if:
                // - Results not full yet, OR
                // - Neighbor is closer than current worst result
                let shouldAdd = !result.isFull || (result.worst.map { distance < $0.distance } ?? true)
                if shouldAdd {
                    candidates.insert((primaryKey: neighborPK, distance: distance))
                    result.insert((primaryKey: neighborPK, distance: distance))
                }
            }
        }

        // Return results sorted by distance ascending (best first)
        return result.toSortedArray()
    }

    /// Select M neighbors using heuristic
    private func selectNeighborsHeuristic(
        candidates: [(primaryKey: Tuple, distance: Double)],
        M: Int
    ) -> [Tuple] {
        let sorted = candidates.sorted { $0.distance < $1.distance }
        return sorted.prefix(M).map { $0.primaryKey }
    }

    /// Insert a new node into HNSW graph
    ///
    /// **Optimization**: Uses vector cache to reduce FDB reads from O(M²) to O(M).
    /// Previously, neighbor pruning loaded each neighbor's vector individually,
    /// resulting in M × M reads per level. With caching, vectors are loaded once
    /// and reused across all pruning operations.
    private func insert(
        primaryKey: Tuple,
        queryVector: [Float],
        transaction: any TransactionProtocol
    ) async throws {
        // Assign random level
        let nodeLevel = assignRandomLevel()

        // Get current entry point
        guard let entryPointPK = try await getEntryPoint(transaction: transaction) else {
            // First node in graph
            let metadata = HNSWNodeMetadata(level: nodeLevel)
            try setNodeMetadata(primaryKey: primaryKey, metadata: metadata, transaction: transaction)
            setEntryPoint(primaryKey: primaryKey, transaction: transaction)
            return
        }

        let entryMetadata = try await getNodeMetadata(primaryKey: entryPointPK, transaction: transaction)!
        let currentLevel = entryMetadata.level

        // Create vector cache for this insert operation
        // This reduces FDB reads when same vectors are accessed multiple times
        let vectorCache = VectorCache()

        // Search for nearest neighbors from top to target level
        var entryPoints = [entryPointPK]

        // Phase 1: Greedy search from top to nodeLevel + 1
        for level in stride(from: currentLevel, through: nodeLevel + 1, by: -1) {
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

        // Phase 2: Insert at each layer from nodeLevel to 0
        for level in stride(from: nodeLevel, through: 0, by: -1) {
            let candidates = try await searchLayer(
                queryVector: queryVector,
                entryPoints: entryPoints,
                ef: parameters.efConstruction,
                level: level,
                transaction: transaction,
                cache: vectorCache
            )

            let M_level = (level == 0) ? parameters.M_max0 : parameters.M_max
            let neighbors = selectNeighborsHeuristic(candidates: candidates, M: M_level)

            // Add bidirectional edges
            for neighborPK in neighbors {
                addEdge(fromPK: primaryKey, toPK: neighborPK, level: level, transaction: transaction)

                // Prune neighbor's connections if exceeds M
                let neighborNeighbors = try await getNeighbors(
                    primaryKey: neighborPK,
                    level: level,
                    transaction: transaction,
                    snapshot: false
                )

                if neighborNeighbors.count > M_level {
                    // Use cached vector loading to avoid redundant FDB reads
                    let neighborVector = try await loadVectorCached(primaryKey: neighborPK, transaction: transaction, cache: vectorCache)

                    var neighborCandidates: [(primaryKey: Tuple, distance: Double)] = []
                    for nnPK in neighborNeighbors {
                        // Vectors are often already cached from searchLayer traversal
                        let nnVector = try await loadVectorCached(primaryKey: nnPK, transaction: transaction, cache: vectorCache)
                        let distance = calculateDistance(neighborVector, nnVector)
                        neighborCandidates.append((primaryKey: nnPK, distance: distance))
                    }

                    let prunedNeighbors = selectNeighborsHeuristic(candidates: neighborCandidates, M: M_level)

                    for nnPK in neighborNeighbors {
                        if !prunedNeighbors.contains(nnPK) {
                            removeEdge(fromPK: neighborPK, toPK: nnPK, level: level, transaction: transaction)
                        }
                    }
                }
            }

            entryPoints = candidates.map { $0.primaryKey }
        }

        // Save node metadata
        let metadata = HNSWNodeMetadata(level: nodeLevel)
        try setNodeMetadata(primaryKey: primaryKey, metadata: metadata, transaction: transaction)

        // Update entry point if new node has higher level
        if nodeLevel > currentLevel {
            setEntryPoint(primaryKey: primaryKey, transaction: transaction)
        }
    }

    /// Search for k nearest neighbors using HNSW graph
    ///
    /// **Optimization**: Uses vector cache to avoid redundant FDB reads when
    /// traversing the graph. Vectors accessed during upper-layer greedy search
    /// are cached and reused if encountered again at lower layers.
    public func search(
        queryVector: [Float],
        k: Int,
        searchParams: HNSWSearchParameters,
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }

        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        guard searchParams.ef >= k else {
            throw VectorIndexError.invalidArgument("ef must be >= k")
        }

        // Get entry point - return empty if graph not built
        guard let entryPointPK = try await getEntryPoint(transaction: transaction) else {
            return []  // Empty graph returns empty results (no error)
        }

        let entryMetadata = try await getNodeMetadata(primaryKey: entryPointPK, transaction: transaction)!
        let currentLevel = entryMetadata.level

        // Create vector cache for this search operation
        let vectorCache = VectorCache()

        // Phase 1: Greedy search from top to layer 1
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

        // Phase 2: Search at layer 0 with ef
        let candidates = try await searchLayer(
            queryVector: queryVector,
            entryPoints: entryPoints,
            ef: searchParams.ef,
            level: 0,
            transaction: transaction,
            cache: vectorCache
        )

        // Return top k with primaryKey converted to array
        // Use compactMap to safely handle potential tuple decoding errors
        return candidates.prefix(k).compactMap { candidate -> (primaryKey: [any TupleElement], distance: Double)? in
            guard let elements = try? Tuple.unpack(from: candidate.primaryKey.pack()) else {
                return nil  // Skip invalid entries instead of crashing
            }
            return (primaryKey: elements, distance: candidate.distance)
        }
    }

    /// Search with default parameters (uses configured efSearch)
    public func search(
        queryVector: [Float],
        k: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        // Use configured efSearch, but ensure ef >= k
        let searchParams = HNSWSearchParameters(ef: max(k, parameters.efSearch))
        return try await search(
            queryVector: queryVector,
            k: k,
            searchParams: searchParams,
            transaction: transaction
        )
    }
}

// MARK: - HNSW Node Deletion

extension HNSWIndexMaintainer {
    /// Delete a node from the HNSW graph
    private func deleteNode(
        primaryKey: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        guard let metadata = try await getNodeMetadata(
            primaryKey: primaryKey,
            transaction: transaction,
            snapshot: false
        ) else {
            return
        }

        // Rewire neighbors and remove edges at each level
        for level in 0...metadata.level {
            let neighbors = try await getNeighbors(
                primaryKey: primaryKey,
                level: level,
                transaction: transaction,
                snapshot: false
            )

            // Rewire neighbors to each other
            if neighbors.count > 1 {
                try await rewireNeighbors(
                    neighbors: neighbors,
                    level: level,
                    transaction: transaction
                )
            }

            // Remove all edges to/from the deleted node
            for neighborPK in neighbors {
                removeEdge(
                    fromPK: primaryKey,
                    toPK: neighborPK,
                    level: level,
                    transaction: transaction
                )
            }
        }

        // Delete node metadata
        let nodeKey = nodesSubspace.pack(primaryKey)
        transaction.clear(key: nodeKey)

        // Update entry point if necessary
        let currentEntryPoint = try await getEntryPoint(
            transaction: transaction,
            snapshot: false
        )
        if currentEntryPoint == primaryKey {
            try await updateEntryPointAfterDeletion(transaction: transaction)
        }
    }

    /// Rewire neighbors of a deleted node
    private func rewireNeighbors(
        neighbors: [Tuple],
        level: Int,
        transaction: any TransactionProtocol
    ) async throws {
        let M_level = (level == 0) ? parameters.M_max0 : parameters.M_max

        // Load all neighbor vectors
        var neighborVectors: [(primaryKey: Tuple, vector: [Float])] = []
        for neighborPK in neighbors {
            let vector = try await loadVectorFromFlatIndex(primaryKey: neighborPK, transaction: transaction)
            neighborVectors.append((primaryKey: neighborPK, vector: vector))
        }

        // For each neighbor, try to connect it to other neighbors
        for i in 0..<neighborVectors.count {
            let (neighborPK, neighborVector) = neighborVectors[i]

            let currentNeighbors = try await getNeighbors(
                primaryKey: neighborPK,
                level: level,
                transaction: transaction,
                snapshot: false
            )

            if currentNeighbors.count >= M_level {
                continue
            }

            // Calculate distances to other neighbors
            var candidates: [(primaryKey: Tuple, distance: Double)] = []
            for j in 0..<neighborVectors.count where j != i {
                let (otherPK, otherVector) = neighborVectors[j]

                if currentNeighbors.contains(otherPK) {
                    continue
                }

                let distance = calculateDistance(neighborVector, otherVector)
                candidates.append((primaryKey: otherPK, distance: distance))
            }

            // Sort by distance and add edges
            candidates.sort { $0.distance < $1.distance }
            let availableSlots = M_level - currentNeighbors.count
            let toConnect = candidates.prefix(availableSlots)

            for candidate in toConnect {
                addEdge(
                    fromPK: neighborPK,
                    toPK: candidate.primaryKey,
                    level: level,
                    transaction: transaction
                )
            }
        }
    }

    /// Find and set new entry point after deletion
    private func updateEntryPointAfterDeletion(
        transaction: any TransactionProtocol
    ) async throws {
        let (begin, end) = nodesSubspace.range()
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: false
        )

        var maxLevel = -1
        var newEntryPoint: Tuple? = nil
        let decoder = JSONDecoder()

        for try await (key, value) in sequence {
            let nodePK = try nodesSubspace.unpack(key)
            let metadata = try decoder.decode(HNSWNodeMetadata.self, from: Data(value))

            if metadata.level > maxLevel {
                maxLevel = metadata.level
                newEntryPoint = nodePK
            }
        }

        if let newEntryPoint = newEntryPoint {
            setEntryPoint(primaryKey: newEntryPoint, transaction: transaction)
        } else {
            transaction.clear(key: entryPointKey)
        }
    }
}

// MARK: - Helper Methods

extension HNSWIndexMaintainer {
    /// Extract vector from item using DataAccess
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    public func extractVector(from item: Item) throws -> [Float] {
        // Use optimized DataAccess method - KeyPath when available, falls back to KeyExpression
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        var result: [Float] = []
        for element in fieldValues {
            if let array = element as? [Float] {
                result.append(contentsOf: array)
            } else if let array = element as? [Float32] {
                result.append(contentsOf: array.map { Float($0) })
            } else if let array = element as? [Double] {
                result.append(contentsOf: array.map { Float($0) })
            } else if let f = element as? Float {
                result.append(f)
            } else if let d = element as? Double {
                result.append(Float(d))
            } else {
                throw VectorIndexError.invalidArgument("Vector field must contain numeric values")
            }
        }

        guard result.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: result.count
            )
        }

        return result
    }

    /// Get maximum level across all nodes - O(1) via entry point
    ///
    /// **Optimization**: The entry point always has the highest level in the graph
    /// (per HNSW invariant maintained in insert()). Instead of scanning all nodes O(n),
    /// we simply read the entry point's metadata O(1).
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Maximum level in the graph (0 if empty)
    public func getMaxLevel(transaction: any TransactionProtocol) async throws -> Int {
        // Entry point always has the highest level (HNSW invariant)
        guard let entryPointPK = try await getEntryPoint(transaction: transaction) else {
            return 0  // Empty graph
        }

        guard let metadata = try await getNodeMetadata(
            primaryKey: entryPointPK,
            transaction: transaction
        ) else {
            return 0  // Entry point not found (shouldn't happen)
        }

        return metadata.level
    }

    // MARK: - Node Count Management

    /// Get current node count - O(1) atomic counter read
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Number of nodes in the graph
    public func getNodeCount(transaction: any TransactionProtocol) async throws -> Int64 {
        guard let bytes = try await transaction.getValue(for: nodeCountKey, snapshot: true) else {
            return 0
        }
        return bytesToInt64(bytes)
    }

    /// Increment node count atomically
    private func incrementNodeCount(transaction: any TransactionProtocol) {
        let incrementBytes = int64ToBytes(1)
        transaction.atomicOp(key: nodeCountKey, param: incrementBytes, mutationType: .add)
    }

    /// Decrement node count atomically
    private func decrementNodeCount(transaction: any TransactionProtocol) {
        let decrementBytes = int64ToBytes(-1)
        transaction.atomicOp(key: nodeCountKey, param: decrementBytes, mutationType: .add)
    }

    /// Convert Int64 to little-endian bytes for FDB atomic operations
    private func int64ToBytes(_ value: Int64) -> [UInt8] {
        return withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }

    /// Convert little-endian bytes to Int64
    private func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        guard bytes.count == 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }
}

// MARK: - ACORN Filtered Search

extension HNSWIndexMaintainer {

    /// Search with predicate filter (ACORN-1 algorithm)
    ///
    /// ACORN (Approximate Containment Queries Over Real-Value Navigable Networks)
    /// enables efficient filtered vector search over HNSW graphs.
    ///
    /// **Algorithm**: During graph traversal, predicates are evaluated on candidates.
    /// Non-matching nodes are still used for graph traversal (to maintain connectivity)
    /// but are not added to the result set.
    ///
    /// **Reference**: Patel et al., "ACORN: Performant and Predicate-Agnostic Search
    /// Over Vector Embeddings and Structured Data", SIGMOD 2024
    ///
    /// - Parameters:
    ///   - queryVector: Query vector for similarity search
    ///   - k: Number of nearest neighbors to return
    ///   - predicate: Filter predicate (item must satisfy to be included in results)
    ///   - fetchItem: Function to fetch item by primary key (provided by caller)
    ///   - acornParams: ACORN parameters (expansion factor, max evaluations)
    ///   - searchParams: HNSW search parameters (ef)
    ///   - transaction: FDB transaction
    /// - Returns: Array of (primaryKey, distance) for items passing the predicate
    public func searchWithFilter(
        queryVector: [Float],
        k: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        fetchItem: @escaping @Sendable (Tuple, any TransactionProtocol) async throws -> Item?,
        acornParams: ACORNParameters = .default,
        searchParams: HNSWSearchParameters = HNSWSearchParameters(),
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }

        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        // Get entry point - return empty if graph not built
        guard let entryPointPK = try await getEntryPoint(transaction: transaction) else {
            return []
        }

        let entryMetadata = try await getNodeMetadata(primaryKey: entryPointPK, transaction: transaction)!
        let currentLevel = entryMetadata.level

        // Expand ef based on ACORN parameters
        let expandedEf = max(k, searchParams.ef) * acornParams.expansionFactor

        // Create vector cache for this search operation
        let vectorCache = VectorCache()

        // Phase 1: Greedy search from top to layer 1 (no filtering at upper layers)
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

        // Phase 2: Filtered search at layer 0 with expanded ef
        // Pass k separately so result heap can guarantee k results capacity
        let candidates = try await searchLayerWithFilter(
            queryVector: queryVector,
            entryPoints: entryPoints,
            k: k,
            ef: expandedEf,
            level: 0,
            predicate: predicate,
            fetchItem: fetchItem,
            acornParams: acornParams,
            transaction: transaction,
            cache: vectorCache
        )

        // Return top k with primaryKey converted to array
        return candidates.prefix(k).compactMap { candidate -> (primaryKey: [any TupleElement], distance: Double)? in
            guard let elements = try? Tuple.unpack(from: candidate.primaryKey.pack()) else {
                return nil
            }
            return (primaryKey: elements, distance: candidate.distance)
        }
    }

    /// Search layer with predicate evaluation (ACORN-1)
    ///
    /// Key difference from standard searchLayer:
    /// - All neighbors are added to candidates (for graph connectivity)
    /// - Only predicate-passing neighbors are added to results
    ///
    /// - Parameters:
    ///   - k: Minimum number of results to return (result heap capacity)
    ///   - ef: Expanded ef for traversal (controls exploration breadth)
    private func searchLayerWithFilter(
        queryVector: [Float],
        entryPoints: [Tuple],
        k: Int,
        ef: Int,
        level: Int,
        predicate: @escaping @Sendable (Item) async throws -> Bool,
        fetchItem: @escaping @Sendable (Tuple, any TransactionProtocol) async throws -> Item?,
        acornParams: ACORNParameters,
        transaction: any TransactionProtocol,
        cache: VectorCache?
    ) async throws -> [(primaryKey: Tuple, distance: Double)] {
        var visited = Set<Tuple>()
        var candidates = CandidateHeap<Tuple>()
        // Result heap sized to max(k, ef) to guarantee we can return k results
        // while still benefiting from expanded ef for exploration
        var result = ResultHeap<Tuple>(k: max(k, ef))
        var predicateEvaluations = 0

        // Initialize with entry points
        // Entry points are evaluated for predicate too
        for entryPK in entryPoints {
            visited.insert(entryPK)

            let entryVector = try await loadVectorCached(
                primaryKey: entryPK,
                transaction: transaction,
                cache: cache
            )
            let distance = calculateDistance(queryVector, entryVector)

            // Always add to candidates (for graph traversal)
            candidates.insert((primaryKey: entryPK, distance: distance))

            // Evaluate predicate on entry point
            if let item = try await fetchItem(entryPK, transaction) {
                predicateEvaluations += 1
                let passes = try await predicate(item)

                // Only add to results if predicate passes
                if passes {
                    result.insert((primaryKey: entryPK, distance: distance))
                }
            }
        }

        // Greedy search with predicate filtering
        while !candidates.isEmpty {
            guard let current = candidates.pop() else { break }

            // Termination check (based on result heap, not candidates)
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

                // Determine if this neighbor should be explored
                let shouldExplore = !result.isFull || (result.worst.map { distance < $0.distance } ?? true)

                // Always add to candidates for graph connectivity
                // (even non-matching nodes help reach matching nodes)
                if shouldExplore {
                    candidates.insert((primaryKey: neighborPK, distance: distance))
                }

                // ACORN: Predicate evaluation for results
                if let maxEvals = acornParams.maxPredicateEvaluations,
                   predicateEvaluations >= maxEvals {
                    continue  // Skip predicate eval if budget exhausted
                }

                // Fetch item and evaluate predicate
                if let item = try await fetchItem(neighborPK, transaction) {
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
