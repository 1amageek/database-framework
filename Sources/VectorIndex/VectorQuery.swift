// VectorQuery.swift
// VectorIndex - Query extension for vector similarity search

import Foundation
import DatabaseEngine
import Core
import FoundationDB
import Vector

// MARK: - Vector Query Builder

/// Builder for vector similarity search queries
///
/// **Usage**:
/// ```swift
/// import VectorIndex
///
/// // Basic search
/// let similar = try await context.findSimilar(Product.self)
///     .vector(\.embedding, dimensions: 128)
///     .query(queryVector, k: 10)
///     .metric(.cosine)
///     .execute()
/// // Returns: [(item: Product, distance: Double)]
///
/// // Filtered search (ACORN algorithm)
/// let filtered = try await context.findSimilar(Product.self)
///     .vector(\.embedding, dimensions: 128)
///     .query(queryVector, k: 10)
///     .filter { product in product.category == "electronics" }
///     .acorn(expansionFactor: 3)
///     .execute()
/// ```
public struct VectorQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private var queryVector: [Float]?
    private var k: Int = 10
    private var distanceMetric: VectorDistanceMetric = .cosine
    private var filterPredicate: (@Sendable (T) async throws -> Bool)?
    private var acornParams: ACORNParameters = .default

    internal init(queryContext: IndexQueryContext, fieldName: String, dimensions: Int) {
        self.queryContext = queryContext
        self.fieldName = fieldName
        self.dimensions = dimensions
    }

    /// Set the query vector and number of results
    ///
    /// - Parameters:
    ///   - vector: The query vector to search with
    ///   - k: Number of nearest neighbors to return
    /// - Returns: Updated query builder
    public func query(_ vector: [Float], k: Int) -> Self {
        var copy = self
        copy.queryVector = vector
        copy.k = k
        return copy
    }

    /// Set the distance metric
    ///
    /// - Parameter metric: Distance metric (.cosine, .euclidean, .dotProduct)
    /// - Returns: Updated query builder
    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.distanceMetric = metric
        return copy
    }

    // MARK: - ACORN Filter API

    /// Add a filter predicate for ACORN filtered search
    ///
    /// Uses the ACORN algorithm (Approximate Containment Queries Over Real-Value
    /// Navigable Networks) to efficiently filter results during graph traversal.
    ///
    /// **Reference**: Patel et al., "ACORN: Performant and Predicate-Agnostic Search
    /// Over Vector Embeddings and Structured Data", SIGMOD 2024
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.findSimilar(Product.self)
    ///     .vector(\.embedding, dimensions: 128)
    ///     .query(queryVector, k: 10)
    ///     .filter { product in
    ///         product.category == "electronics" && product.price < 1000
    ///     }
    ///     .execute()
    /// ```
    ///
    /// - Parameter predicate: Filter predicate (must return true for item to be included)
    /// - Returns: Updated query builder
    public func filter(_ predicate: @escaping @Sendable (T) -> Bool) -> Self {
        var copy = self
        copy.filterPredicate = { item in predicate(item) }
        return copy
    }

    /// Add an async filter predicate for ACORN filtered search
    ///
    /// - Parameter predicate: Async filter predicate
    /// - Returns: Updated query builder
    public func filter(_ predicate: @escaping @Sendable (T) async throws -> Bool) -> Self {
        var copy = self
        copy.filterPredicate = predicate
        return copy
    }

    /// Add a type-safe equality filter
    ///
    /// Convenience method for simple equality filters.
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.findSimilar(Product.self)
    ///     .vector(\.embedding, dimensions: 128)
    ///     .query(queryVector, k: 10)
    ///     .filter(\.category, equals: "electronics")
    ///     .execute()
    /// ```
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the field
    ///   - value: Value to match
    /// - Returns: Updated query builder
    public func filter<V: Equatable & Sendable>(_ keyPath: KeyPath<T, V> & Sendable, equals value: V) -> Self {
        filter { item in item[keyPath: keyPath] == value }
    }

    /// Set ACORN parameters for filtered search
    ///
    /// - Parameter expansionFactor: ef expansion multiplier (default: 2)
    /// - Returns: Updated query builder
    public func acorn(expansionFactor: Int) -> Self {
        var copy = self
        copy.acornParams = ACORNParameters(expansionFactor: expansionFactor)
        return copy
    }

    /// Set ACORN parameters for filtered search
    ///
    /// - Parameters:
    ///   - expansionFactor: ef expansion multiplier (default: 2)
    ///   - maxPredicateEvaluations: Maximum predicate evaluations (nil for unlimited)
    /// - Returns: Updated query builder
    public func acorn(expansionFactor: Int = 2, maxPredicateEvaluations: Int?) -> Self {
        var copy = self
        copy.acornParams = ACORNParameters(
            expansionFactor: expansionFactor,
            maxPredicateEvaluations: maxPredicateEvaluations
        )
        return copy
    }

    /// Execute the vector similarity search
    ///
    /// If a filter predicate is set, uses ACORN filtered search.
    /// Otherwise, uses HNSW or Flat search based on index configuration.
    ///
    /// - Returns: Array of (item, distance) tuples sorted by distance
    /// - Throws: Error if search fails or query vector not set
    public func execute() async throws -> [(item: T, distance: Double)] {
        guard let vector = queryVector else {
            throw VectorQueryError.noQueryVector
        }

        guard vector.count == dimensions else {
            throw VectorQueryError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let indexName = buildIndexName()

        // If filter is set, use ACORN filtered search
        if let predicate = filterPredicate {
            return try await executeWithFilter(
                indexName: indexName,
                queryVector: vector,
                predicate: predicate
            )
        }

        // Standard search without filter - use HNSW or Flat based on configuration
        return try await executeVectorSearch(
            indexName: indexName,
            queryVector: vector,
            k: k
        )
    }

    /// Execute vector search using HNSW or Flat algorithm based on configuration
    ///
    /// **Algorithm Selection**:
    /// 1. Look up VectorIndexConfiguration for this index
    /// 2. If `.hnsw` algorithm: use HNSWIndexMaintainer.search()
    /// 3. Otherwise (`.flat` or no config): use FlatVectorIndexMaintainer.search()
    private func executeVectorSearch(
        indexName: String,
        queryVector: [Float],
        k: Int
    ) async throws -> [(item: T, distance: Double)] {
        // Find the index descriptor to get VectorIndexKind
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? VectorIndexKind<T> else {
            throw VectorQueryError.indexNotFound(indexName)
        }

        // Check for VectorIndexConfiguration
        let configs = queryContext.context.container.indexConfigurations[indexName] ?? []
        let vectorConfig = configs.first { config in
            type(of: config).kindIdentifier == VectorIndexKind<T>.identifier
        } as? _VectorIndexConfiguration

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace: Subspace
        if let subspaceKey = vectorConfig?.subspaceKey {
            indexSubspace = typeSubspace.subspace(indexName).subspace(subspaceKey)
        } else {
            indexSubspace = typeSubspace.subspace(indexName)
        }

        // Execute search using appropriate maintainer
        let primaryKeysWithDistances: [(primaryKey: [any TupleElement], distance: Double)] = try await queryContext.withTransaction { transaction in
            // Create index for maintainer
            let index = Index(
                name: indexName,
                kind: kind,
                rootExpression: FieldKeyExpression(fieldName: self.fieldName),
                keyPaths: indexDescriptor.keyPaths
            )

            // Select algorithm based on configuration
            if let vectorConfig = vectorConfig {
                switch vectorConfig.algorithm {
                case .hnsw(let hnswParams):
                    // Use HNSW search
                    let params = HNSWParameters(
                        m: hnswParams.m,
                        efConstruction: hnswParams.efConstruction,
                        efSearch: hnswParams.efSearch
                    )
                    let maintainer = HNSWIndexMaintainer<T>(
                        index: index,
                        dimensions: self.dimensions,
                        metric: kind.metric,
                        subspace: indexSubspace,
                        idExpression: FieldKeyExpression(fieldName: "id"),
                        parameters: params
                    )
                    // Use efSearch >= k for good recall
                    let searchParams = HNSWSearchParameters(ef: max(k, hnswParams.efSearch))
                    return try await maintainer.search(
                        queryVector: queryVector,
                        k: k,
                        searchParams: searchParams,
                        transaction: transaction
                    )

                case .flat:
                    // Use flat search
                    let maintainer = FlatVectorIndexMaintainer<T>(
                        index: index,
                        dimensions: self.dimensions,
                        metric: kind.metric,
                        subspace: indexSubspace,
                        idExpression: FieldKeyExpression(fieldName: "id")
                    )
                    return try await maintainer.search(
                        queryVector: queryVector,
                        k: k,
                        transaction: transaction
                    )
                }
            } else {
                // Default: flat scan (safe, exact, no config required)
                let maintainer = FlatVectorIndexMaintainer<T>(
                    index: index,
                    dimensions: self.dimensions,
                    metric: kind.metric,
                    subspace: indexSubspace,
                    idExpression: FieldKeyExpression(fieldName: "id")
                )
                return try await maintainer.search(
                    queryVector: queryVector,
                    k: k,
                    transaction: transaction
                )
            }
        }

        // Convert primary keys to Tuple for fetchItems
        let tuples = primaryKeysWithDistances.map { Tuple($0.primaryKey) }

        // Fetch items by primary keys
        let items = try await queryContext.fetchItems(ids: tuples, type: T.self)

        // Match items with distances
        var results: [(item: T, distance: Double)] = []
        for item in items {
            for result in primaryKeysWithDistances {
                if let pkId = result.primaryKey.first as? String, "\(item.id)" == pkId {
                    results.append((item: item, distance: result.distance))
                    break
                } else if let pkId = result.primaryKey.first as? Int64, "\(item.id)" == "\(pkId)" {
                    results.append((item: item, distance: result.distance))
                    break
                }
            }
        }

        return results.sorted { $0.distance < $1.distance }
    }

    /// Execute filtered vector search using ACORN algorithm
    private func executeWithFilter(
        indexName: String,
        queryVector: [Float],
        predicate: @escaping @Sendable (T) async throws -> Bool
    ) async throws -> [(item: T, distance: Double)] {
        // Find the index descriptor to get configuration
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? VectorIndexKind<T> else {
            throw VectorQueryError.indexNotFound(indexName)
        }

        // Find the vector index configuration to check if HNSW is configured
        // Configurations are stored in the container, keyed by index name
        let configs = queryContext.context.container.indexConfigurations[indexName] ?? []
        let vectorConfig = configs.first { config in
            type(of: config).kindIdentifier == VectorIndexKind<T>.identifier
        } as? _VectorIndexConfiguration

        // Get HNSW parameters if configured
        let hnswParams: VectorHNSWParameters
        if let vectorConfig = vectorConfig {
            switch vectorConfig.algorithm {
            case .hnsw(let params):
                hnswParams = params
            case .flat:
                throw VectorQueryError.filterNotSupported("ACORN filtering is only supported for HNSW indexes. Configure the index with .hnsw() algorithm.")
            }
        } else {
            // No explicit config - default is flat, which doesn't support ACORN
            throw VectorQueryError.filterNotSupported("ACORN filtering requires HNSW index. Add VectorIndexConfiguration with .hnsw() algorithm.")
        }

        // Build subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace: Subspace
        if let subspaceKey = vectorConfig?.subspaceKey {
            indexSubspace = typeSubspace.subspace(indexName).subspace(subspaceKey)
        } else {
            indexSubspace = typeSubspace.subspace(indexName)
        }

        return try await queryContext.withTransaction { transaction in
            // Create the HNSW maintainer
            let index = Index(
                name: indexName,
                kind: kind,
                rootExpression: FieldKeyExpression(fieldName: self.fieldName),
                keyPaths: indexDescriptor.keyPaths
            )

            let maintainer = HNSWIndexMaintainer<T>(
                index: index,
                dimensions: self.dimensions,
                metric: kind.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                parameters: HNSWParameters(
                    m: hnswParams.m,
                    efConstruction: hnswParams.efConstruction,
                    efSearch: hnswParams.efSearch
                )
            )

            // Create fetch function for ACORN
            let fetchItem: @Sendable (Tuple, any TransactionProtocol) async throws -> T? = { primaryKey, tx in
                // Fetch item using IndexQueryContext
                let items = try await self.queryContext.fetchItems(ids: [primaryKey], type: T.self)
                return items.first
            }

            // Execute filtered search
            let results = try await maintainer.searchWithFilter(
                queryVector: queryVector,
                k: self.k,
                predicate: predicate,
                fetchItem: fetchItem,
                acornParams: self.acornParams,
                transaction: transaction
            )

            // Fetch items for results
            let ids = results.map { Tuple($0.primaryKey) }
            let items = try await self.queryContext.fetchItems(ids: ids, type: T.self)

            // Create ID to item map for efficient lookup
            var idToItem: [String: T] = [:]
            for item in items {
                if let id = item.id as? any TupleElement {
                    let key = Data(Tuple(id).pack()).base64EncodedString()
                    idToItem[key] = item
                }
            }

            // Combine with distances
            var finalResults: [(item: T, distance: Double)] = []
            for result in results {
                let key = Data(Tuple(result.primaryKey).pack()).base64EncodedString()
                if let item = idToItem[key] {
                    finalResults.append((item: item, distance: result.distance))
                }
            }

            return finalResults
        }
    }

    /// Find the index descriptor using kindIdentifier and fieldName
    ///
    /// This approach:
    /// 1. Filters by kindIdentifier ("vector") for efficiency
    /// 2. Matches by fieldName within the kind
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == VectorIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName
            guard let kind = descriptor.kind as? VectorIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    /// Build the index name based on type and field
    ///
    /// Uses IndexDescriptor lookup for reliable index name resolution.
    /// Falls back to conventional name format if descriptor not found.
    private func buildIndexName() -> String {
        if let descriptor = findIndexDescriptor() {
            return descriptor.name
        }
        // Fallback to conventional format
        return "\(T.persistableType)_vector_\(fieldName)"
    }
}

// MARK: - Vector Entry Point

/// Entry point for vector queries
public struct VectorEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the vector field to search
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    /// - Returns: Vector query builder
    public func vector(_ keyPath: KeyPath<T, [Float]>, dimensions: Int) -> VectorQueryBuilder<T> {
        VectorQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath),
            dimensions: dimensions
        )
    }

    /// Specify the optional vector field to search
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional [Float] field
    ///   - dimensions: Number of dimensions in the vectors
    /// - Returns: Vector query builder
    public func vector(_ keyPath: KeyPath<T, [Float]?>, dimensions: Int) -> VectorQueryBuilder<T> {
        VectorQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath),
            dimensions: dimensions
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start a vector similarity search query
    ///
    /// This method is available when you import `VectorIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import VectorIndex
    ///
    /// let similar = try await context.findSimilar(Product.self)
    ///     .vector(\.embedding, dimensions: 128)
    ///     .query(queryVector, k: 10)
    ///     .metric(.cosine)
    ///     .execute()
    /// // Returns: [(item: Product, distance: Double)]
    /// ```
    ///
    /// - Parameter type: The Persistable type to search
    /// - Returns: Entry point for configuring the search
    public func findSimilar<T: Persistable>(_ type: T.Type) -> VectorEntryPoint<T> {
        VectorEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Vector Query Error

/// Errors for vector query operations
public enum VectorQueryError: Error, CustomStringConvertible {
    /// No query vector provided
    case noQueryVector

    /// Query vector dimension mismatch
    case dimensionMismatch(expected: Int, actual: Int)

    /// Index not found
    case indexNotFound(String)

    /// Filter not supported for this index type
    case filterNotSupported(String)

    public var description: String {
        switch self {
        case .noQueryVector:
            return "No query vector provided for vector similarity search"
        case .dimensionMismatch(let expected, let actual):
            return "Vector dimension mismatch: expected \(expected), got \(actual)"
        case .indexNotFound(let name):
            return "Vector index not found: \(name)"
        case .filterNotSupported(let reason):
            return "Filter not supported: \(reason)"
        }
    }
}
