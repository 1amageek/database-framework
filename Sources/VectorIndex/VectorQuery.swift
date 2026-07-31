// VectorQuery.swift
// VectorIndex - Query extension for vector similarity search

@_spi(PolymorphicRuntime) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

// MARK: - Vector Query Builder

/// Builder for vector similarity search queries
///
/// **Usage**:
/// ```swift
/// import VectorIndex
///
/// // Basic search
/// let similar = try await context.findSimilar(Product.self)
///     .vector(Product.fields.embedding, dimensions: 128)
///     .query(queryVector, k: 10)
///     .metric(.cosine)
///     .execute()
/// // Returns: [(item: Product, distance: Double)]
///
/// // Filtered search using expanded HNSW candidates
/// let filtered = try await context.findSimilar(Product.self)
///     .vector(Product.fields.embedding, dimensions: 128)
///     .query(queryVector, k: 10)
///     .filter { product in product.category == "electronics" }
///     .postFilter(expansionFactor: 3)
///     .execute()
/// ```
public struct VectorQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private let selectedIndexName: String?
    private let graphCache: HNSWGraphCache
    private var graphResourceLimits: HNSWGraphResourceLimits
    private var queryVector: [Float]?
    private var k: Int = 10
    private var distanceMetric: VectorDistanceMetric = .cosine
    private var filterPredicate: (@Sendable (T) async throws -> Bool)?
    private var postFilterParameters: HNSWPostFilterParameters = .default

    internal init(
        queryContext: IndexQueryContext,
        fieldName: String,
        dimensions: Int,
        selectedIndexName: String? = nil,
        graphCache: HNSWGraphCache = HNSWGraphCache(),
        graphResourceLimits: HNSWGraphResourceLimits = .default
    ) {
        self.queryContext = queryContext
        self.fieldName = fieldName
        self.dimensions = dimensions
        self.selectedIndexName = selectedIndexName
        self.graphCache = graphCache
        self.graphResourceLimits = graphResourceLimits
    }

    /// Set the query vector and number of results
    ///
    /// - Parameters:
    ///   - vector: The query vector to search with
    ///   - k: Number of nearest neighbors to return
    /// - Returns: Updated query builder
    public func query(
        _ vector: [Float],
        k: Int
    ) throws(VectorQueryError) -> Self {
        guard k > 0 else {
            throw .invalidResultCount(k)
        }
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

    /// Sets the memory limit used while materializing a persisted HNSW graph.
    public func graphResourceLimits(_ limits: HNSWGraphResourceLimits) -> Self {
        var copy = self
        copy.graphResourceLimits = limits
        return copy
    }

    // MARK: - Filter API

    /// Add a predicate that is evaluated against expanded HNSW candidates.
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.findSimilar(Product.self)
    ///     .vector(Product.fields.embedding, dimensions: 128)
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

    /// Add an async predicate evaluated against expanded HNSW candidates.
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
    ///     .vector(Product.fields.embedding, dimensions: 128)
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

    /// Set candidate expansion parameters for post-filtered search.
    ///
    /// - Parameter expansionFactor: ef expansion multiplier (default: 2)
    /// - Returns: Updated query builder
    public func postFilter(expansionFactor: Int) -> Self {
        var copy = self
        copy.postFilterParameters = HNSWPostFilterParameters(
            expansionFactor: expansionFactor
        )
        return copy
    }

    /// Set candidate expansion and predicate evaluation limits.
    ///
    /// - Parameters:
    ///   - expansionFactor: ef expansion multiplier (default: 2)
    ///   - maxPredicateEvaluations: Maximum predicate evaluations (nil for unlimited)
    /// - Returns: Updated query builder
    public func postFilter(
        expansionFactor: Int = 2,
        maxPredicateEvaluations: Int?
    ) -> Self {
        var copy = self
        copy.postFilterParameters = HNSWPostFilterParameters(
            expansionFactor: expansionFactor,
            maxPredicateEvaluations: maxPredicateEvaluations
        )
        return copy
    }

    /// Execute the vector similarity search
    ///
    /// If a filter predicate is set, applies it to expanded HNSW candidates.
    /// Otherwise, uses HNSW or Flat search based on index configuration.
    ///
    /// - Returns: Array of (item, distance) tuples sorted by distance
    /// - Throws: Error if search fails or query vector not set
    public func execute() async throws -> [(item: T, distance: Double)] {
        if filterPredicate != nil {
            throw VectorQueryError.closureFilterUnsupported
        }

        let response = try await queryContext.context.query(
            toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let item = try QueryRowCodec.decode(row, as: T.self)
            guard let distance = row.annotations["distance"]?.float64Value else {
                throw CanonicalReadError.missingAnnotation("distance")
            }
            return (item: item, distance: distance)
        }
    }

    internal func executeDirect(
        configuration: TransactionConfiguration = .default,
        cachePolicy: CachePolicy = .server
    ) async throws -> [(item: T, distance: Double)] {
        guard let vector = queryVector else {
            throw VectorQueryError.noQueryVector
        }

        guard vector.count == dimensions else {
            throw VectorQueryError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let indexName = try buildIndexName()

        // If a filter is set, expand HNSW candidates before post-filtering.
        if let predicate = filterPredicate {
            return try await executeWithFilter(
                indexName: indexName,
                queryVector: vector,
                predicate: predicate,
                configuration: configuration,
                cachePolicy: cachePolicy
            )
        }

        // Standard search without filter - use HNSW or Flat based on configuration
        return try await executeVectorSearch(
            indexName: indexName,
            queryVector: vector,
            k: k,
            configuration: configuration,
            cachePolicy: cachePolicy
        )
    }

    /// Execute vector search using the configured index layout.
    ///
    /// The read algorithm must match the maintainer that owns the persisted
    /// index layout. An index without runtime configuration uses exact flat
    /// search.
    private func executeVectorSearch(
        indexName: String,
        queryVector: [Float],
        k: Int,
        configuration: TransactionConfiguration,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, distance: Double)] {
        let (indexDescriptor, specification) = try resolveVectorIndex(
            named: indexName
        )

        // Check for VectorIndexConfiguration
        let configs = queryContext.context.container.indexConfigurations[indexName] ?? []
        let runtimePolicy = try VectorRuntimePolicy.resolve(in: configs)

        // Execute search using appropriate maintainer
        let primaryKeysWithDistances: [(primaryKey: [any TupleElement], distance: Double)] =
            try await queryContext.withReadableIndex(
                named: indexName,
                kindIdentifier: VectorIndexSpecification.identifier,
                for: T.self,
                configuration: configuration
            ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            let indexSubspace: Subspace
            if let subspaceKey = runtimePolicy?.subspaceKey {
                indexSubspace = readableIndex.subspace.subspace(subspaceKey)
            } else {
                indexSubspace = readableIndex.subspace
            }
            // Create index for maintainer
            let index = Index(
                name: indexName,
                kind: indexDescriptor.kind,
                rootExpression: FieldKeyExpression(fieldName: self.fieldName),
                isUnique: indexDescriptor.isUnique,
                storedFieldNames: indexDescriptor.storedFieldNames
            )

            let algorithm = runtimePolicy?.algorithm ?? .flat

            switch algorithm {
            case .hnsw(let hnswParams):
                // Use HNSW search
                let params = HNSWParameters(
                    m: hnswParams.m,
                    efConstruction: hnswParams.efConstruction,
                    efSearch: hnswParams.efSearch
                )
                let maintainer = HNSWIndexMaintainer<T>(
                    index: index,
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: indexSubspace,
                    idExpression: FieldKeyExpression(fieldName: "id"),
                    parameters: params,
                    graphCache: graphCache,
                    resourceLimits: graphResourceLimits
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
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: indexSubspace,
                    idExpression: FieldKeyExpression(fieldName: "id")
                )
                return try await maintainer.search(
                    queryVector: queryVector,
                    k: k,
                    transaction: transaction
                )

            case .ivf(let ivfParams):
                // Use IVF search
                let params = IVFParameters(
                    nlist: ivfParams.nlist,
                    nprobe: ivfParams.nprobe,
                    kmeansIterations: ivfParams.kmeansIterations
                )
                let maintainer = IVFIndexMaintainer<T>(
                    index: index,
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: indexSubspace,
                    idExpression: FieldKeyExpression(fieldName: "id"),
                    parameters: params
                )
                return try await maintainer.search(
                    queryVector: queryVector,
                    k: k,
                    transaction: transaction
                )

            case .pq(let pqParams):
                // Use PQ search
                let params = PQParameters(
                    m: pqParams.m,
                    ksub: 256,
                    niter: pqParams.niter
                )
                let maintainer = try PQIndexMaintainer<T>(
                    index: index,
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: indexSubspace,
                    idExpression: FieldKeyExpression(fieldName: "id"),
                    parameters: params
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
        let items = try await queryContext.fetchItems(ids: tuples, type: T.self, cachePolicy: cachePolicy)

        var itemByIdentifier: [ByteString: T] = [:]
        itemByIdentifier.reserveCapacity(items.count)
        for item in items {
            itemByIdentifier[try item.persistableIdentifierTuple().pack()] = item
        }

        var results: [(item: T, distance: Double)] = []
        results.reserveCapacity(primaryKeysWithDistances.count)
        for result in primaryKeysWithDistances {
            if let item = itemByIdentifier[Tuple(result.primaryKey).pack()] {
                results.append((item: item, distance: result.distance))
            }
        }

        return results.sorted { $0.distance < $1.distance }
    }

    internal func toSelectQuery() throws -> SelectQuery {
        guard let vector = queryVector else {
            throw VectorQueryError.noQueryVector
        }

        guard vector.count == dimensions else {
            throw VectorQueryError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let parameters: [String: FieldValue] = [
            VectorReadParameter.fieldName: .string(fieldName),
            VectorReadParameter.dimensions: .int64(Int64(dimensions)),
            VectorReadParameter.queryVector: .vector(
                try Vector(float32: vector)
            ),
            VectorReadParameter.k: .int64(Int64(k)),
            VectorReadParameter.metric: .string(distanceMetric.rawValue)
        ]

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: try buildIndexName(),
                    kindIdentifier: VectorIndexSpecification.identifier,
                    parameters: parameters
                )
            ),
            limit: UInt64(k)
        )
    }

    /// Execute HNSW search followed by application predicate evaluation.
    private func executeWithFilter(
        indexName: String,
        queryVector: [Float],
        predicate: @escaping @Sendable (T) async throws -> Bool,
        configuration: TransactionConfiguration,
        cachePolicy: CachePolicy
    ) async throws -> [(item: T, distance: Double)] {
        let (indexDescriptor, specification) = try resolveVectorIndex(
            named: indexName
        )

        // Find the vector index configuration to check if HNSW is configured
        // Configurations are stored in the container, keyed by index name
        let configs = queryContext.context.container.indexConfigurations[indexName] ?? []
        let runtimePolicy = try VectorRuntimePolicy.resolve(in: configs)

        // Get HNSW parameters if configured
        let hnswParams: VectorHNSWParameters
        if let runtimePolicy {
            switch runtimePolicy.algorithm {
            case .hnsw(let params):
                hnswParams = params
            case .flat:
                throw VectorQueryError.filterNotSupported("Post-filtered search requires an HNSW index. Configure the index with .hnsw() algorithm.")
            case .ivf:
                throw VectorQueryError.filterNotSupported("Post-filtered search requires an HNSW index. IVF does not provide HNSW candidates.")
            case .pq:
                throw VectorQueryError.filterNotSupported("Post-filtered search requires an HNSW index. PQ does not provide HNSW candidates.")
            }
        } else {
            throw VectorQueryError.filterNotSupported("Post-filtered search requires an explicitly configured HNSW index.")
        }

        return try await queryContext.withReadableIndex(
            named: indexName,
            kindIdentifier: VectorIndexSpecification.identifier,
            for: T.self,
            configuration: configuration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            let indexSubspace: Subspace
            if let subspaceKey = runtimePolicy?.subspaceKey {
                indexSubspace = readableIndex.subspace.subspace(subspaceKey)
            } else {
                indexSubspace = readableIndex.subspace
            }
            // Create the HNSW maintainer
            let index = Index(
                name: indexName,
                kind: indexDescriptor.kind,
                rootExpression: FieldKeyExpression(fieldName: self.fieldName),
                isUnique: indexDescriptor.isUnique,
                storedFieldNames: indexDescriptor.storedFieldNames
            )

            let maintainer = HNSWIndexMaintainer<T>(
                index: index,
                dimensions: specification.dimensions,
                metric: specification.metric,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                parameters: HNSWParameters(
                    m: hnswParams.m,
                    efConstruction: hnswParams.efConstruction,
                    efSearch: hnswParams.efSearch
                ),
                graphCache: graphCache,
                resourceLimits: graphResourceLimits
            )

            // Fetch each HNSW candidate before evaluating the application predicate.
            let fetchItem: @Sendable (Tuple, any TransactionAccess) async throws -> T? = { primaryKey, tx in
                // Fetch item using IndexQueryContext
                let items = try await self.queryContext.fetchItems(
                    ids: [primaryKey],
                    type: T.self,
                    cachePolicy: cachePolicy
                )
                return items.first
            }

            // Execute filtered search
            let results = try await maintainer.searchWithPostFilter(
                queryVector: queryVector,
                k: self.k,
                predicate: predicate,
                fetchItem: fetchItem,
                postFilterParameters: self.postFilterParameters,
                transaction: transaction
            )

            // Fetch items for results
            let ids = results.map { Tuple($0.primaryKey) }
            let items = try await self.queryContext.fetchItems(ids: ids, type: T.self, cachePolicy: cachePolicy)

            var itemByIdentifier: [ByteString: T] = [:]
            itemByIdentifier.reserveCapacity(items.count)
            for item in items {
                let identifier = try item.persistableIdentifierTuple()
                itemByIdentifier[identifier.pack()] = item
            }

            var finalResults: [(item: T, distance: Double)] = []
            finalResults.reserveCapacity(results.count)
            for result in results {
                if let item = itemByIdentifier[Tuple(result.primaryKey).pack()] {
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
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        try matchingIndexDescriptors().first
    }

    private func matchingIndexDescriptors() throws -> [IndexDescriptor] {
        try queryContext.indexDescriptors(for: T.self).filter { descriptor in
            guard descriptor.kindIdentifier
                    == VectorIndexSpecification.identifier,
                  descriptor.fieldNames == [fieldName] else {
                return false
            }
            let specification = try VectorIndexSpecification(descriptor.kind)
            return specification.dimensions == dimensions
                && specification.metric.rawValue == distanceMetric.rawValue
        }
    }

    private func resolveVectorIndex(
        named indexName: String
    ) throws -> (IndexDescriptor, VectorIndexSpecification) {
        guard let descriptor = queryContext.indexDescriptors(
            for: T.self
        ).first(where: { $0.name == indexName }) else {
            throw VectorQueryError.indexNotFound(indexName)
        }
        let specification = try VectorIndexSpecification(descriptor.kind)
        guard specification.metadata.fieldNames == [fieldName] else {
            throw VectorQueryError.indexFieldMismatch(indexName)
        }
        guard specification.dimensions == dimensions else {
            throw VectorQueryError.dimensionMismatch(
                expected: specification.dimensions,
                actual: dimensions
            )
        }
        guard specification.metric.rawValue == distanceMetric.rawValue else {
            throw VectorQueryError.metricMismatch(
                expected: specification.metric.rawValue,
                actual: distanceMetric.rawValue
            )
        }
        return (descriptor, specification)
    }

    /// Resolve the declared vector index for the selected field.
    private func buildIndexName() throws -> String {
        let matches = try matchingIndexDescriptors()
        if let selectedIndexName {
            guard matches.contains(where: {
                $0.name == selectedIndexName
            }) else {
                throw VectorQueryError.indexNotFound(selectedIndexName)
            }
            return selectedIndexName
        }
        guard let descriptor = matches.first else {
            throw VectorQueryError.indexNotFound("\(T.persistableType).\(fieldName)")
        }
        guard matches.count == 1 else {
            throw VectorQueryError.ambiguousIndexes(
                entity: T.persistableType,
                field: fieldName
            )
        }
        return descriptor.name
    }
}

// MARK: - Polymorphic Vector Query Builder

public enum PolymorphicVectorQueryError: Error, Sendable, CustomStringConvertible {
    case indexNotFound(groupIdentifier: String, fieldName: String)

    public var description: String {
        switch self {
        case .indexNotFound(let groupIdentifier, let fieldName):
            return """
                No polymorphic vector index was found for group '\(groupIdentifier)' and field '\(fieldName)'.
                Declare the shared vector index on the polymorphic group metadata with its compiled field descriptor.
                """
        }
    }
}

/// Builder for vector similarity search across a polymorphic logical group.
///
/// Use `context.findPolymorphic(ConcreteType.self)` to start the query, then
/// narrow by a vector field that is shared across the conforming models.
public struct PolymorphicVectorQueryBuilder<Member: Persistable & Polymorphable>: Sendable {
    private var base: PolymorphicQuery<Member>
    private let fieldName: String
    private let dimensions: Int
    private var queryVector: [Float]?
    private var k: Int = 10
    private var distanceMetric: VectorDistanceMetric = .cosine

    internal init(
        base: PolymorphicQuery<Member>,
        fieldName: String,
        dimensions: Int
    ) {
        self.base = base
        self.fieldName = fieldName
        self.dimensions = dimensions
    }

    /// Set the query vector and number of nearest neighbors.
    public func query(
        _ vector: [Float],
        k: Int
    ) throws(VectorQueryError) -> Self {
        guard k > 0 else {
            throw .invalidResultCount(k)
        }
        var copy = self
        copy.queryVector = vector
        copy.k = k
        copy.base = copy.base.limit(UInt64(k))
        return copy
    }

    /// Set the vector distance metric.
    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.distanceMetric = metric
        return copy
    }

    /// Set canonical read consistency for the search.
    public func consistency(_ consistency: ReadConsistency?) -> Self {
        var copy = self
        copy.base = copy.base.consistency(consistency)
        return copy
    }

    /// Set canonical page size for the search.
    public func pageSize(_ count: Int?) -> Self {
        var copy = self
        copy.base = copy.base.pageSize(count)
        return copy
    }

    /// Continue from a previous polymorphic continuation token.
    public func continuing(from continuation: QueryContinuation?) -> Self {
        var copy = self
        copy.base = copy.base.continuing(from: continuation)
        return copy
    }

    /// Execute the polymorphic vector search.
    public func execute() async throws -> [PolymorphicQueryResult] {
        try await executePage().results
    }

    /// Execute the polymorphic vector search and return page metadata.
    public func executePage() async throws -> PolymorphicQueryPage {
        try await base.executePage(accessPath: .index(try makeIndexScan()))
    }

    /// Execute the polymorphic vector search and return the first result.
    public func first() async throws -> PolymorphicQueryResult? {
        try await executePage().results.first
    }

    private func makeIndexScan() throws -> IndexScanSource {
        guard let vector = queryVector else {
            throw VectorQueryError.noQueryVector
        }

        guard vector.count == dimensions else {
            throw VectorQueryError.dimensionMismatch(expected: dimensions, actual: vector.count)
        }

        let parameters: [String: FieldValue] = [
            VectorReadParameter.fieldName: .string(fieldName),
            VectorReadParameter.dimensions: .int64(Int64(dimensions)),
            VectorReadParameter.queryVector: .vector(
                try Vector(float32: vector)
            ),
            VectorReadParameter.k: .int64(Int64(k)),
            VectorReadParameter.metric: .string(distanceMetric.rawValue)
        ]

        return IndexScanSource(
            indexName: try buildIndexName(),
            kindIdentifier: VectorIndexSpecification.identifier,
            parameters: parameters
        )
    }

    private func buildIndexName() throws -> String {
        if let resolvedIndexName = try base.resolveIndexName(
            kindIdentifier: VectorIndexSpecification.identifier,
            fieldName: fieldName
        ) {
            return resolvedIndexName
        }

        throw PolymorphicVectorQueryError.indexNotFound(
            groupIdentifier: base.identifier,
            fieldName: fieldName
        )
    }
}

public extension PolymorphicQuery where Member: Persistable & Polymorphable {
    /// Search a shared vector field across all members of the polymorphic group.
    func vector(
        _ field: Field<Member, Vector>,
        dimensions: Int
    ) -> PolymorphicVectorQueryBuilder<Member> {
        PolymorphicVectorQueryBuilder(
            base: self,
            fieldName: field.name,
            dimensions: dimensions
        )
    }

    /// Search a shared optional vector field across all members of the polymorphic group.
    func vector(
        _ field: Field<Member, Vector?>,
        dimensions: Int
    ) -> PolymorphicVectorQueryBuilder<Member> {
        PolymorphicVectorQueryBuilder(
            base: self,
            fieldName: field.name,
            dimensions: dimensions
        )
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
    ///   - field: Compiled vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    /// - Returns: Vector query builder
    public func vector(
        _ field: Field<T, Vector>,
        dimensions: Int
    ) -> VectorQueryBuilder<T> {
        VectorQueryBuilder(
            queryContext: queryContext,
            fieldName: field.name,
            dimensions: dimensions
        )
    }

    /// Specify the optional vector field to search
    ///
    /// - Parameters:
    ///   - field: Compiled optional vector field metadata
    ///   - dimensions: Number of dimensions in the vectors
    /// - Returns: Vector query builder
    public func vector(
        _ field: Field<T, Vector?>,
        dimensions: Int
    ) -> VectorQueryBuilder<T> {
        VectorQueryBuilder(
            queryContext: queryContext,
            fieldName: field.name,
            dimensions: dimensions
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Start a vector similarity search query
    ///
    /// This method is available when you import `VectorIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import VectorIndex
    ///
    /// let similar = try await context.findSimilar(Product.self)
    ///     .vector(Product.fields.embedding, dimensions: 128)
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

    /// The nearest-neighbor result count must be positive.
    case invalidResultCount(Int)

    /// Query vector dimension mismatch
    case dimensionMismatch(expected: Int, actual: Int)

    /// Index not found
    case indexNotFound(String)

    /// The named index does not cover the requested field.
    case indexFieldMismatch(String)

    /// The requested metric differs from the index construction metric.
    case metricMismatch(expected: String, actual: String)

    /// More than one vector index matches the requested field and semantics.
    case ambiguousIndexes(entity: String, field: String)

    /// Filter not supported for this index type
    case filterNotSupported(String)

    /// Closure-based filters cannot be lowered into
    case closureFilterUnsupported

    public var description: String {
        switch self {
        case .noQueryVector:
            return "No query vector provided for vector similarity search"
        case .invalidResultCount(let count):
            return "Vector result count must be positive, got \(count)"
        case .dimensionMismatch(let expected, let actual):
            return "Vector dimension mismatch: expected \(expected), got \(actual)"
        case .indexNotFound(let name):
            return "Vector index not found: \(name)"
        case .indexFieldMismatch(let name):
            return "Vector index '\(name)' does not match the requested field"
        case .metricMismatch(let expected, let actual):
            return "Vector metric mismatch: expected \(expected), got \(actual)"
        case .ambiguousIndexes(let entity, let field):
            return "Multiple vector indexes match \(entity).\(field)"
        case .filterNotSupported(let reason):
            return "Filter not supported: \(reason)"
        case .closureFilterUnsupported:
            return "Closure-based vector filters are not supported on the canonical read path"
        }
    }
}
