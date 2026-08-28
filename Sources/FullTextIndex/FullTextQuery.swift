// FullTextQuery.swift
// FullTextIndex - Query extension for full-text search

@_spi(PolymorphicRuntime) import DatabaseEngine
import DatabaseKit
import DatabaseTypes

// MARK: - Full-Text Query Builder

/// Builder for full-text search queries
///
/// **Usage**:
/// ```swift
/// import FullTextIndex
///
/// // Basic search (no ranking)
/// let articles = try await context.search(Article.self)
///     .fullText(Article.fields.content)
///     .terms(["swift", "concurrency"], mode: .all)
///     .limit(20)
///     .execute()
///
/// // BM25 ranked search
/// let ranked = try await context.search(Article.self)
///     .fullText(Article.fields.content)
///     .terms(["swift", "concurrency"])
///     .bm25(k1: 1.5, b: 0.8)
///     .executeWithScores()
/// ```
public struct FullTextQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let field: FieldIdentity
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var fetchLimit: Int?
    private var bm25Params: BM25Parameters = .default
    private var facetFields: [FieldIdentity] = []
    private var facetLimit: Int = 10

    internal init(
        queryContext: IndexQueryContext,
        field: FieldIdentity
    ) {
        self.queryContext = queryContext
        self.field = field
    }

    /// Set search terms and match mode
    ///
    /// - Parameters:
    ///   - terms: The terms to search for
    ///   - mode: How to match terms (.all = AND, .any = OR, .phrase = exact phrase)
    /// - Returns: Updated query builder
    public func terms(_ terms: [String], mode: TextMatchMode = .all) -> Self {
        var copy = self
        copy.searchTerms = terms
        copy.matchMode = mode
        return copy
    }

    /// Limit the number of results
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.fetchLimit = count
        return copy
    }

    /// Set BM25 parameters for ranked search
    ///
    /// - Parameters:
    ///   - k1: Term frequency saturation (default: 1.2)
    ///   - b: Document length normalization (default: 0.75)
    /// - Returns: Updated query builder
    public func bm25(k1: Float = 1.2, b: Float = 0.75) -> Self {
        var copy = self
        copy.bm25Params = BM25Parameters(k1: k1, b: b)
        return copy
    }

    /// Add faceted search for specified fields
    ///
    /// Facets provide aggregated counts for each unique value in the specified
    /// fields, allowing callers to filter results by category, brand, and more.
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.search(Product.self)
    ///     .fullText(Product.fields.description)
    ///     .terms(["laptop"])
    ///     .facet(Product.fields.category, limit: 10)
    ///     .facet(Product.fields.brand, limit: 10)
    ///     .executeWithFacets()
    /// // results.facets["category"] = [("electronics", 42), ("computers", 35)]
    /// ```
    ///
    /// - Parameters:
    ///   - field: A declared String or [String] field to count
    ///   - limit: Maximum number of values per field (default: 10)
    /// - Returns: Updated query builder
    public func facet(
        _ field: Field<T, String>,
        limit: Int = 10
    ) -> Self {
        var copy = self
        copy.facetFields.append(field.identity)
        copy.facetLimit = limit
        return copy
    }

    public func facet(
        _ field: Field<T, String?>,
        limit: Int = 10
    ) -> Self {
        var copy = self
        copy.facetFields.append(field.identity)
        copy.facetLimit = limit
        return copy
    }

    public func facet(
        _ field: Field<T, [String]>,
        limit: Int = 10
    ) -> Self {
        var copy = self
        copy.facetFields.append(field.identity)
        copy.facetLimit = limit
        return copy
    }

    public func facet(
        _ field: Field<T, [String]?>,
        limit: Int = 10
    ) -> Self {
        var copy = self
        copy.facetFields.append(field.identity)
        copy.facetLimit = limit
        return copy
    }

    /// Execute the full-text search
    ///
    /// - Returns: Array of matching items
    /// - Throws: Error if search fails
    public func execute() async throws -> [T] {
        guard !searchTerms.isEmpty else {
            return []
        }

        let response = try await queryContext.context.query(
            try toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            try QueryRowCodec.decode(row, as: T.self)
        }
    }

    /// Execute the full-text search with faceted results
    ///
    /// Returns matching items along with facet counts for specified fields.
    /// Facet counts and total count are returned as canonical response metadata.
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.search(Product.self)
    ///     .fullText(Product.fields.description)
    ///     .terms(["laptop"])
    ///     .facet(Product.fields.category, limit: 10)
    ///     .facet(Product.fields.brand, limit: 10)
    ///     .executeWithFacets()
    ///
    /// print("Found \(results.items.count) items")
    /// for (field, values) in results.facets {
    ///     print("\(field):")
    ///     for (value, count) in values {
    ///         print("  \(value): \(count)")
    ///     }
    /// }
    /// ```
    ///
    /// - Returns: FacetedSearchResult containing items and facet counts
    /// - Throws: Error if search fails
    public func executeWithFacets() async throws -> FacetedSearchResult<T> {
        guard !searchTerms.isEmpty else {
            return FacetedSearchResult(items: [], facets: [:], totalCount: 0)
        }

        let response = try await queryContext.context.query(
            try toSelectQuery(includeFacets: true),
            as: T.self,
            options: .default
        )

        let items: [T] = try response.rows.map { row in
            try QueryRowCodec.decode(row, as: T.self)
        }

        guard let encodedTotalCount = response.metadata[
            FullTextReadParameter.totalCount
        ]?.uint64Value,
              let totalCount = Int(exactly: encodedTotalCount) else {
            throw FullTextQueryError.invalidResponse(
                "Missing or out-of-range total-count metadata"
            )
        }
        return FacetedSearchResult(
            items: items,
            facets: try decodeFacetMetadata(response.metadata),
            totalCount: totalCount
        )
    }

    /// Execute the full-text search with BM25 scores
    ///
    /// Returns results ranked by BM25 score (higher is better match).
    ///
    /// **Usage**:
    /// ```swift
    /// let ranked = try await context.search(Article.self)
    ///     .fullText(Article.fields.content)
    ///     .terms(["swift", "concurrency"])
    ///     .bm25(k1: 1.5, b: 0.8)
    ///     .executeWithScores()
    ///
    /// for (article, score) in ranked {
    ///     print("\(article.title): \(score)")
    /// }
    /// ```
    ///
    /// - Returns: Array of (item, score) tuples sorted by score descending
    /// - Throws: Error if search fails
    public func executeWithScores() async throws -> [(item: T, score: Double)] {
        guard !searchTerms.isEmpty else {
            return []
        }

        let response = try await queryContext.context.query(
            try toSelectQuery(returnScores: true),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let item = try QueryRowCodec.decode(row, as: T.self)
            guard let score = row.annotations["score"]?.float64Value else {
                throw CanonicalReadError.missingAnnotation("score")
            }
            return (item: item, score: score)
        }
    }

    internal func toSelectQuery(
        returnScores: Bool = false,
        includeFacets: Bool = false
    ) throws -> SelectQuery {
        var parameters: [String: FieldValue] = [
            FullTextReadParameter.fieldName: .string(field.name),
            FullTextReadParameter.terms: .array(searchTerms.map(FieldValue.string)),
            FullTextReadParameter.matchMode: .string(matchMode.accessPathIdentifier),
            FullTextReadParameter.returnScores: .bool(returnScores),
            FullTextReadParameter.includeFacets: .bool(includeFacets),
        ]

        if let fetchLimit {
            parameters[FullTextReadParameter.limit] = .int64(Int64(fetchLimit))
        }
        if returnScores {
            parameters[FullTextReadParameter.bm25K1] = .float64(
                Double(bm25Params.k1)
            )
            parameters[FullTextReadParameter.bm25B] = .float64(
                Double(bm25Params.b)
            )
        }
        if includeFacets, !facetFields.isEmpty {
            parameters[FullTextReadParameter.facetFields] = .array(
                facetFields.map { .string($0.name) }
            )
            parameters[FullTextReadParameter.facetLimit] = .int64(Int64(facetLimit))
        }

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: try buildIndexName(),
                    indexType: .text(.fullText),
                    parameters: parameters
                )
            ),
            limit: try fetchLimit.map {
                guard let value = UInt64(exactly: $0) else {
                    throw FullTextQueryError.invalidLimit($0)
                }
                return value
            }
        )
    }

    /// Finds the full-text index descriptor for the requested field.
    private func matchingIndexDescriptors() throws -> [IndexDescriptor] {
        queryContext.indexDescriptors(for: T.self).filter { descriptor in
            descriptor.type == .text(.fullText)
                && descriptor.fieldIdentities.contains(field)
        }
    }

    /// Build the index name based on type and field
    ///
    /// Uses IndexDescriptor lookup for reliable index name resolution.
    private func buildIndexName() throws -> String {
        let matches = try matchingIndexDescriptors()
        guard let descriptor = matches.first else {
            throw FullTextQueryError.indexNotFound(
                "\(T.persistableType).\(field.name)"
            )
        }
        guard matches.count == 1 else {
            throw FullTextQueryError.ambiguousIndex(
                entity: T.persistableType,
                field: field.name
            )
        }
        return descriptor.name
    }

    private func decodeFacetMetadata(
        _ metadata: [String: FieldValue]
    ) throws -> [String: [(value: String, count: Int64)]] {
        var facets: [String: [(value: String, count: Int64)]] = [:]

        for (key, value) in metadata {
            guard key.hasPrefix(FullTextReadParameter.facetMetadataPrefix) else {
                continue
            }
            guard let buckets = value.arrayValue else {
                throw FullTextQueryError.invalidResponse(
                    "Facet metadata '\(key)' is not an array"
                )
            }

            let fieldName = String(key.dropFirst(FullTextReadParameter.facetMetadataPrefix.count))
            var decodedBuckets: [(value: String, count: Int64)] = []
            decodedBuckets.reserveCapacity(buckets.count)
            for bucket in buckets {
                guard let elements = bucket.arrayValue,
                      elements.count == 2,
                      let facetValue = elements[0].stringValue,
                      let encodedCount = elements[1].uint64Value,
                      let count = Int64(exactly: encodedCount) else {
                    throw FullTextQueryError.invalidResponse(
                        "Facet metadata '\(key)' contains an invalid bucket"
                    )
                }
                decodedBuckets.append((value: facetValue, count: count))
            }
            facets[fieldName] = decodedBuckets
        }

        return facets
    }
}

// MARK: - Polymorphic Full-Text Query Builder

public enum PolymorphicFullTextQueryError: Error, Sendable, CustomStringConvertible {
    case indexNotFound(groupIdentifier: String, fieldName: String)

    public var description: String {
        switch self {
        case .indexNotFound(let groupIdentifier, let fieldName):
            return """
                No polymorphic full-text index was found for group '\(groupIdentifier)' and field '\(fieldName)'.
                Declare the shared full-text index on the polymorphic group metadata with a KeyPath-based descriptor.
                """
        }
    }
}

/// Builder for full-text search across a polymorphic logical group.
///
/// Use `context.findPolymorphic(ConcreteType.self)` to start the query, then
/// narrow by a field that is shared across the conforming models.
public struct PolymorphicFullTextQueryBuilder<Member: Persistable & Polymorphable>: Sendable {
    private var base: PolymorphicQuery<Member>
    private let field: FieldIdentity
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var returnScores = false
    private var bm25Params: BM25Parameters = .default

    internal init(
        base: PolymorphicQuery<Member>,
        field: FieldIdentity
    ) {
        self.base = base
        self.field = field
    }

    /// Set search terms and match mode.
    public func terms(_ terms: [String], mode: TextMatchMode = .all) -> Self {
        var copy = self
        copy.searchTerms = terms
        copy.matchMode = mode
        return copy
    }

    /// Set a single search term and match mode.
    public func term(_ term: String, mode: TextMatchMode = .all) -> Self {
        terms([term], mode: mode)
    }

    /// Set search terms using a variadic API.
    public func terms(_ terms: String..., mode: TextMatchMode = .all) -> Self {
        self.terms(terms, mode: mode)
    }

    /// Limit the number of matching rows.
    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.base = copy.base.limit(count)
        return copy
    }

    /// Skip the first N matching rows.
    public func offset(_ count: UInt64) -> Self {
        var copy = self
        copy.base = copy.base.offset(count)
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

    /// Return BM25 scores in the annotations of each result row.
    public func bm25(k1: Float = 1.2, b: Float = 0.75) -> Self {
        var copy = self
        copy.returnScores = true
        copy.bm25Params = BM25Parameters(k1: k1, b: b)
        return copy
    }

    /// Execute the polymorphic full-text search.
    public func execute() async throws -> [PolymorphicQueryResult] {
        try await executePage().results
    }

    /// Execute the polymorphic full-text search and return page metadata.
    public func executePage() async throws -> PolymorphicQueryPage {
        guard !searchTerms.isEmpty else {
            return PolymorphicQueryPage(results: [], continuation: nil, metadata: [:])
        }

        return try await base.executePage { schema in
            .index(try makeIndexScan(in: schema))
        }
    }

    /// Execute the polymorphic full-text search and return the first result.
    public func first() async throws -> PolymorphicQueryResult? {
        try await executePage().results.first
    }

    private func makeIndexScan(in schema: Schema) throws -> IndexScanSource {
        var parameters: [String: FieldValue] = [
            FullTextReadParameter.fieldName: .string(field.name),
            FullTextReadParameter.terms: .array(searchTerms.map(FieldValue.string)),
            FullTextReadParameter.matchMode: .string(matchMode.accessPathIdentifier),
            FullTextReadParameter.returnScores: .bool(returnScores),
            FullTextReadParameter.includeFacets: .bool(false),
        ]

        if let limit = base.limitCount {
            parameters[FullTextReadParameter.limit] = .int64(Int64(limit))
        }
        if returnScores {
            parameters[FullTextReadParameter.bm25K1] = .float64(
                Double(bm25Params.k1)
            )
            parameters[FullTextReadParameter.bm25B] = .float64(
                Double(bm25Params.b)
            )
        }

        return IndexScanSource(
            indexName: try buildIndexName(in: schema),
            indexType: .text(.fullText),
            parameters: parameters
        )
    }

    private func buildIndexName(in schema: Schema) throws -> String {
        if let resolvedIndexName = try base.resolveIndexName(
            indexType: .text(.fullText),
            fieldName: field.name,
            in: schema
        ) {
            return resolvedIndexName
        }

        throw PolymorphicFullTextQueryError.indexNotFound(
            groupIdentifier: base.identifier,
            fieldName: field.name
        )
    }
}

extension PolymorphicQuery where Member: Persistable & Polymorphable {
    /// Search a shared String field across all members of the polymorphic group.
    public func fullText(
        _ field: Field<Member, String>
    ) -> PolymorphicFullTextQueryBuilder<Member> {
        PolymorphicFullTextQueryBuilder(
            base: self,
            field: field.identity
        )
    }

    /// Search a shared optional String field across all members of the polymorphic group.
    public func fullText(
        _ field: Field<Member, String?>
    ) -> PolymorphicFullTextQueryBuilder<Member> {
        PolymorphicFullTextQueryBuilder(
            base: self,
            field: field.identity
        )
    }
}

// MARK: - Full-Text Entry Point

/// Entry point for full-text queries
public struct FullTextEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the text field to search
    ///
    /// - Parameter field: Declared String field to search
    /// - Returns: Full-text query builder
    public func fullText(
        _ field: Field<T, String>
    ) -> FullTextQueryBuilder<T> {
        FullTextQueryBuilder(
            queryContext: queryContext,
            field: field.identity
        )
    }

    /// Specify the optional text field to search
    ///
    /// - Parameter field: Declared optional String field to search
    /// - Returns: Full-text query builder
    public func fullText(
        _ field: Field<T, String?>
    ) -> FullTextQueryBuilder<T> {
        FullTextQueryBuilder(
            queryContext: queryContext,
            field: field.identity
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Start a full-text search query
    ///
    /// This method is available when you import `FullTextIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import FullTextIndex
    ///
    /// let articles = try await context.search(Article.self)
    ///     .fullText(Article.fields.content)
    ///     .terms(["swift", "concurrency"], mode: .all)
    ///     .limit(20)
    ///     .execute()
    /// ```
    ///
    /// - Parameter type: The Persistable type to search
    /// - Returns: Entry point for configuring the search
    public func search<T: Persistable>(_ type: T.Type) -> FullTextEntryPoint<T> {
        FullTextEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Full-Text Query Error

/// Errors for full-text query operations
public enum FullTextQueryError: Error, CustomStringConvertible {
    /// Index not found
    case indexNotFound(String)

    case ambiguousIndex(entity: String, field: String)
    case invalidLimit(Int)

    /// The canonical query endpoint returned malformed metadata.
    case invalidResponse(String)

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Full-text index not found: \(name)"
        case .ambiguousIndex(let entity, let field):
            return "Multiple full-text indexes match '\(entity).\(field)'"
        case .invalidLimit(let limit):
            return "Full-text query limit must be nonnegative: \(limit)"
        case .invalidResponse(let reason):
            return "Invalid full-text query response: \(reason)"
        }
    }
}

extension TextMatchMode {
    fileprivate var accessPathIdentifier: String {
        switch self {
        case .all:
            return "all"
        case .any:
            return "any"
        case .phrase:
            return "phrase"
        }
    }
}
