// FullTextQuery.swift
// FullTextIndex - Query extension for full-text search

@_spi(DatabaseExecution) @_spi(PolymorphicRuntime) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

// MARK: - Full-Text Query Builder

/// Builder for full-text search queries
///
/// **Usage**:
/// ```swift
/// import FullTextIndex
///
/// // Basic search (no ranking)
/// let articles = try await context.search(Article.self)
///     .fullText(\.content)
///     .terms(["swift", "concurrency"], mode: .all)
///     .limit(20)
///     .execute()
///
/// // BM25 ranked search
/// let ranked = try await context.search(Article.self)
///     .fullText(\.content)
///     .terms(["swift", "concurrency"])
///     .bm25(k1: 1.5, b: 0.8)
///     .executeWithScores()
/// ```
public struct FullTextQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let field: FieldIdentity
    private let selectedIndexName: String?
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var fetchLimit: Int?
    private var bm25Params: BM25Parameters = .default
    private var facetFields: [FieldIdentity] = []
    private var facetLimit: Int = 10

    internal init(
        queryContext: IndexQueryContext,
        field: FieldIdentity,
        selectedIndexName: String? = nil
    ) {
        self.queryContext = queryContext
        self.field = field
        self.selectedIndexName = selectedIndexName
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
    /// Facets provide aggregated counts for each unique value in the specified fields,
    /// allowing users to filter search results by category, brand, etc.
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.search(Product.self)
    ///     .fullText(\.description)
    ///     .terms(["laptop"])
    ///     .facets(["category", "brand"], limit: 10)
    ///     .executeWithFacets()
    /// // results.facets["category"] = [("electronics", 42), ("computers", 35)]
    /// ```
    ///
    /// - Parameters:
    ///   - fields: Field names to compute facets for
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

    internal func facets(
        _ fields: [FieldIdentity],
        limit: Int
    ) -> Self {
        var copy = self
        copy.facetFields = fields
        copy.facetLimit = limit
        return copy
    }

    /// Execute the full-text search
    ///
    /// - Returns: Array of matching items
    /// - Throws: Error if search fails
    public func execute() async throws -> [T] {
        guard !searchTerms.isEmpty else {
            return try await withAuthorizedFullModelRead(orderBy: nil) {
                _, _, _ in []
            }
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

    internal func executeDirect(
        configuration: TransactionConfiguration = .default
    ) async throws -> [T] {
        try await withAuthorizedFullModelRead(orderBy: nil) {
            indexName, _, indexConfiguration in
            guard !searchTerms.isEmpty else { return [] }
            return try await queryContext.withReadableIndex(
                named: indexName,
                indexType: .text(.fullText),
                for: T.self,
                authorization: IndexReadAuthorization(
                    limit: fetchLimit,
                    offset: nil,
                    orderBy: nil
                ),
                configuration: configuration
            ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            let matchingIds: [Tuple]
            if self.matchMode == .phrase {
                // Phrase search requires position-verified matching via maintainer
                matchingIds = try await self.searchPhrase(
                    indexName: indexName,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction
                )
            } else {
                matchingIds = try await self.searchFullText(
                    terms: self.searchTerms,
                    matchMode: self.matchMode,
                    configuration: indexConfiguration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction
                )
            }
            var items = try await self.fetchIndexedItems(
                ids: matchingIds,
                indexName: indexName
            )
            if let limit = self.fetchLimit, items.count > limit {
                items = Array(items.prefix(limit))
            }
            return items
            }
        }
    }

    /// Execute the full-text search with faceted results
    ///
    /// Returns matching items along with facet counts for specified fields.
    /// Facet counts are computed directly from matching items, allowing flexible
    /// faceting without requiring pre-indexed facet data.
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.search(Product.self)
    ///     .fullText(\.description)
    ///     .terms(["laptop"])
    ///     .facets(["category", "brand"], limit: 10)
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
            return try await withAuthorizedFullModelRead(orderBy: nil) {
                _, _, _ in
                FacetedSearchResult(items: [], facets: [:], totalCount: 0)
            }
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

    internal func executeFacetedDirect(
        configuration: TransactionConfiguration = .default
    ) async throws -> FacetedSearchResult<T> {
        try await withAuthorizedFullModelRead(orderBy: nil) {
            indexName, _, indexConfiguration in
            guard !searchTerms.isEmpty else {
                return FacetedSearchResult(
                    items: [],
                    facets: [:],
                    totalCount: 0
                )
            }
            return try await queryContext.withReadableIndex(
                named: indexName,
                indexType: .text(.fullText),
                for: T.self,
                authorization: IndexReadAuthorization(
                    limit: fetchLimit,
                    offset: nil,
                    orderBy: nil
                ),
                configuration: configuration
            ) { readableIndex, transaction in
            guard let readableIndex else {
                return FacetedSearchResult(
                    items: [],
                    facets: [:],
                    totalCount: 0
                )
            }
            let matchingIds: [Tuple]
            if self.matchMode == .phrase {
                matchingIds = try await self.searchPhrase(
                    indexName: indexName,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction
                )
            } else {
                matchingIds = try await self.searchFullText(
                    terms: self.searchTerms,
                    matchMode: self.matchMode,
                    configuration: indexConfiguration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction
                )
            }
            let allItems = try await self.fetchIndexedItems(
                ids: matchingIds,
                indexName: indexName
            )
            let totalCount = allItems.count
            let facetCounts: [String: [(value: String, count: Int64)]]
            if self.facetFields.isEmpty {
                facetCounts = [:]
            } else {
                facetCounts = try self.computeFacetsFromItems(
                    allItems,
                    fields: self.facetFields,
                    limit: self.facetLimit
                )
            }
            let items: [T]
            if let limit = self.fetchLimit, allItems.count > limit {
                items = Array(allItems.prefix(limit))
            } else {
                items = allItems
            }
            return FacetedSearchResult(
                items: items,
                facets: facetCounts,
                totalCount: totalCount
            )
            }
        }
    }

    /// Compute facet counts directly from items
    ///
    /// This allows faceting without requiring pre-indexed facet data.
    ///
    /// - Parameters:
    ///   - items: Items to compute facets for
    ///   - fields: Field names to compute facets for
    ///   - limit: Maximum number of values per field
    /// - Returns: Dictionary of field -> [(value, count)] sorted by count descending
    private func computeFacetsFromItems(
        _ items: [T],
        fields: [FieldIdentity],
        limit: Int
    ) throws -> [String: [(value: String, count: Int64)]] {
        var fieldCounts: [String: [String: Int64]] = [:]

        // Initialize counts for each field
        for field in fields {
            fieldCounts[field.name] = [:]
        }

        // Count values for each field
        for item in items {
            for field in fields {
                let values = try FullTextFieldValueExtractor.strings(
                    from: item,
                    field: field
                )
                for value in values {
                    fieldCounts[field.name]![value, default: 0] += 1
                }
            }
        }

        // Sort and limit results
        var result: [String: [(value: String, count: Int64)]] = [:]
        for (field, counts) in fieldCounts {
            let sorted = counts.sorted { $0.value > $1.value }
            result[field] = Array(sorted.prefix(limit).map { (value: $0.key, count: $0.value) })
        }

        return result
    }

    /// Search for an exact phrase using position-verified matching
    ///
    /// Creates a FullTextIndexMaintainer to call `searchPhrase()` which verifies
    /// term positions form a consecutive sequence. Requires `storePositions=true`
    /// on the index — throws `FullTextIndexError.invalidQuery` otherwise.
    private func searchPhrase(
        indexName: String,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess
    ) async throws -> [Tuple] {
        let (indexDescriptor, configuration) = try resolveFullTextIndex(
            named: indexName
        )

        let index = ResolvedIndex(
            descriptor: indexDescriptor,
            rootExpression: KeyExpressionFactory.from(keyPaths: indexDescriptor.fieldNames)
        )

        let maintainer = FullTextIndexMaintainer<T>(
            index: index,
            tokenizer: configuration.tokenizer,
            storePositions: configuration.storePositions,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let phraseString = searchTerms.joined(separator: " ")
        let results = try await maintainer.searchPhrase(phraseString, transaction: transaction)
        return results.map { Tuple($0) }
    }

    /// Search full-text index and return matching IDs
    private func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess
    ) async throws -> [Tuple] {
        let termsSubspace = indexSubspace.subspace("terms")

        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })

        // Get matching document IDs based on match mode
        let matchingIds: [[any TupleElement]]
        switch matchMode {
        case .all:
            matchingIds = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        case .any:
            var union: [[any TupleElement]] = []
            for group in termGroups {
                let matches = try await searchTermsAND(
                    group,
                    termsSubspace: termsSubspace,
                    transaction: transaction
                )
                union = try FullTextPostingListAlgebra.union(union, matches)
            }
            matchingIds = union
        case .phrase:
            throw FullTextQueryError.invalidExecutionPath(
                "Phrase matching must use the position-aware search path"
            )
        }

        return matchingIds.map { Tuple($0) }
    }

    /// Search for documents containing all terms (AND query)
    private func searchTermsAND(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionReadAccess
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: [[any TupleElement]]?

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            if let existing = intersection {
                let reduced = try FullTextPostingListAlgebra.intersection(
                    existing,
                    results
                )
                if reduced.isEmpty {
                    return []
                }
                intersection = reduced
            } else {
                intersection = results
            }
        }

        return intersection ?? []
    }

    /// Search for documents containing a term
    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any TransactionReadAccess
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for (key, _) in sequence {
            guard termSubspace.contains(key) else { break }

            let keyTuple = try termSubspace.unpack(key)
            let elements = try keyTuple.elements()
            results.append(elements)
        }

        return results
    }

    private func normalizeQueryTermGroups(
        _ terms: [String],
        configuration: FullTextIndexConfiguration
    ) -> [[String]] {
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        return terms.map { term in
            uniqueTerms(normalizer.normalizedTerms(from: term))
        }
        .filter { !$0.isEmpty }
    }

    private func uniqueTerms(_ terms: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []
        result.reserveCapacity(terms.count)
        for term in terms where !seen.contains(term) {
            seen.insert(term)
            result.append(term)
        }
        return result
    }

    /// Execute the full-text search with BM25 scores
    ///
    /// Returns results ranked by BM25 score (higher is better match).
    ///
    /// **Usage**:
    /// ```swift
    /// let ranked = try await context.search(Article.self)
    ///     .fullText(\.content)
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
            return try await withAuthorizedFullModelRead(
                orderBy: ["score"]
            ) { _, _, _ in [] }
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

    internal func executeScoredDirect(
        configuration: TransactionConfiguration = .default
    ) async throws -> [(item: T, score: Double)] {
        try await withAuthorizedFullModelRead(orderBy: ["score"]) {
            indexName, indexDescriptor, indexConfiguration in
            guard !searchTerms.isEmpty else { return [] }
            return try await queryContext.withReadableIndex(
                named: indexName,
                indexType: .text(.fullText),
                for: T.self,
                authorization: IndexReadAuthorization(
                    limit: fetchLimit,
                    offset: nil,
                    orderBy: ["score"]
                ),
                configuration: configuration
            ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            // Create maintainer using makeIndexMaintainer
            let index = ResolvedIndex(
                descriptor: indexDescriptor,
                rootExpression: KeyExpressionFactory.from(keyPaths: indexDescriptor.fieldNames)
            )

            let maintainer = FullTextIndexMaintainer<T>(
                index: index,
                tokenizer: indexConfiguration.tokenizer,
                storePositions: indexConfiguration.storePositions,
                ngramSize: indexConfiguration.ngramSize,
                minTermLength: indexConfiguration.minTermLength,
                subspace: readableIndex.subspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )

            // Search with BM25 scores
            let scoredResults = try await maintainer.searchWithScores(
                terms: self.searchTerms,
                matchMode: self.matchMode,
                bm25Params: self.bm25Params,
                transaction: transaction,
                limit: self.fetchLimit
            )

            let ids = scoredResults.map { $0.id }
            let items = try await self.fetchIndexedItems(
                ids: ids,
                indexName: indexName
            )
            var results: [(item: T, score: Double)] = []
            results.reserveCapacity(scoredResults.count)
            for (item, scoredResult) in zip(items, scoredResults) {
                results.append((item: item, score: scoredResult.score))
            }
            return results
            }
        }
    }

    private func fetchIndexedItems(
        ids: [Tuple],
        indexName: String
    ) async throws -> [T] {
        let fetched = try await queryContext.fetchItemsPreservingOrder(
            ids: ids,
            type: T.self
        )
        var items: [T] = []
        items.reserveCapacity(fetched.count)
        for (id, item) in zip(ids, fetched) {
            guard let item else {
                throw FullTextQueryError.indexedItemMissing(
                    index: indexName,
                    primaryKey: id.pack()
                )
            }
            items.append(item)
        }
        return items
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

    private func resolveFullTextIndex(
        named indexName: String
    ) throws -> (IndexDescriptor, FullTextIndexConfiguration) {
        guard let descriptor = queryContext.indexDescriptors(
            for: T.self
        ).first(where: { $0.name == indexName }) else {
            throw FullTextQueryError.indexNotFound(indexName)
        }
        guard descriptor.type == .text(.fullText),
            descriptor.fieldIdentities.contains(field)
        else {
            throw FullTextQueryError.indexFieldMismatch(indexName)
        }
        return (
            descriptor,
            try FullTextIndexConfiguration(definition: descriptor.declaration.definition)
        )
    }

    /// Admits the complete model projection before any index transaction is
    /// created. Empty and non-empty searches therefore have identical LIST,
    /// field, lifecycle, and index-configuration failure semantics.
    private func withAuthorizedFullModelRead<Result: Sendable>(
        orderBy: [String]?,
        _ operation: @Sendable @escaping (
            String,
            IndexDescriptor,
            FullTextIndexConfiguration
        ) async throws -> Result
    ) async throws -> Result {
        try await queryContext.context.withDataOperation {
            let indexName = try buildIndexName()
            let (descriptor, configuration) = try resolveFullTextIndex(
                named: indexName
            )
            guard let entity = queryContext.schema.entity(
                named: T.persistableType
            ) else {
                throw IndexQueryContextError.entityNotFound(
                    T.persistableType
                )
            }
            let admission = try queryContext.context.admitLogicalRead(
                listAuthorization: IndexReadAuthorization(
                    limit: fetchLimit,
                    offset: nil,
                    orderBy: orderBy
                ),
                fieldPlan: .fullEntity(
                    entity,
                    including: Set(descriptor.fieldNames).union(
                        descriptor.includedFieldNames
                    )
                ),
                restrictingTo: [T.persistableType]
            )
            return try await queryContext.context
                .withReadAuthorizationAdmission(admission) {
                    try await operation(
                        indexName,
                        descriptor,
                        configuration
                    )
                }
        }
    }

    /// Build the index name based on type and field
    ///
    /// Uses IndexDescriptor lookup for reliable index name resolution.
    private func buildIndexName() throws -> String {
        let matches = try matchingIndexDescriptors()
        if let selectedIndexName {
            guard matches.contains(where: {
                $0.name == selectedIndexName
            }) else {
                throw FullTextQueryError.indexNotFound(selectedIndexName)
            }
            return selectedIndexName
        }
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
        return try await base.executePage(accessPath: .index(try makeIndexScan()))
    }

    /// Execute the polymorphic full-text search and return the first result.
    public func first() async throws -> PolymorphicQueryResult? {
        try await executePage().results.first
    }

    private func makeIndexScan() throws -> IndexScanSource {
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
            indexName: try buildIndexName(),
            indexType: .text(.fullText),
            parameters: parameters
        )
    }

    private func buildIndexName() throws -> String {
        if let resolvedIndexName = try base.resolveIndexName(
            indexType: .text(.fullText),
            fieldName: field.name
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
    /// - Parameter keyPath: KeyPath to the String field
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
    /// - Parameter keyPath: KeyPath to the optional String field
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
    ///     .fullText(\.content)
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
    /// No search terms provided
    case noSearchTerms

    /// Index not found
    case indexNotFound(String)

    /// The named index does not include the requested field.
    case indexFieldMismatch(String)

    case ambiguousIndex(entity: String, field: String)
    case invalidLimit(Int)

    /// The canonical query endpoint returned malformed metadata.
    case invalidResponse(String)

    /// Internal dispatch selected a search path with incompatible semantics.
    case invalidExecutionPath(String)

    /// An index entry references an entity that is not present in storage.
    case indexedItemMissing(index: String, primaryKey: ByteString)

    public var description: String {
        switch self {
        case .noSearchTerms:
            return "No search terms provided for full-text search"
        case .indexNotFound(let name):
            return "Full-text index not found: \(name)"
        case .indexFieldMismatch(let name):
            return "Full-text index '\(name)' does not include the requested field"
        case .ambiguousIndex(let entity, let field):
            return "Multiple full-text indexes match '\(entity).\(field)'"
        case .invalidLimit(let limit):
            return "Full-text query limit must be nonnegative: \(limit)"
        case .invalidResponse(let reason):
            return "Invalid full-text query response: \(reason)"
        case .invalidExecutionPath(let reason):
            return "Invalid full-text execution path: \(reason)"
        case .indexedItemMissing(let index, let primaryKey):
            return "Full-text index '\(index)' references missing item \(primaryKey)"
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
