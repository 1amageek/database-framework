// FullTextQuery.swift
// FullTextIndex - Query extension for full-text search

import Foundation
import DatabaseEngine
import Core
import QueryIR
import DatabaseClientProtocol
import StorageKit
import FullText

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
    private let fieldName: String
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var fetchLimit: Int?
    private var bm25Params: BM25Parameters = .default
    private var facetFields: [String] = []
    private var facetLimit: Int = 10

    internal init(queryContext: IndexQueryContext, fieldName: String) {
        self.queryContext = queryContext
        self.fieldName = fieldName
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
    public func facets(_ fields: [String], limit: Int = 10) -> Self {
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
            return []
        }

        FullTextReadBridge.registerReadExecutors()
        let response = try await queryContext.context.query(
            toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let data = try JSONEncoder().encode(row.fields)
            return try JSONDecoder().decode(T.self, from: data)
        }
    }

    internal func executeDirect(
        configuration: TransactionConfiguration = .default,
        cachePolicy: CachePolicy = .server
    ) async throws -> [T] {
        guard !searchTerms.isEmpty else {
            return []
        }

        let indexName = buildIndexName()

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let matchingIds: [Tuple] = try await queryContext.withTransaction(configuration: configuration) { transaction in
            if self.matchMode == .phrase {
                // Phrase search requires position-verified matching via maintainer
                return try await self.searchPhrase(
                    indexName: indexName,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
            return try await self.searchFullText(
                terms: self.searchTerms,
                matchMode: self.matchMode,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        var items = try await queryContext.fetchItems(ids: matchingIds, type: T.self, cachePolicy: cachePolicy)

        // Apply limit if specified
        if let limit = fetchLimit, items.count > limit {
            items = Array(items.prefix(limit))
        }

        return items
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
            return FacetedSearchResult(items: [], facets: [:], totalCount: 0)
        }

        FullTextReadBridge.registerReadExecutors()
        let response = try await queryContext.context.query(
            toSelectQuery(includeFacets: true),
            as: T.self,
            options: .default
        )

        let items: [T] = try response.rows.map { row in
            let data = try JSONEncoder().encode(row.fields)
            return try JSONDecoder().decode(T.self, from: data)
        }

        return FacetedSearchResult(
            items: items,
            facets: decodeFacetMetadata(response.metadata),
            totalCount: Int(response.metadata[FullTextReadParameter.totalCount]?.int64Value ?? Int64(items.count))
        )
    }

    internal func executeFacetedDirect(
        configuration: TransactionConfiguration = .default,
        cachePolicy: CachePolicy = .server
    ) async throws -> FacetedSearchResult<T> {
        guard !searchTerms.isEmpty else {
            return FacetedSearchResult(items: [], facets: [:], totalCount: 0)
        }

        let indexName = buildIndexName()

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute search within transaction
        let matchingIds: [Tuple] = try await queryContext.withTransaction(configuration: configuration) { transaction in
            if self.matchMode == .phrase {
                return try await self.searchPhrase(
                    indexName: indexName,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )
            }
            return try await self.searchFullText(
                terms: self.searchTerms,
                matchMode: self.matchMode,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch all matching items
        let allItems = try await queryContext.fetchItems(ids: matchingIds, type: T.self, cachePolicy: cachePolicy)
        let totalCount = allItems.count

        // Compute facet counts from items
        var facetCounts: [String: [(value: String, count: Int64)]] = [:]

        if !facetFields.isEmpty {
            facetCounts = computeFacetsFromItems(allItems, fields: facetFields, limit: facetLimit)
        }

        // Apply limit if specified
        var items = allItems
        if let limit = fetchLimit, items.count > limit {
            items = Array(items.prefix(limit))
        }

        return FacetedSearchResult(
            items: items,
            facets: facetCounts,
            totalCount: totalCount
        )
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
        fields: [String],
        limit: Int
    ) -> [String: [(value: String, count: Int64)]] {
        var fieldCounts: [String: [String: Int64]] = [:]

        // Initialize counts for each field
        for field in fields {
            fieldCounts[field] = [:]
        }

        // Count values for each field
        for item in items {
            for field in fields {
                let values = extractFieldValues(from: item, field: field)
                for value in values {
                    fieldCounts[field]![value, default: 0] += 1
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

    /// Extract field values from an item for faceting
    private func extractFieldValues(from item: T, field: String) -> [String] {
        guard let value = item[dynamicMember: field] else {
            return []
        }

        // Handle arrays
        if let array = value as? [String] {
            return array
        }

        // Handle single values
        if let string = value as? String {
            return [string]
        }

        // Handle other types by converting to string
        if let convertible = value as? CustomStringConvertible {
            return [convertible.description]
        }

        return []
    }

    /// Search for an exact phrase using position-verified matching
    ///
    /// Creates a FullTextIndexMaintainer to call `searchPhrase()` which verifies
    /// term positions form a consecutive sequence. Requires `storePositions=true`
    /// on the index — throws `FullTextIndexError.invalidQuery` otherwise.
    private func searchPhrase(
        indexName: String,
        indexSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [Tuple] {
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? FullTextIndexKind<T> else {
            throw FullTextQueryError.indexNotFound(indexName)
        }

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: self.fieldName),
            keyPaths: indexDescriptor.keyPaths
        )

        let maintainer = FullTextIndexMaintainer<T>(
            index: index,
            tokenizer: kind.tokenizer,
            storePositions: kind.storePositions,
            ngramSize: kind.ngramSize,
            minTermLength: kind.minTermLength,
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
        indexSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [Tuple] {
        let termsSubspace = indexSubspace.subspace("terms")

        // Normalize terms
        let normalizedTerms = terms.map { $0.lowercased().trimmingCharacters(in: .whitespaces) }

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
            matchingIds = try await searchTermsOR(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        case .phrase:
            // Phrase search is handled by searchPhrase() in execute()/executeWithFacets().
            // This path should not be reached, but fall back to AND as a safety measure.
            matchingIds = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
        }

        return matchingIds.map { Tuple($0) }
    }

    /// Search for documents containing all terms (AND query)
    private func searchTermsAND(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var intersection: Set<String>? = nil
        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )
            var currentSet: Set<String> = []

            for elements in results {
                let idKey = elementsToStableKey(elements)
                currentSet.insert(idKey)

                if intersection == nil || intersection!.contains(idKey) {
                    idToElements[idKey] = elements
                }
            }

            if let prev = intersection {
                intersection = prev.intersection(currentSet)
                if intersection!.isEmpty {
                    return []
                }
            } else {
                intersection = currentSet
            }
        }

        guard let finalIntersection = intersection else { return [] }
        return finalIntersection.compactMap { idToElements[$0] }
    }

    /// Search for documents containing any term (OR query)
    private func searchTermsOR(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [[any TupleElement]] {
        guard !terms.isEmpty else { return [] }

        var idToElements: [String: [any TupleElement]] = [:]

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction
            )

            for elements in results {
                let idKey = elementsToStableKey(elements)
                idToElements[idKey] = elements
            }
        }

        return Array(idToElements.values)
    }

    /// Search for documents containing a term
    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [[any TupleElement]] {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()

        var results: [[any TupleElement]] = []

        let sequence = try await transaction.collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for (key, _) in sequence {
            guard termSubspace.contains(key) else { break }

            guard let keyTuple = try? termSubspace.unpack(key) else {
                continue
            }
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }
            results.append(elements)
        }

        return results
    }

    /// Convert TupleElements to a stable key
    private func elementsToStableKey(_ elements: [any TupleElement]) -> String {
        let packed = Tuple(elements).pack()
        return Data(packed).base64EncodedString()
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
            return []
        }

        FullTextReadBridge.registerReadExecutors()
        let response = try await queryContext.context.query(
            toSelectQuery(returnScores: true),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let data = try JSONEncoder().encode(row.fields)
            let item = try JSONDecoder().decode(T.self, from: data)
            let score = row.annotations["score"]?.doubleValue ?? 0
            return (item: item, score: score)
        }
    }

    internal func executeScoredDirect(
        configuration: TransactionConfiguration = .default,
        cachePolicy: CachePolicy = .server
    ) async throws -> [(item: T, score: Double)] {
        guard !searchTerms.isEmpty else {
            return []
        }

        let indexName = buildIndexName()

        // Find the index descriptor to get configuration
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? FullTextIndexKind<T> else {
            throw FullTextQueryError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction(configuration: configuration) { transaction in
            // Create maintainer using makeIndexMaintainer
            let index = Index(
                name: indexName,
                kind: kind,
                rootExpression: FieldKeyExpression(fieldName: self.fieldName),
                keyPaths: indexDescriptor.keyPaths
            )

            let maintainer = FullTextIndexMaintainer<T>(
                index: index,
                tokenizer: kind.tokenizer,
                storePositions: kind.storePositions,
                ngramSize: kind.ngramSize,
                minTermLength: kind.minTermLength,
                subspace: indexSubspace,
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

            // Fetch items
            let ids = scoredResults.map { $0.id }
            let items = try await self.queryContext.fetchItems(ids: ids, type: T.self, cachePolicy: cachePolicy)

            // Create a map of id -> score for efficient lookup
            var idToScore: [String: Double] = [:]
            for result in scoredResults {
                let key = Data(result.id.pack()).base64EncodedString()
                idToScore[key] = result.score
            }

            // Combine items with scores
            var results: [(item: T, score: Double)] = []
            for item in items {
                let idTuple = Tuple(item.id as! any TupleElement)
                let key = Data(idTuple.pack()).base64EncodedString()
                if let score = idToScore[key] {
                    results.append((item: item, score: score))
                }
            }

            // Sort by score descending (in case fetchItems changed order)
            results.sort { $0.score > $1.score }

            return results
        }
    }

    internal func toSelectQuery(
        returnScores: Bool = false,
        includeFacets: Bool = false
    ) -> SelectQuery {
        var parameters: [String: QueryParameterValue] = [
            FullTextReadParameter.fieldName: .string(fieldName),
            FullTextReadParameter.terms: .array(searchTerms.map(QueryParameterValue.string)),
            FullTextReadParameter.matchMode: .string(matchMode.accessPathIdentifier),
            FullTextReadParameter.returnScores: .bool(returnScores),
            FullTextReadParameter.includeFacets: .bool(includeFacets)
        ]

        if let fetchLimit {
            parameters[FullTextReadParameter.limit] = .int(Int64(fetchLimit))
        }
        if returnScores {
            parameters[FullTextReadParameter.bm25K1] = .double(Double(bm25Params.k1))
            parameters[FullTextReadParameter.bm25B] = .double(Double(bm25Params.b))
        }
        if includeFacets, !facetFields.isEmpty {
            parameters[FullTextReadParameter.facetFields] = .array(facetFields.map(QueryParameterValue.string))
            parameters[FullTextReadParameter.facetLimit] = .int(Int64(facetLimit))
        }

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: buildIndexName(),
                    kindIdentifier: FullTextIndexKind<T>.identifier,
                    parameters: parameters
                )
            ),
            limit: fetchLimit
        )
    }

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == FullTextIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? FullTextIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    /// Build the index name based on type and field
    ///
    /// Uses IndexDescriptor lookup for reliable index name resolution.
    private func buildIndexName() -> String {
        if let descriptor = findIndexDescriptor() {
            return descriptor.name
        }
        // Fallback to conventional format
        return "\(T.persistableType)_fulltext_\(fieldName)"
    }

    private func decodeFacetMetadata(_ metadata: [String: FieldValue]) -> [String: [(value: String, count: Int64)]] {
        var facets: [String: [(value: String, count: Int64)]] = [:]

        for (key, value) in metadata {
            guard key.hasPrefix(FullTextReadParameter.facetMetadataPrefix),
                  let buckets = value.arrayValue else {
                continue
            }

            let fieldName = String(key.dropFirst(FullTextReadParameter.facetMetadataPrefix.count))
            facets[fieldName] = buckets.compactMap { bucket in
                guard let elements = bucket.arrayValue,
                      elements.count == 2,
                      let facetValue = elements[0].stringValue,
                      let count = elements[1].int64Value else {
                    return nil
                }
                return (value: facetValue, count: count)
            }
        }

        return facets
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
    public func fullText(_ keyPath: KeyPath<T, String>) -> FullTextQueryBuilder<T> {
        FullTextQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }

    /// Specify the optional text field to search
    ///
    /// - Parameter keyPath: KeyPath to the optional String field
    /// - Returns: Full-text query builder
    public func fullText(_ keyPath: KeyPath<T, String?>) -> FullTextQueryBuilder<T> {
        FullTextQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

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

    public var description: String {
        switch self {
        case .noSearchTerms:
            return "No search terms provided for full-text search"
        case .indexNotFound(let name):
            return "Full-text index not found: \(name)"
        }
    }
}

private extension TextMatchMode {
    var accessPathIdentifier: String {
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
