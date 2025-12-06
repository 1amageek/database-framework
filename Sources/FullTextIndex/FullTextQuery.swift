// FullTextQuery.swift
// FullTextIndex - Query extension for full-text search

import Foundation
import DatabaseEngine
import Core
import FoundationDB
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

    /// Execute the full-text search
    ///
    /// - Returns: Array of matching items
    /// - Throws: Error if search fails
    public func execute() async throws -> [T] {
        guard !searchTerms.isEmpty else {
            return []
        }

        let indexName = buildIndexName()

        return try await queryContext.executeFullTextSearch(
            type: T.self,
            indexName: indexName,
            terms: searchTerms,
            matchMode: matchMode,
            limit: fetchLimit
        )
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

        let indexName = buildIndexName()

        // Find the index descriptor to get configuration
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? FullTextIndexKind<T> else {
            throw FullTextQueryError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
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
            let items = try await self.queryContext.fetchItems(ids: ids, type: T.self)

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

    /// Build the index name based on type and field
    private func buildIndexName() -> String {
        return "\(T.persistableType)_\(fieldName)_fulltext"
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
