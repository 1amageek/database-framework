// Search.swift
// FullTextIndex - FullText search query for Fusion
//
// This file is part of FullTextIndex module, not DatabaseEngine.
// DatabaseEngine does not know about FullTextIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import FullText

/// FullText search query for Fusion
///
/// Searches text fields using inverted index with optional BM25 scoring.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Article.self) {
///     Search(\.content, context: context.indexQueryContext)
///         .terms(["swift", "concurrency"])
///         .mode(.all)
///         .bm25(k1: 1.5, b: 0.75)
/// }
/// .execute()
/// ```
public struct Search<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var k1: Float = 1.2
    private var b: Float = 0.75

    // MARK: - Initialization (FusionContext)

    /// Create a Search query for a text field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the String field to search
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Article.self) {
    ///     Search(\.content).terms(["swift", "concurrency"])
    /// }
    /// ```
    public init(_ keyPath: KeyPath<T, String>) {
        guard let context = FusionContext.current else {
            fatalError("Search must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Search query for an optional text field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional String field to search
    public init(_ keyPath: KeyPath<T, String?>) {
        guard let context = FusionContext.current else {
            fatalError("Search must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Search query for a text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, String>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Search query for an optional text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, String?>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Search query with a field name string
    ///
    /// - Parameters:
    ///   - fieldName: The field name to search
    ///   - context: IndexQueryContext for database access
    public init(fieldName: String, context: IndexQueryContext) {
        self.fieldName = fieldName
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set search terms
    ///
    /// - Parameter terms: Array of terms to search for
    /// - Returns: Updated query
    public func terms(_ terms: [String]) -> Self {
        var copy = self
        copy.searchTerms = terms
        return copy
    }

    /// Set search terms with match mode
    ///
    /// - Parameters:
    ///   - terms: Array of terms to search for
    ///   - mode: How to match terms (.all = AND, .any = OR, .phrase)
    /// - Returns: Updated query
    public func terms(_ terms: [String], mode: TextMatchMode) -> Self {
        var copy = self
        copy.searchTerms = terms
        copy.matchMode = mode
        return copy
    }

    /// Set match mode
    ///
    /// - Parameter mode: How to match terms
    /// - Returns: Updated query
    public func mode(_ mode: TextMatchMode) -> Self {
        var copy = self
        copy.matchMode = mode
        return copy
    }

    /// Set BM25 parameters
    ///
    /// - Parameters:
    ///   - k1: Term frequency saturation (default: 1.2)
    ///   - b: Document length normalization (default: 0.75)
    /// - Returns: Updated query
    public func bm25(k1: Float = 1.2, b: Float = 0.75) -> Self {
        var copy = self
        copy.k1 = k1
        copy.b = b
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == FullTextIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName
            guard let kind = descriptor.kind as? FullTextIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard !searchTerms.isEmpty else { return [] }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "fulltext"
            )
        }

        let indexName = descriptor.name

        // Execute search with BM25 scoring using existing FullTextQueryBuilder infrastructure
        let results = try await queryContext.executeFullTextSearch(
            type: T.self,
            indexName: indexName,
            terms: searchTerms,
            matchMode: matchMode,
            limit: nil
        )

        // Filter to candidates if provided
        var filteredResults = results
        if let candidateIds = candidates {
            filteredResults = results.filter { candidateIds.contains("\($0.id)") }
        }

        // For now, assign equal scores since basic executeFullTextSearch doesn't return scores
        // TODO: Use executeFullTextSearchWithScores when available in FullTextIndex module
        let count = Double(filteredResults.count)
        return filteredResults.enumerated().map { index, item in
            // Items earlier in the list get higher scores (assuming they're more relevant)
            let score = count > 1 ? 1.0 - Double(index) / count : 1.0
            return ScoredResult(item: item, score: score)
        }
    }
}
