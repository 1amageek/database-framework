// Search.swift
// FullTextIndex - FullText search query for Fusion
//
// This file is part of FullTextIndex module, not DatabaseEngine.
// DatabaseEngine dispatches text semantics without depending on this module.

import DatabaseEngine
import DatabaseKit
import DatabaseMath
import DatabaseTypes
import StorageKit

/// FullText search query for Fusion
///
/// Searches text fields using inverted index with optional BM25 scoring.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Article.self) {
///     Search(\.content)
///         .terms(["swift", "concurrency"])
///         .mode(.all)
///         .bm25(k1: 1.5, b: 0.75)
/// }
/// .execute()
/// ```
public struct Search<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext!
    private let field: FieldIdentity
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
    public init(_ field: Field<T, String>) {
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Search query for an optional text field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional String field to search
    public init(_ field: Field<T, String?>) {
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Search query for a text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ field: Field<T, String>, context: IndexQueryContext) {
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Search query for an optional text field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional String field to search
    ///   - context: IndexQueryContext for database access
    public init(_ field: Field<T, String?>, context: IndexQueryContext) {
        self.field = field.identity
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

    /// Finds the text index descriptor for the requested fields.
    private func resolveIndexDescriptor() throws -> IndexDescriptor {
        let matches = queryContext.indexDescriptors(for: T.self).filter {
            descriptor in
            descriptor.type == .text(.fullText)
                && descriptor.fieldIdentities.contains(field)
        }
        guard let descriptor = matches.first else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: field.name,
                indexType: .text(.fullText)
            )
        }
        guard matches.count == 1 else {
            throw FullTextQueryError.ambiguousIndex(
                entity: T.persistableType,
                field: field.name
            )
        }
        return descriptor
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Search requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["score"]
            ),
            indexDescriptor: { try self.resolveIndexDescriptor() },
            operation: { [self] candidates, execution in
                try await executeBound(
                    candidates: candidates,
                    execution: execution
                )
            }
        )
    }

    private func executeBound(
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        let descriptor = try resolveIndexDescriptor()
        let parameters = try BM25Parameters.validated(
            k1: Double(k1),
            b: Double(b)
        )
        return try await FullTextReadExecutor().executeScoredFusion(
            queryContext: queryContext,
            descriptor: descriptor,
            configuration: try FullTextIndexConfiguration(
                definition: descriptor.declaration.definition
            ),
            terms: searchTerms,
            matchMode: matchMode,
            parameters: parameters,
            candidates: candidates,
            execution: execution
        )
    }

}
