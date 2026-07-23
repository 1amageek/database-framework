// Rank.swift
// RankIndex - Rank-based scoring query for Fusion
//
// This file is part of RankIndex module, not DatabaseEngine.
// Rank is a reranking operation that scores items based on a numeric field.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit

/// Rank-based scoring query for Fusion
///
/// Scores items based on a numeric field value.
/// Used for reranking results by popularity, rating, price, etc.
///
/// **Note**: Rank requires candidates from previous stages.
/// It should not be used as the first stage in a fusion pipeline.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.description, context: context.indexQueryContext).terms(["coffee"])
///     Rank(\.rating, context: context.indexQueryContext).order(.descending)
/// }
/// .execute()
///
/// // Or for sales rank (lower = better)
/// Rank(\.salesRank, context: context.indexQueryContext).order(.ascending)
/// ```
public struct Rank<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    /// Sort order for ranking
    public enum Order: Sendable {
        /// Lower value = higher score (e.g., sales rank, price)
        case ascending
        /// Higher value = higher score (e.g., rating, popularity)
        case descending
    }

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var order: Order = .descending

    // MARK: - Initialization (FusionContext)

    /// Create a Rank query for an exact numeric field.
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Search(\.description).terms(["coffee"])
    ///     Rank(\.rating).order(.descending)
    /// }
    /// ```
    public init<Value: RankNumericValue>(_ keyPath: KeyPath<T, Value>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an optional exact numeric field.
    public init<Value: RankNumericValue>(_ keyPath: KeyPath<T, Value?>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Rank query for an exact numeric field with explicit context.
    public init<Value: RankNumericValue>(
        _ keyPath: KeyPath<T, Value>,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an optional exact numeric field with explicit context.
    public init<Value: RankNumericValue>(
        _ keyPath: KeyPath<T, Value?>,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query with a field name string
    public init(fieldName: String, context: IndexQueryContext) {
        self.fieldName = fieldName
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set the sort order
    ///
    /// - Parameter order: Sort order (.ascending or .descending)
    /// - Returns: Updated query
    public func order(_ order: Order) -> Self {
        var copy = self
        copy.order = order
        return copy
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        // Rank query requires candidates from previous stages
        // It should not be used as the first stage
        guard let candidateIDs = candidates, !candidateIDs.isEmpty else {
            // Return empty - Rank is designed for reranking, not initial search
            // If used as first stage, it contributes nothing to the fusion
            return []
        }

        // Fetch items
        let items = try await queryContext.fetchItems(
            identifiers: Array(candidateIDs),
            type: T.self
        )

        var entries: [RankValueEntry<T>] = []
        entries.reserveCapacity(items.count)
        for item in items {
            let value = try RankValueOrdering.numericValue(
                from: item[dynamicMember: fieldName],
                fieldName: fieldName
            )
            let identifierKey = try RankValueOrdering.identifierKey(for: item.id)
            entries.append(
                RankValueEntry(
                    item: item,
                    value: value,
                    identifierKey: identifierKey
                )
            )
        }

        let direction: RankValueDirection
        switch order {
        case .ascending:
            direction = .ascending
        case .descending:
            direction = .descending
        }
        let sorted = try RankValueOrdering.sorted(entries, direction: direction)

        // Convert rank to score (1st = 1.0, last = 0.0)
        let count = Double(sorted.count)
        return sorted.enumerated().map { index, tuple in
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            return ScoredResult(item: tuple.item, score: score)
        }
    }
}
