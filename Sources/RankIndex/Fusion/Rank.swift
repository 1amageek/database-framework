// Rank.swift
// RankIndex - Rank-based scoring query for Fusion
//
// This file is part of RankIndex module, not DatabaseEngine.
// Rank is a reranking operation that scores items based on a numeric field.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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

    /// Create a Rank query for an Int field
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
    public init(_ keyPath: KeyPath<T, Int>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an Int64 field
    public init(_ keyPath: KeyPath<T, Int64>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for a Double field
    public init(_ keyPath: KeyPath<T, Double>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for a Float field
    public init(_ keyPath: KeyPath<T, Float>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an optional Int field
    public init(_ keyPath: KeyPath<T, Int?>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an optional Double field
    public init(_ keyPath: KeyPath<T, Double?>) {
        guard let context = FusionContext.current else {
            fatalError("Rank must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Rank query for an Int field with explicit context
    public init(_ keyPath: KeyPath<T, Int>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an Int64 field with explicit context
    public init(_ keyPath: KeyPath<T, Int64>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for a Double field with explicit context
    public init(_ keyPath: KeyPath<T, Double>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for a Float field with explicit context
    public init(_ keyPath: KeyPath<T, Float>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an optional Int field with explicit context
    public init(_ keyPath: KeyPath<T, Int?>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Rank query for an optional Double field with explicit context
    public init(_ keyPath: KeyPath<T, Double?>, context: IndexQueryContext) {
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

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        // Rank query requires candidates from previous stages
        // It should not be used as the first stage
        guard let candidateIds = candidates, !candidateIds.isEmpty else {
            // Return empty - Rank is designed for reranking, not initial search
            // If used as first stage, it contributes nothing to the fusion
            return []
        }

        // Fetch items
        let items = try await queryContext.fetchItemsByStringIds(
            type: T.self,
            ids: Array(candidateIds)
        )

        // Extract numeric values
        let itemsWithValue: [(item: T, value: Double)] = items.compactMap { item in
            guard let rawValue = item[dynamicMember: fieldName] else { return nil }

            let doubleValue: Double
            switch rawValue {
            case let v as Double:
                doubleValue = v
            case let v as Float:
                doubleValue = Double(v)
            case let v as Int:
                doubleValue = Double(v)
            case let v as Int64:
                doubleValue = Double(v)
            case let v as Int32:
                doubleValue = Double(v)
            case let v as Int16:
                doubleValue = Double(v)
            case let v as Int8:
                doubleValue = Double(v)
            case let v as UInt:
                doubleValue = Double(v)
            case let v as UInt64:
                doubleValue = Double(v)
            case let v as UInt32:
                doubleValue = Double(v)
            default:
                return nil
            }

            return (item: item, value: doubleValue)
        }

        guard !itemsWithValue.isEmpty else {
            return items.map { ScoredResult(item: $0, score: 0.5) }
        }

        // Sort by value
        let sorted: [(item: T, value: Double)]
        switch order {
        case .ascending:
            sorted = itemsWithValue.sorted { $0.value < $1.value }
        case .descending:
            sorted = itemsWithValue.sorted { $0.value > $1.value }
        }

        // Convert rank to score (1st = 1.0, last = 0.0)
        let count = Double(sorted.count)
        return sorted.enumerated().map { index, tuple in
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            return ScoredResult(item: tuple.item, score: score)
        }
    }
}
