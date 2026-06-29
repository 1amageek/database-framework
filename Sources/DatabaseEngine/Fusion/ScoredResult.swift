// ScoredResult.swift
// DatabaseEngine - Fusion query result type

import Foundation
import Core

/// Scored search result from fusion queries
///
/// Represents an item with its relevance score after fusion.
/// Score is normalized to [0, 1] range where higher is better.
///
/// **Usage**:
/// ```swift
/// let results: [ScoredResult<Product>] = try await context.fuse(Product.self) {
///     Search(\.description).terms(["coffee"])
///     Similar(\.embedding, dimensions: 384).query(vector, k: 100)
/// }
/// .execute()
///
/// for result in results {
///     print("\(result.item.name): \(result.score)")
/// }
/// ```
public struct ScoredResult<T: Persistable>: Sendable {
    /// The matched item
    public let item: T

    /// Relevance score (0.0 to 1.0, higher is better)
    public let score: Double

    public init(item: T, score: Double) {
        self.item = item
        self.score = score
    }
}

extension ScoredResult: Equatable where T: Equatable {
    public static func == (lhs: ScoredResult<T>, rhs: ScoredResult<T>) -> Bool {
        lhs.item == rhs.item && lhs.score == rhs.score
    }
}

extension ScoredResult: Hashable where T: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(item)
        hasher.combine(score)
    }
}
