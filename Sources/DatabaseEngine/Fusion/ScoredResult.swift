// ScoredResult.swift
// DatabaseEngine - Fusion query result type

import DatabaseKit

/// Scored search result from fusion queries
///
/// Represents an item with its relevance score after fusion.
/// A higher score is better. The numeric range depends on the selected Fusion
/// strategy; reciprocal-rank and multi-input sums are not constrained to 1.
///
public struct ScoredResult<T: Persistable>: Sendable {
    /// The matched item
    public let item: T

    /// Relevance score. Higher is better; its range depends on the strategy.
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
