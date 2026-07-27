// HNSWPostFilterParameters.swift
// VectorIndex - HNSW candidate oversampling and post-filter parameters

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Controls candidate oversampling before an application predicate is evaluated.
///
/// The HNSW graph search is completed first. The returned candidates are then
/// fetched and evaluated in distance order. This contract does not claim that
/// the predicate participates in graph traversal and therefore does not provide
/// ACORN semantics.
///
/// **Usage**:
/// ```swift
/// let results = try await context.findSimilar(Product.self)
///     .vector(Product.fields.embedding, dimensions: 384)
///     .query(queryVector, k: 10)
///     .filter { product in product.category == "electronics" }
///     .postFilter(expansionFactor: 3)
///     .execute()
/// ```
public struct HNSWPostFilterParameters: Sendable, Hashable {

    /// ef expansion factor
    ///
    /// Multiplier for the candidate count and HNSW exploration factor.
    /// Higher values improve recall but increase latency.
    ///
    /// **Recommendation**:
    /// - 2: Good balance for moderately selective predicates
    /// - 3-5: Better recall for highly selective predicates
    /// - 1: Standard HNSW candidate count followed by filtering
    ///
    /// **Default**: 2
    public let expansionFactor: Int

    /// Maximum predicate evaluations
    ///
    /// Optional limit on the number of predicate evaluations per search.
    /// Useful for expensive predicates (e.g., those requiring additional I/O).
    ///
    /// When the limit is reached, remaining candidates are not fetched or
    /// evaluated.
    ///
    /// **Default**: nil (unlimited)
    public let maxPredicateEvaluations: Int?

    /// Default post-filter parameters
    ///
    /// - expansionFactor: 2
    /// - maxPredicateEvaluations: nil (unlimited)
    public static let `default` = HNSWPostFilterParameters(expansionFactor: 2)

    /// Initialize with custom parameters
    ///
    /// - Parameters:
    ///   - expansionFactor: ef expansion multiplier, validated when search executes
    ///   - maxPredicateEvaluations: Optional limit on predicate evaluations
    public init(expansionFactor: Int = 2, maxPredicateEvaluations: Int? = nil) {
        self.expansionFactor = expansionFactor
        self.maxPredicateEvaluations = maxPredicateEvaluations
    }

    /// Low expansion for less selective predicates
    ///
    /// Use when the predicate matches most items (> 50%).
    public static let lowExpansion = HNSWPostFilterParameters(expansionFactor: 1)

    /// Medium expansion (default)
    ///
    /// Good balance for moderately selective predicates (10-50% match rate).
    public static let mediumExpansion = HNSWPostFilterParameters(expansionFactor: 2)

    /// High expansion for highly selective predicates
    ///
    /// Use when the predicate matches few items (< 10%).
    public static let highExpansion = HNSWPostFilterParameters(expansionFactor: 4)
}
