// ACORNParameters.swift
// VectorIndex - ACORN filtered search parameters
//
// Reference: Patel et al., "ACORN: Performant and Predicate-Agnostic Search
// Over Vector Embeddings and Structured Data", SIGMOD 2024
// https://arxiv.org/abs/2403.04871

import Foundation

/// ACORN-1 filtered search parameters
///
/// ACORN (Approximate Containment Queries Over Real-Value Navigable Networks)
/// enables efficient filtered vector search over HNSW graphs without requiring
/// predicate-specific index structures.
///
/// **Algorithm Overview**:
/// During HNSW graph traversal, ACORN dynamically filters neighbors that don't
/// match the predicate. This emulates searching on a predicate-specific
/// "oracle partition" without actually building one.
///
/// **Key Concept: Predicate Subgraph Traversal**:
/// - Non-matching nodes are still added to the candidate queue for graph connectivity
/// - Only matching nodes are added to the result set
/// - This allows traversal through non-matching regions to reach matching nodes
///
/// **Strategies**:
/// - **ACORN-1** (implemented): Standard HNSW construction, expansion at search time
/// - **ACORN-γ**: M×γ neighbor expansion at construction (not implemented)
///
/// **Usage**:
/// ```swift
/// let results = try await context.findSimilar(Product.self)
///     .vector(\.embedding, dimensions: 384)
///     .query(queryVector, k: 10)
///     .filter { product in product.category == "electronics" }
///     .acorn(expansionFactor: 3)
///     .execute()
/// ```
public struct ACORNParameters: Sendable, Hashable {

    /// ef expansion factor
    ///
    /// Multiplier for the `ef` (exploration factor) parameter during filtered search.
    /// Higher values improve recall but increase latency.
    ///
    /// **Recommendation**:
    /// - 2: Good balance for moderately selective predicates
    /// - 3-5: Better recall for highly selective predicates
    /// - 1: No expansion (equivalent to standard HNSW with post-filtering)
    ///
    /// **Default**: 2
    public let expansionFactor: Int

    /// Maximum predicate evaluations
    ///
    /// Optional limit on the number of predicate evaluations per search.
    /// Useful for expensive predicates (e.g., those requiring additional I/O).
    ///
    /// When the limit is reached, the search continues but skips predicate
    /// evaluation for remaining candidates (they won't be added to results
    /// but still help with graph traversal).
    ///
    /// **Default**: nil (unlimited)
    public let maxPredicateEvaluations: Int?

    /// Default ACORN parameters
    ///
    /// - expansionFactor: 2
    /// - maxPredicateEvaluations: nil (unlimited)
    public static let `default` = ACORNParameters(expansionFactor: 2)

    /// Initialize with custom parameters
    ///
    /// - Parameters:
    ///   - expansionFactor: ef expansion multiplier (must be >= 1)
    ///   - maxPredicateEvaluations: Optional limit on predicate evaluations
    public init(expansionFactor: Int = 2, maxPredicateEvaluations: Int? = nil) {
        precondition(expansionFactor >= 1, "expansionFactor must be >= 1")
        self.expansionFactor = expansionFactor
        self.maxPredicateEvaluations = maxPredicateEvaluations
    }

    /// Low expansion for less selective predicates
    ///
    /// Use when the predicate matches most items (> 50%).
    public static let lowExpansion = ACORNParameters(expansionFactor: 1)

    /// Medium expansion (default)
    ///
    /// Good balance for moderately selective predicates (10-50% match rate).
    public static let mediumExpansion = ACORNParameters(expansionFactor: 2)

    /// High expansion for highly selective predicates
    ///
    /// Use when the predicate matches few items (< 10%).
    public static let highExpansion = ACORNParameters(expansionFactor: 4)
}
