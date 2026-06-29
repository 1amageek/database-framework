// FusionBuilder.swift
// DatabaseEngine - Builder for fusion queries with ResultBuilder support

import Foundation
import Core

// MARK: - FusionBuilder

/// Builder for creating and executing fusion queries
///
/// FusionBuilder combines multiple queries using a pipeline or parallel
/// execution model, then applies a fusion algorithm to merge results.
///
/// **Pipeline Execution**:
/// Each query added with the builder executes sequentially, with candidates
/// from earlier stages restricting later stages.
///
/// **Parallel Execution**:
/// Use `Parallel { }` to execute multiple queries concurrently within a stage.
///
/// **Usage**:
/// ```swift
/// let qc = context.indexQueryContext
///
/// let results = try await context.fuse(Product.self) {
///     // Stage 1: FullText search
///     qc.search(Product.self, \.description).terms(["organic", "coffee"])
///
///     // Stage 2: Vector + Spatial in parallel
///     Parallel {
///         qc.similar(Product.self, \.embedding, dimensions: 384).query(vector, k: 100)
///         qc.nearby(Product.self, \.location).within(radiusKm: 5, of: here)
///     }
///
///     // Stage 3: Rank by popularity
///     qc.rank(Product.self, \.popularity)
/// }
/// .algorithm(.rrf())
/// .limit(10)
/// ```
///
/// Alternatively, you can pass context directly to query constructors:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.description, context: context.indexQueryContext).terms(["coffee"])
///     Similar(\.embedding, dimensions: 384, context: context.indexQueryContext).query(vector, k: 100)
/// }
/// .execute()
/// ```
public struct FusionBuilder<T: Persistable>: Sendable {

    private let stages: [any FusionStage<T>]
    private var algorithm: Algorithm
    private var limitCount: Int?

    // MARK: - Algorithm

    /// Fusion algorithm for combining results
    public enum Algorithm: Sendable {
        /// Reciprocal Rank Fusion
        ///
        /// Combines results based on their rank positions across sources.
        /// Items appearing in multiple sources receive higher scores.
        ///
        /// Formula: `score(d) = Σ 1/(k + rank_i(d))`
        ///
        /// - Parameter k: Rank constant (default: 60, higher = smoother blending)
        ///
        /// Reference: Cormack et al., "Reciprocal Rank Fusion outperforms
        /// Condorcet and individual Rank Learning Methods" (SIGIR 2009)
        case rrf(k: Int = 60)

        /// Sum of normalized scores
        ///
        /// Adds together the normalized scores from each source.
        /// Good when scores from different sources are comparable.
        case sum

        /// Maximum score
        ///
        /// Takes the maximum score from any source.
        /// Good when you want the best match from any single source.
        case max

        /// Weighted sum of scores
        ///
        /// Applies weights to each query result set before summing.
        /// **Important**: Weights are per-query, not per-stage. A Parallel stage
        /// with 2 queries counts as 2 sources.
        ///
        /// **Example**:
        /// ```swift
        /// context.fuse(Product.self) {
        ///     Search(...)      // source 0
        ///     Parallel {
        ///         Similar(...) // source 1
        ///         Nearby(...)  // source 2
        ///     }
        ///     Rank(...)        // source 3
        /// }
        /// .algorithm(.weighted([0.3, 0.3, 0.2, 0.2]))
        /// ```
        ///
        /// - Parameter weights: Array of weights (one per query source)
        case weighted([Double])
    }

    // MARK: - Initialization

    internal init(
        stages: [any FusionStage<T>],
        algorithm: Algorithm = .rrf(),
        limit: Int? = nil
    ) {
        self.stages = stages
        self.algorithm = algorithm
        self.limitCount = limit
    }

    // MARK: - Configuration

    /// Set the fusion algorithm
    ///
    /// - Parameter algorithm: The algorithm to use for combining results
    /// - Returns: Updated builder
    public func algorithm(_ algorithm: Algorithm) -> Self {
        var copy = self
        copy.algorithm = algorithm
        return copy
    }

    /// Limit the number of results
    ///
    /// - Parameter count: Maximum number of results to return
    /// - Returns: Updated builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    // MARK: - Execution

    /// Execute the fusion query
    ///
    /// Executes all stages in order, applying candidate filtering between
    /// stages, then combines results using the specified fusion algorithm.
    ///
    /// - Returns: Array of scored results, sorted by score descending
    public func execute() async throws -> [ScoredResult<T>] {
        guard !stages.isEmpty else { return [] }

        var candidateIds: Set<String>? = nil
        var allResults: [[ScoredResult<T>]] = []

        // Execute stages sequentially
        for (stageIndex, stage) in stages.enumerated() {
            // Stage 0 has no candidate restriction
            // Subsequent stages filter to candidates from previous stages
            let stageCandidates = stageIndex > 0 ? candidateIds : nil

            let stageResults = try await stage.execute(candidates: stageCandidates)

            // Update candidate set (intersection of all results in this stage)
            var stageIds: Set<String> = []
            for results in stageResults {
                for result in results {
                    stageIds.insert(itemId(result.item))
                }
            }

            if candidateIds == nil {
                candidateIds = stageIds
            } else {
                candidateIds = candidateIds!.intersection(stageIds)
            }

            // Collect all results for fusion
            allResults.append(contentsOf: stageResults)
        }

        // Filter all results to final candidate set
        // This ensures items filtered out in later stages don't appear in fusion
        let filteredResults: [[ScoredResult<T>]]
        if let finalCandidates = candidateIds, !finalCandidates.isEmpty {
            filteredResults = allResults.map { results in
                results.filter { finalCandidates.contains(itemId($0.item)) }
            }
        } else {
            filteredResults = allResults
        }

        // Apply fusion algorithm
        var fused = applyAlgorithm(algorithm, to: filteredResults)

        // Apply limit
        if let limit = limitCount {
            fused = Array(fused.prefix(limit))
        }

        return fused
    }

    // MARK: - Private Helpers

    private func itemId(_ item: T) -> String {
        "\(item.id)"
    }

    private func applyAlgorithm(
        _ algorithm: Algorithm,
        to sources: [[ScoredResult<T>]]
    ) -> [ScoredResult<T>] {
        var scores: [String: (item: T, score: Double)] = [:]

        switch algorithm {
        case .rrf(let k):
            for source in sources {
                for (rank, result) in source.enumerated() {
                    let id = itemId(result.item)
                    let rrfScore = 1.0 / Double(k + rank + 1)
                    if let existing = scores[id] {
                        scores[id] = (existing.item, existing.score + rrfScore)
                    } else {
                        scores[id] = (result.item, rrfScore)
                    }
                }
            }

        case .sum:
            for source in sources {
                for result in source {
                    let id = itemId(result.item)
                    if let existing = scores[id] {
                        scores[id] = (existing.item, existing.score + result.score)
                    } else {
                        scores[id] = (result.item, result.score)
                    }
                }
            }

        case .max:
            for source in sources {
                for result in source {
                    let id = itemId(result.item)
                    if let existing = scores[id] {
                        scores[id] = (existing.item, Swift.max(existing.score, result.score))
                    } else {
                        scores[id] = (result.item, result.score)
                    }
                }
            }

        case .weighted(let weights):
            for (sourceIndex, source) in sources.enumerated() {
                let weight = sourceIndex < weights.count ? weights[sourceIndex] : 1.0
                for result in source {
                    let id = itemId(result.item)
                    let weightedScore = result.score * weight
                    if let existing = scores[id] {
                        scores[id] = (existing.item, existing.score + weightedScore)
                    } else {
                        scores[id] = (result.item, weightedScore)
                    }
                }
            }
        }

        return scores.values
            .sorted { $0.score > $1.score }
            .map { ScoredResult(item: $0.item, score: $0.score) }
    }
}

// MARK: - FusionStageBuilder

/// ResultBuilder for constructing fusion stages
@resultBuilder
public struct FusionStageBuilder<T: Persistable> {

    /// Build a block of stages
    public static func buildBlock(_ stages: (any FusionStage<T>)...) -> [any FusionStage<T>] {
        stages
    }

    /// Convert a single query to a SingleStage
    public static func buildExpression(_ query: some FusionQuery<T>) -> any FusionStage<T> {
        SingleStage(query: query)
    }

    /// Pass through Parallel stages
    public static func buildExpression(_ parallel: Parallel<T>) -> any FusionStage<T> {
        parallel
    }

    /// Handle optional stages
    public static func buildOptional(_ stage: (any FusionStage<T>)?) -> [any FusionStage<T>] {
        if let stage = stage {
            return [stage]
        }
        return []
    }

    /// Handle if-else first branch
    public static func buildEither(first stage: [any FusionStage<T>]) -> [any FusionStage<T>] {
        stage
    }

    /// Handle if-else second branch
    public static func buildEither(second stage: [any FusionStage<T>]) -> [any FusionStage<T>] {
        stage
    }

    /// Handle arrays (for loops)
    public static func buildArray(_ components: [[any FusionStage<T>]]) -> [any FusionStage<T>] {
        components.flatMap { $0 }
    }

    /// Convert array to final result
    public static func buildFinalResult(_ stages: [any FusionStage<T>]) -> [any FusionStage<T>] {
        stages
    }

    /// Handle single stage in block context (needed for some Swift versions)
    public static func buildBlock(_ stage: any FusionStage<T>) -> [any FusionStage<T>] {
        [stage]
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Create a fusion query combining multiple search sources
    ///
    /// Fusion enables hybrid search by combining results from different
    /// query types (FullText, Vector, Spatial, etc.) using various
    /// fusion algorithms like Reciprocal Rank Fusion (RRF).
    ///
    /// **Pipeline Execution**:
    /// Queries execute sequentially, with each stage restricting candidates
    /// for subsequent stages. This enables optimization where fast queries
    /// (e.g., scalar filters) narrow the search space for slower queries
    /// (e.g., vector search).
    ///
    /// **Parallel Execution**:
    /// Use `Parallel { }` to execute multiple queries concurrently within
    /// a single stage.
    ///
    /// **Usage**:
    /// ```swift
    /// let qc = context.indexQueryContext
    ///
    /// // Simple hybrid search
    /// let results = try await context.fuse(Product.self) {
    ///     qc.search(Product.self, \.description).terms(["coffee"])
    ///     qc.similar(Product.self, \.embedding, dimensions: 384).query(vector, k: 100)
    /// }
    /// .algorithm(.rrf())
    /// .limit(10)
    ///
    /// // Pipeline with parallel stage
    /// let results = try await context.fuse(Product.self) {
    ///     qc.filter(Product.self, \.category, equals: "electronics")
    ///
    ///     Parallel {
    ///         qc.search(Product.self, \.description).terms(["wireless"])
    ///         qc.similar(Product.self, \.embedding, dimensions: 384).query(vector, k: 100)
    ///     }
    ///
    ///     qc.rank(Product.self, \.rating).order(.descending)
    /// }
    /// .algorithm(.weighted([0.2, 0.4, 0.4]))
    /// .limit(20)
    /// ```
    ///
    /// - Parameters:
    ///   - type: The Persistable type to search
    ///   - content: Builder closure containing fusion stages
    /// - Returns: A FusionBuilder for configuring and executing the query
    public func fuse<T: Persistable>(
        _ type: T.Type,
        @FusionStageBuilder<T> _ content: () -> [any FusionStage<T>]
    ) -> FusionBuilder<T> {
        // Make indexQueryContext available via FusionContext.current during stage building
        let stages = FusionContext.withContext(indexQueryContext) {
            content()
        }
        return FusionBuilder(stages: stages)
    }
}
