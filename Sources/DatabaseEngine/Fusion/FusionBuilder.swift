// FusionBuilder.swift
// DatabaseEngine - Builder for fusion queries with ResultBuilder support

import DatabaseKit
import StorageKit

private struct FusionBuilderStorageClock: StorageMonotonicClock {
    private static let clock = ContinuousClock()
    private static let origin = clock.now

    var now: StorageInstant {
        StorageInstant(
            durationSinceReference: Self.origin.duration(to: Self.clock.now)
        )
    }

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {
        let remaining = now.duration(to: deadline)
        guard remaining > .zero else { return }
        do {
            try await Self.clock.sleep(for: remaining)
        } catch {
            throw .cancelled
        }
    }
}

// MARK: - FusionBuilder

/// Builder for creating and executing fusion queries
///
/// FusionBuilder combines multiple queries using a pipeline or parallel
/// execution model, then applies a canonical fusion strategy to merge results.
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
/// .strategy(.reciprocalRank())
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

    private let context: DatabaseContext?
    private let stages: [any FusionStage<T>]
    private var strategy: FusionStrategy
    private var limitCount: Int?

    // MARK: - Initialization

    internal init(
        stages: [any FusionStage<T>],
        context: DatabaseContext? = nil,
        strategy: FusionStrategy = .reciprocalRank(),
        limit: Int? = nil
    ) {
        self.context = context
        self.stages = stages
        self.strategy = strategy
        self.limitCount = limit
    }

    // MARK: - Configuration

    /// Set the canonical fusion strategy.
    ///
    /// Weighted strategies use one weight per query source, including each
    /// query inside a parallel stage.
    ///
    /// - Parameter strategy: The strategy used to combine ordered inputs.
    /// - Returns: Updated builder
    public func strategy(_ strategy: FusionStrategy) -> Self {
        var copy = self
        copy.strategy = strategy
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
    /// stages, then combines results using the specified fusion strategy.
    ///
    /// - Returns: Array of scored results, sorted by score descending
    public func execute(
        options: ReadExecutionOptions = .default
    ) async throws -> [ScoredResult<T>] {
        try validateConfiguration()
        guard !stages.isEmpty else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion requires at least one input"
            )
        }

        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: context?.container.monotonicClock
                ?? FusionBuilderStorageClock()
        )
        guard let context else {
            return try await executeStages(execution: execution)
        }
        let canonicalRead = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        return try await context.executeCanonicalRead(
            configuration: canonicalRead.transactionConfiguration
        ) { _ in
            try context.indexQueryContext.authorizeListAccess(
                entityName: T.persistableType,
                authorization: IndexReadAuthorization(
                    limit: limitCount,
                    offset: nil,
                    orderBy: ["score"]
                )
            )
            return try await executeStages(execution: execution)
        }
    }

    private func executeStages(
        execution: ReadExecutionContext
    ) async throws -> [ScoredResult<T>] {
        let workMeter = execution.workMeter

        var candidateIDs: Set<T.ID>? = nil
        var candidateReservation: DatabaseIntermediateReservation?
        defer { candidateReservation?.release() }
        var allResultsBuilder = try FusionStageResultBuilder<T>(
            execution: execution,
            expectedCount: stages.count
        )
        // Execute stages sequentially
        for (stageIndex, stage) in stages.enumerated() {
            // Stage 0 has no candidate restriction
            // Subsequent stages filter to candidates from previous stages
            let stageCandidates = stageIndex > 0 ? candidateIDs : nil

            let stageResults = try await stage.execute(
                candidates: stageCandidates,
                execution: execution
            )
            let retainedStageResults = stageResults.retainedElements
            let resultCount = retainedStageResults.reduce(into: 0) {
                $0 += $1.count
            }
            try workMeter.consume(UInt64(resultCount), at: .indexScan)
            guard retainedStageResults.allSatisfy({ results in
                results.retainedElements.allSatisfy { $0.score.isFinite }
            }) else {
                throw FusionQueryError.invalidConfiguration(
                    "Fusion sources must produce finite scores"
                )
            }

            // A parallel stage admits the union of its query results. Each
            // later stage intersects that union with the prior candidates.
            var stageIDReservation: DatabaseIntermediateReservation? =
                try workMeter.reserveIntermediate(
                    rows: UInt64(resultCount),
                    bytes: try Self.identitySetFootprint(
                        count: resultCount
                    ).bytes,
                    at: .deduplication
                )
            defer { stageIDReservation?.release() }
            var stageIDs = Set<T.ID>(minimumCapacity: resultCount)
            for results in retainedStageResults {
                for result in results.retainedElements {
                    stageIDs.insert(result.item.id)
                }
            }
            let unusedStageIdentityCount = resultCount - stageIDs.count
            if unusedStageIdentityCount > 0 {
                try stageIDReservation?.releasePartial(
                    rows: UInt64(unusedStageIdentityCount)
                )
            }

            if let previousCandidates = candidateIDs {
                let maximumIntersectionCount = min(
                    previousCandidates.count,
                    stageIDs.count
                )
                let nextReservation = try workMeter.reserveIntermediate(
                    rows: UInt64(maximumIntersectionCount),
                    bytes: try Self.identitySetFootprint(
                        count: maximumIntersectionCount
                    ).bytes,
                    at: .deduplication
                )
                var ownsNextReservation = true
                defer {
                    if ownsNextReservation { nextReservation.release() }
                }
                let nextCandidates = previousCandidates.intersection(stageIDs)
                let unusedIntersectionCount = maximumIntersectionCount
                    - nextCandidates.count
                if unusedIntersectionCount > 0 {
                    try nextReservation.releasePartial(
                        rows: UInt64(unusedIntersectionCount)
                    )
                }
                candidateIDs = nextCandidates
                candidateReservation?.release()
                candidateReservation = nextReservation
                ownsNextReservation = false
            } else {
                candidateIDs = stageIDs
                candidateReservation = stageIDReservation
                stageIDReservation = nil
            }

            for result in retainedStageResults {
                try allResultsBuilder.append(result)
            }
        }
        let allResults = try allResultsBuilder.finish()
        guard !allResults.isEmpty else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion requires at least one input"
            )
        }

        // Apply the strategy directly over eligible rows so the pipeline does
        // not materialize a second copy of every source result.
        let fusedResults: DatabaseSharedRetainedArray<
            CanonicalFusionAlgebraResult<ReferenceIdentifier, ScoredResult<T>>
        >
        do {
            fusedResults = try CanonicalFusionAlgebra.fuse(
                sources: allResults.retainedElements.lazy.map(
                    \.retainedElements
                ),
                orderedSources: allResults.retainedElements.lazy.map {
                    $0.ordering.isRankOrdered
                },
                strategy: strategy,
                isEligible: { result in
                    candidateIDs?.contains(result.item.id) ?? true
                },
                workMeter: workMeter,
                identity: { result in
                    result.item.id.persistableIdentifierValue
                },
                signal: { result in
                    .higherIsBetter(result.score)
                },
                payloadsAreEquivalent: { lhs, rhs in
                    try Self.payloadsAreEquivalent(lhs.item, rhs.item)
                }
            )
        } catch let error as CanonicalFusionAlgebraError<ReferenceIdentifier> {
            throw FusionQueryError.invalidConfiguration(
                Self.describe(error)
            )
        }
        var fused: [ScoredResult<T>] = []
        let visibleCount = min(limitCount ?? fusedResults.count, fusedResults.count)
        guard let outputCount = UInt32(exactly: visibleCount) else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion output count exceeds the supported range"
            )
        }
        try workMeter.recordOutputRows(outputCount)
        fused.reserveCapacity(visibleCount)
        for entry in fusedResults.prefix(visibleCount) {
            fused.append(
                ScoredResult(item: entry.payload.item, score: entry.score)
            )
        }

        return fused
    }

    private static func identitySetFootprint(
        count: Int
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<Set<T.ID>>.stride)
        ).adding(identitySetElementFootprint(count: count))
    }

    private static func payloadsAreEquivalent(
        _ lhs: borrowing T,
        _ rhs: borrowing T
    ) throws -> Bool {
        for name in T.allFields {
            guard try lhs.persistedValue(forFieldNamed: name)
                == rhs.persistedValue(forFieldNamed: name) else {
                return false
            }
        }
        return true
    }

    private static func identitySetElementFootprint(
        count: Int
    ) throws -> DatabaseIntermediateFootprint {
        guard count >= 0 else {
            throw FusionQueryError.invalidConfiguration(
                "Fusion identity count must not be negative"
            )
        }
        return try DatabaseIntermediateFootprint(
            rows: 1,
            bytes: UInt64(max(1, MemoryLayout<T.ID>.stride) + 48)
        ).multiplied(by: UInt64(count))
    }

    private static func describe(
        _ error: CanonicalFusionAlgebraError<ReferenceIdentifier>
    ) -> String {
        switch error {
        case .weightCountMismatch(let expected, let actual):
            return "Weighted fusion requires \(expected) weights, got \(actual)"
        case .nonFiniteWeight:
            return "Fusion weights must be finite"
        case .negativeWeight:
            return "Fusion weights must not be negative"
        case .duplicateIdentity:
            return "A fusion input produced a duplicate identity"
        case .inconsistentPayload:
            return "Fusion inputs disagree about one persisted identity"
        case .inconsistentSignal:
            return "A fusion input produced inconsistent score signals"
        case .nonFiniteSignal:
            return "Fusion sources must produce finite scores"
        case .unorderedRankSource:
            return "Rank fusion requires an ordered source"
        case .scoreOverflow:
            return "Fusion score accumulation exceeded the finite range"
        case .inputCountOverflow:
            return "Fusion input count exceeds the supported range"
        }
    }

    // MARK: - Private Helpers

    private func validateConfiguration() throws {
        if let limitCount, limitCount < 0 {
            throw FusionQueryError.invalidConfiguration(
                "Fusion result limit must not be negative"
            )
        }
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

// MARK: - DatabaseContext Extension

extension DatabaseContext {

    /// Create a fusion query combining multiple search sources
    ///
    /// Fusion enables hybrid search by combining results from different
    /// query types (FullText, Vector, Spatial, etc.) using various
    /// fusion strategies like Reciprocal Rank Fusion (RRF).
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
    /// .strategy(.reciprocalRank())
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
    /// .strategy(.weighted([0.2, 0.4, 0.4]))
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
        return FusionBuilder(stages: stages, context: self)
    }
}
