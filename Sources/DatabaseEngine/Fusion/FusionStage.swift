// FusionStage.swift
// DatabaseEngine - Execution stages for fusion queries

import DatabaseKit

public struct FusionStagePlan<Item: Persistable>: Sendable {
    public typealias Operation = @Sendable (
        Set<Item.ID>?,
        ReadExecutionContext
    ) async throws -> FusionStageResult<Item>

    private let queryContext: IndexQueryContext?
    private let operation: Operation

    public init(operation: @escaping Operation) {
        self.queryContext = nil
        self.operation = operation
    }

    public init(
        context: IndexQueryContext,
        operation: @escaping Operation
    ) {
        self.queryContext = context
        self.operation = operation
    }

    public func execute(
        candidates: Set<Item.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionStageResult<Item> {
        guard let queryContext else {
            let result = try await operation(candidates, execution)
            try result.validateWorkMeter(execution.workMeter)
            return result
        }
        let canonicalRead = CanonicalReadExecution.resolve(
            requested: execution.options.consistency,
            default: .snapshot
        )
        return try await queryContext.withReadSnapshot(
            configuration: canonicalRead.transactionConfiguration
        ) {
            let result = try await operation(candidates, execution)
            try result.validateWorkMeter(execution.workMeter)
            return result
        }
    }
}

/// Protocol for execution stages in a fusion pipeline
///
/// A stage represents one or more queries that execute together.
/// Stages execute sequentially in a pipeline, with each stage
/// potentially restricting candidates for subsequent stages.
public protocol FusionStage<Item>: Sendable {
    /// The item type this stage returns
    associatedtype Item: Persistable

    /// Execute the stage and return results from all queries
    ///
    /// - Parameter candidates: Optional candidate IDs from previous stages
    /// - Returns: Array of result arrays (one per query in the stage)
    var fusionStagePlan: FusionStagePlan<Item> { get }
}

public extension FusionStage {
    func execute(
        candidates: Set<Item.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionStageResult<Item> {
        try await fusionStagePlan.execute(
            candidates: candidates,
            execution: execution
        )
    }
}

// MARK: - SingleStage

/// A stage containing a single query
///
/// Used internally by FusionBuilder when a single query is added.
public struct SingleStage<T: Persistable>: FusionStage {
    public typealias Item = T

    let query: any FusionQuery<T>

    public init(query: any FusionQuery<T>) {
        self.query = query
    }

    public var fusionStagePlan: FusionStagePlan<T> {
        let operation: FusionStagePlan<T>.Operation = {
            candidates,
            execution in
            let results = try await query.execute(
                candidates: candidates,
                execution: execution
            )
            var output = try FusionStageResultBuilder<T>(
                execution: execution,
                expectedCount: 1
            )
            try output.append(results)
            return try output.finish()
        }
        if let context = query.fusionQueryPlan.executionContext {
            return FusionStagePlan(context: context, operation: operation)
        }
        return FusionStagePlan(operation: operation)
    }
}

// MARK: - Parallel

/// A stage containing multiple queries that execute in parallel
///
/// All queries in a Parallel stage run concurrently using TaskGroup.
/// Their results are collected and returned as separate arrays.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.title).terms(["coffee"])
///
///     Parallel {
///         Similar(\.embedding, dimensions: 384).query(vector, k: 100)
///         Nearby(\.location).within(radiusKm: 5, of: here)
///     }
/// }
/// .execute()
/// ```
public struct Parallel<T: Persistable>: FusionStage {
    public typealias Item = T

    let queries: [any FusionQuery<T>]

    /// Create a parallel stage using ResultBuilder
    ///
    /// - Parameter content: Builder closure containing queries
    public init(@FusionQueryBuilder<T> _ content: () -> [any FusionQuery<T>]) {
        self.queries = content()
    }

    /// Create a parallel stage from an array of queries
    ///
    /// - Parameter queries: Array of queries to execute in parallel
    public init(queries: [any FusionQuery<T>]) {
        self.queries = queries
    }

    public var fusionStagePlan: FusionStagePlan<T> {
        let operation: FusionStagePlan<T>.Operation = {
            candidates,
            execution in
            try await executeBound(
                candidates: candidates,
                execution: execution
            )
        }
        if let context = queries.lazy.compactMap({
            $0.fusionQueryPlan.executionContext
        }).first {
            return FusionStagePlan(context: context, operation: operation)
        }
        return FusionStagePlan(operation: operation)
    }

    private func executeBound(
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionStageResult<T> {
        guard !queries.isEmpty else {
            return try FusionStageResultBuilder<T>(
                execution: execution,
                expectedCount: 0
            ).finish()
        }

        let indexedReservation = try execution.workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: queries.count,
                element: FusionQueryResult<T>?.self
            ).bytes,
            at: .indexScan
        )
        defer { indexedReservation.release() }
        let indexedResults = try await withThrowingTaskGroup(
            of: (Int, FusionQueryResult<T>).self
        ) { group in
            // Launch all queries in parallel with index tracking
            for (index, query) in queries.enumerated() {
                group.addTask {
                    let results = try await query.execute(
                        candidates: candidates,
                        execution: execution
                    )
                    return (index, results)
                }
            }

            // Collect results maintaining order
            var indexedResults = [FusionQueryResult<T>?](
                repeating: nil,
                count: queries.count
            )
            while let result = try await group.next() {
                indexedResults[result.0] = result.1
            }
            return indexedResults
        }
        var output = try FusionStageResultBuilder<T>(
            execution: execution,
            expectedCount: indexedResults.count
        )
        for (index, result) in indexedResults.enumerated() {
            guard let result else {
                throw FusionQueryError.invalidConfiguration(
                    "Parallel fusion source \(index) produced no result"
                )
            }
            try output.append(result)
        }
        return try output.finish()
    }
}

// MARK: - FusionQueryBuilder

/// ResultBuilder for constructing query arrays in Parallel stages
@resultBuilder
public struct FusionQueryBuilder<T: Persistable> {

    public static func buildBlock(_ queries: (any FusionQuery<T>)...) -> [any FusionQuery<T>] {
        queries
    }

    public static func buildExpression(_ query: some FusionQuery<T>) -> any FusionQuery<T> {
        query
    }

    public static func buildArray(_ components: [[any FusionQuery<T>]]) -> [any FusionQuery<T>] {
        components.flatMap { $0 }
    }

    public static func buildOptional(_ component: [any FusionQuery<T>]?) -> [any FusionQuery<T>] {
        component ?? []
    }

    public static func buildEither(first component: [any FusionQuery<T>]) -> [any FusionQuery<T>] {
        component
    }

    public static func buildEither(second component: [any FusionQuery<T>]) -> [any FusionQuery<T>] {
        component
    }
}
