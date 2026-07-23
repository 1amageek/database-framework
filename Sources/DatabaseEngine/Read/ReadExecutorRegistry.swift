#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import QueryIR

public protocol IndexReadExecutor: Sendable {
    var kindIdentifier: String { get }

    /// Produce an index-native row set.
    ///
    /// Executors must not apply SQL `WHERE` / `ORDER BY` / projection /
    /// `DISTINCT` / `LIMIT` / `OFFSET` — the dispatcher does. Executors are
    /// responsible only for producing the candidate rows ordered in index-native
    /// form (e.g. distance ascending, rank descending) together with any
    /// per-row annotations (`distance`, `score`, `rank`, …).
    func executeRows<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult
}

public protocol PolymorphicIndexReadExecutor: Sendable {
    var kindIdentifier: String { get }

    /// Produce an index-native row set for a polymorphic group. Same contract
    /// as `IndexReadExecutor.executeRows` — no SQL post-processing in executors.
    func executeRows(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> IndexReadResult
}

public protocol FusionReadExecutor: Sendable {
    var strategyIdentifier: String { get }

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        fusionSource: FusionSource,
        as type: T.Type,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> QueryResponse
}

public struct ReadExecutorRegistry: Sendable {
    private let indexExecutors: [String: any IndexReadExecutor]
    private let polymorphicIndexExecutors: [String: any PolymorphicIndexReadExecutor]
    private let fusionExecutors: [String: any FusionReadExecutor]

    public init(
        indexExecutors: [any IndexReadExecutor] = [],
        polymorphicIndexExecutors: [any PolymorphicIndexReadExecutor] = [],
        fusionExecutors: [any FusionReadExecutor] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        self.indexExecutors = try Self.indexExecutorsByIdentifier(indexExecutors)
        self.polymorphicIndexExecutors = try Self.polymorphicExecutorsByIdentifier(
            polymorphicIndexExecutors
        )
        self.fusionExecutors = try Self.fusionExecutorsByIdentifier(fusionExecutors)
    }

    public func indexExecutor(for kindIdentifier: String) -> (any IndexReadExecutor)? {
        indexExecutors[kindIdentifier]
    }

    public func polymorphicIndexExecutor(for kindIdentifier: String) -> (any PolymorphicIndexReadExecutor)? {
        polymorphicIndexExecutors[kindIdentifier]
    }

    public func fusionExecutor(for strategyIdentifier: String) -> (any FusionReadExecutor)? {
        fusionExecutors[strategyIdentifier]
    }

    private static func indexExecutorsByIdentifier(
        _ executors: [any IndexReadExecutor]
    ) throws(DatabaseRuntimeConfigurationError) -> [String: any IndexReadExecutor] {
        var result: [String: any IndexReadExecutor] = [:]
        for executor in executors {
            guard result[executor.kindIdentifier] == nil else {
                throw .duplicateIndexReadExecutor(executor.kindIdentifier)
            }
            result[executor.kindIdentifier] = executor
        }
        return result
    }

    private static func polymorphicExecutorsByIdentifier(
        _ executors: [any PolymorphicIndexReadExecutor]
    ) throws(DatabaseRuntimeConfigurationError) -> [String: any PolymorphicIndexReadExecutor] {
        var result: [String: any PolymorphicIndexReadExecutor] = [:]
        for executor in executors {
            guard result[executor.kindIdentifier] == nil else {
                throw .duplicatePolymorphicIndexReadExecutor(executor.kindIdentifier)
            }
            result[executor.kindIdentifier] = executor
        }
        return result
    }

    private static func fusionExecutorsByIdentifier(
        _ executors: [any FusionReadExecutor]
    ) throws(DatabaseRuntimeConfigurationError) -> [String: any FusionReadExecutor] {
        var result: [String: any FusionReadExecutor] = [:]
        for executor in executors {
            guard result[executor.strategyIdentifier] == nil else {
                throw .duplicateFusionReadExecutor(executor.strategyIdentifier)
            }
            result[executor.strategyIdentifier] = executor
        }
        return result
    }
}
