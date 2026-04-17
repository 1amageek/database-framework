import Foundation
import Synchronization
import Core
import QueryIR
import DatabaseClientProtocol

public protocol IndexReadExecutor: Sendable {
    var kindIdentifier: String { get }

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse
}

public protocol PolymorphicIndexReadExecutor: Sendable {
    var kindIdentifier: String { get }

    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse
}

public protocol FusionReadExecutor: Sendable {
    var strategyIdentifier: String { get }

    func execute<T: Persistable>(
        context: FDBContext,
        selectQuery: SelectQuery,
        fusionSource: FusionSource,
        as type: T.Type,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse
}

public final class ReadExecutorRegistry: Sendable {
    public static let shared = ReadExecutorRegistry()

    private struct State: Sendable {
        var indexExecutors: [String: any IndexReadExecutor] = [:]
        var polymorphicIndexExecutors: [String: any PolymorphicIndexReadExecutor] = [:]
        var fusionExecutors: [String: any FusionReadExecutor] = [:]
    }

    private let state = Mutex(State())

    public init() {}

    public func register(_ executor: any IndexReadExecutor) {
        state.withLock { $0.indexExecutors[executor.kindIdentifier] = executor }
    }

    public func registerPolymorphic(_ executor: any PolymorphicIndexReadExecutor) {
        state.withLock { $0.polymorphicIndexExecutors[executor.kindIdentifier] = executor }
    }

    public func register(_ executor: any FusionReadExecutor) {
        state.withLock { $0.fusionExecutors[executor.strategyIdentifier] = executor }
    }

    public func indexExecutor(for kindIdentifier: String) -> (any IndexReadExecutor)? {
        state.withLock { $0.indexExecutors[kindIdentifier] }
    }

    public func polymorphicIndexExecutor(for kindIdentifier: String) -> (any PolymorphicIndexReadExecutor)? {
        state.withLock { $0.polymorphicIndexExecutors[kindIdentifier] }
    }

    public func fusionExecutor(for strategyIdentifier: String) -> (any FusionReadExecutor)? {
        state.withLock { $0.fusionExecutors[strategyIdentifier] }
    }
}
