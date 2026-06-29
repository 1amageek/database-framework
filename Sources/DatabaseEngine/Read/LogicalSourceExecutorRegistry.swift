import Foundation
import Synchronization
import QueryIR
import DatabaseClientProtocol

public protocol GraphTableSourceExecutor: Sendable {
    func execute(
        context: FDBContext,
        graphTableSource: GraphTableSource,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> [QueryRow]
}

public protocol SPARQLSourceExecutor: Sendable {
    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        options: ReadExecutionOptions,
        partitionValues: [String: String]?
    ) async throws -> QueryResponse
}

public final class LogicalSourceExecutorRegistry: Sendable {
    public static let shared = LogicalSourceExecutorRegistry()

    private struct State: Sendable {
        var graphTableExecutor: (any GraphTableSourceExecutor)?
        var sparqlExecutor: (any SPARQLSourceExecutor)?
    }

    private let state = Mutex(State())

    public init() {}

    public func register(_ executor: any GraphTableSourceExecutor) {
        state.withLock { $0.graphTableExecutor = executor }
    }

    public func register(_ executor: any SPARQLSourceExecutor) {
        state.withLock { $0.sparqlExecutor = executor }
    }

    public var graphTableExecutor: (any GraphTableSourceExecutor)? {
        state.withLock { $0.graphTableExecutor }
    }

    public var sparqlExecutor: (any SPARQLSourceExecutor)? {
        state.withLock { $0.sparqlExecutor }
    }
}
