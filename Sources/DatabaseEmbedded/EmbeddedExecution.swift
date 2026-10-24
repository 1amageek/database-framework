import Database
import DatabaseAccess
import DatabaseEngine
import DatabaseKit
import DatabaseWire

/// Cleanup ownership without a second execution queue, counter or query engine.
actor EmbeddedExecution: DatabaseExecuting {
    private let connection: QueryConnection
    private let container: DBContainer?
    private var closingTask: Task<Void, Never>?

    init(connection: QueryConnection, container: DBContainer?) {
        self.connection = connection
        self.container = container
    }

    nonisolated func execute(_ input: QueryInput, parameters: [QueryParameter],
        options: QueryExecutionOptions, metadata: OperationRequestMetadata) async throws -> AccessResult {
        guard metadata.traceID == nil && metadata.idempotencyKey == nil else {
            throw EmbeddedAccessError.unsupportedRequestMetadata
        }
        let result = try await connection.execute(input, parameters: parameters, options: options)
        return try adapt(result)
    }

    func close() async {
        if let closingTask { await closingTask.value; return }
        let connection = connection, container = container
        let cleanup = Task {
            await connection.close()
            if let container { await container.shutdown() }
        }
        closingTask = cleanup
        await cleanup.value
    }

    private nonisolated func adapt(_ result: QueryExecuteOperation.Response) throws -> AccessResult {
        switch result {
        case .rows(let page): return .rows(try RowStream(page: page))
        case .rdfGraph(let page): return .rdfGraph(try GraphStream(page: page))
        case .boolean(let value): return .boolean(value)
        case .command(let kind, let count): return .command(kind, affectedRows: count)
        case .copy(let bytes, let count): return .copy(bytes: bytes, affectedRows: count)
        case .block(let results): return .block(try results.map(adapt))
        }
    }
}
