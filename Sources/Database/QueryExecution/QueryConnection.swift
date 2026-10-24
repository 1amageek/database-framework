import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit

/// An invocation scope over one context; its container retains engine ownership.
public actor QueryConnection {
    private enum Phase { case open, closing, closed }
    private let context: DatabaseContext
    private let wireLimits: DatabaseWireLimits
    private let structuralLimits: QueryStructuralLimits
    private let checkpointCeiling: TransactionCheckpointLimits?
    private let concurrencyLimit: Int
    private var phase = Phase.open
    private var nextIdentifier: UInt64 = 1
    private var executions: [UInt64: Task<QueryExecuteOperation.Response, Error>] = [:]
    private var closingTask: Task<Void, Never>?

    package init(context: DatabaseContext, wireLimits: DatabaseWireLimits,
        structuralLimits: QueryStructuralLimits, checkpointCeiling: TransactionCheckpointLimits?,
        concurrencyLimit: Int) {
        self.context = context; self.wireLimits = wireLimits
        self.structuralLimits = structuralLimits; self.checkpointCeiling = checkpointCeiling
        self.concurrencyLimit = concurrencyLimit
    }

    public func execute(_ sql: SQL,
        options: QueryExecutionOptions = QueryExecutionOptions()) async throws -> QueryExecuteOperation.Response {
        try await executeInput(.sql(sql.text), parameters: sql.parameters, options: options)
    }

    public func execute(_ statement: QueryStatement, parameters: [QueryParameter] = [],
        options: QueryExecutionOptions = QueryExecutionOptions()) async throws -> QueryExecuteOperation.Response {
        try await executeInput(.statement(statement), parameters: parameters, options: options)
    }

    public func execute(_ block: SQLBlock, parameters: [QueryParameter] = [],
        options: QueryExecutionOptions = QueryExecutionOptions()) async throws -> QueryExecuteOperation.Response {
        try await executeInput(.block(block), parameters: parameters, options: options)
    }

    public func execute(_ input: QueryInput, parameters: [QueryParameter] = [],
        options: QueryExecutionOptions = QueryExecutionOptions()) async throws -> QueryExecuteOperation.Response {
        try await executeInput(input, parameters: parameters, options: options)
    }

    private func executeInput(_ input: QueryInput, parameters: [QueryParameter],
        options: QueryExecutionOptions) async throws -> QueryExecuteOperation.Response {
        guard phase == .open else { throw QueryConnectionError.closed }
        try options.validate()
        if Task.isCancelled { throw CancellationError() }
        guard executions.count < concurrencyLimit else {
            throw QueryConnectionError.concurrentExecutionLimitReached(concurrencyLimit)
        }
        guard nextIdentifier != 0 else { throw QueryConnectionError.requestIdentifierExhausted }
        let identifier = nextIdentifier
        nextIdentifier = identifier == .max ? 0 : identifier + 1
        let context = context, wireLimits = wireLimits, structuralLimits = structuralLimits
        let checkpointCeiling = checkpointCeiling
        let task = Task {
            let prepared: EncodedQueryResponse
            do {
                prepared = try await context.executeQueryEncoded(input, parameters: parameters,
                options: options, requestID: identifier, limits: wireLimits,
                structuralLimits: structuralLimits, checkpointCeiling: checkpointCeiling)
            } catch let failure as SQLExecutionFailure where failure.underlyingError is CancellationError {
                throw CancellationError()
            }
            if Task.isCancelled { throw CancellationError() }
            let frame = try prepared.promoteToOutput()
            switch try DatabaseWireDecoder(limits: wireLimits).decodeResponse(
                DatabaseOperationCatalog.queryExecute, from: frame, matching: identifier) {
            case .success(let response): return response
            case .failure(let error): throw error
            }
        }
        executions[identifier] = task
        defer { executions[identifier] = nil }
        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: { task.cancel() }
    }

    /// Stops admission and awaits cancellation cleanup without closing the container.
    public func close() async {
        if let closingTask { await closingTask.value; return }
        guard phase != .closed else { return }
        phase = .closing
        let admitted = Array(executions.values)
        for task in admitted { task.cancel() }
        let drain = Task {
            for task in admitted {
                do { _ = try await task.value }
                catch {
                    // The execute caller owns this outcome; close only awaits termination.
                }
            }
        }
        closingTask = drain
        await drain.value
        executions.removeAll()
        phase = .closed
    }
}
