import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine

extension DatabaseContext {
    /// Executes one semantic statement and publishes its outcome after closure.
    @_spi(DatabaseExecution)
    public func executeStatement(
        _ statement: QueryStatement,
        parameters: [QueryParameter] = [],
        options: SQLBlockExecutionOptions
    ) async throws -> SQLStatementResult {
        let block = try SQLBlock(steps: [.statement(statement)], limits: options.structuralLimits)
        let result = try await executeSQLBlock(block, parameters: parameters, options: options)
        return result.results[0]
    }

    /// Executes an immutable block, rolling back the entire request at its first error.
    @_spi(DatabaseExecution)
    public func executeSQLBlock(
        _ block: SQLBlock,
        parameters: [QueryParameter] = [],
        options: SQLBlockExecutionOptions
    ) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: self).execute(block, parameters: parameters, options: options)
    }
}
