import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine

extension DatabaseContext {
    /// Projects a complete semantic response before commit and releases it only after closure.
    @_spi(DatabaseExecution)
    public func executeEncodedSQLBlock(_ block: SQLBlock,
        parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        encoder: any SQLBlockResponseEncoder) async throws -> EncodedQueryResponse {
        do {
            return try await SQLBlockExecutor(context: self).executeEncoded(block,
                parameters: parameters, options: options, encoder: encoder)
        } catch let failure as SQLExecutionFailure { throw failure }
        catch {
            throw SQLExecutionFailure(underlyingError: error, diagnostic: SQLDiagnostics.diagnostic(for: error))
        }
    }

    /// Encodes one semantic statement through the same aggregate lifecycle.
    @_spi(DatabaseExecution)
    public func executeEncodedStatement(_ statement: QueryStatement,
        parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        encoder: any SQLBlockResponseEncoder) async throws -> EncodedQueryResponse {
        do {
            let block = try SQLBlock(steps: [.statement(statement)], limits: options.structuralLimits)
            return try await executeEncodedSQLBlock(block, parameters: parameters, options: options, encoder: encoder)
        } catch let failure as SQLExecutionFailure { throw failure }
        catch {
            throw SQLExecutionFailure(underlyingError: error, diagnostic: SQLDiagnostics.diagnostic(for: error))
        }
    }
}
