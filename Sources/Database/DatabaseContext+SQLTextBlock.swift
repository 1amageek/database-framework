import DatabaseKit
import QueryAST
@_spi(DatabaseExecution) import DatabaseEngine

extension DatabaseContext {
    /// Executes one bounded SQL text request and retains typed diagnostics.
    public func executeSQLBlock(
        _ sql: String,
        parameters: [QueryParameter] = [],
        options: SQLBlockExecutionOptions
    ) async throws -> SQLBlockResult {
        try await executeSQLText(sql, context: self, options: options) { parsed, meter, deadline, position in
            try await SQLBlockExecutor(context: self).execute(parsed.block, parameters: parameters,
                options: options, workMeter: meter, executionDeadline: deadline,
                occurrences: parsed.occurrences, occurrenceRoots: parsed.roots, failurePosition: position)
        }
    }

    /// Encodes one bounded text request while preserving the original source diagnostic.
    @_spi(DatabaseExecution)
    public func executeEncodedSQLBlock(_ sql: String,
        parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        encoder: any SQLBlockResponseEncoder) async throws -> SQLEncodedResponse {
        try await executeSQLText(sql, context: self, options: options) { parsed, meter, deadline, position in
            try await SQLBlockExecutor(context: self).executeEncoded(parsed.block, parameters: parameters,
                options: options, workMeter: meter, executionDeadline: deadline,
                occurrences: parsed.occurrences, occurrenceRoots: parsed.roots, failurePosition: position,
                encoder: encoder)
        }
    }
}

private func executeSQLText<Result: Sendable>(_ sql: String, context: DatabaseContext,
    options: SQLBlockExecutionOptions,
    execute: @Sendable (SQLParsedBlock, DatabaseWorkMeter, TransactionExecutionDeadline, SQLBlockFailurePosition) async throws -> Result
) async throws -> Result {
    let clock = context.container.monotonicClock
    let meter = DatabaseWorkMeter(budget: options.budget, monotonicClock: clock)
    let deadline = TransactionExecutionDeadline(
        instant: clock.now.advanced(by: .milliseconds(Int64(options.budget.timeoutMilliseconds))),
        timeoutMilliseconds: options.budget.timeoutMilliseconds)
    let parsed: SQLParsedBlock
    do {
        parsed = try SQLParser(structuralLimits: options.structuralLimits,
            preservesCastOutputNames: true).parseBlockWithSource(sql)
        try meter.checkpoint(at: .bindingCandidate)
    } catch {
        throw try sqlTextFailure(error, source: sql, bytePosition: sqlParserBytePosition(error))
    }
    let failurePosition = SQLBlockFailurePosition()
    do {
        return try await execute(parsed, meter, deadline, failurePosition)
    } catch let failure as SQLExecutionFailure {
        throw failure
    } catch let failure as CanonicalSQLBindingFailure {
        let source = SQLBindingSourceFailure(failure, sourceMap: parsed.sourceMap)
        throw try sqlTextFailure(failure, source: sql, bytePosition: source.position)
    } catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
        let original = CanonicalReadError.expressionEvaluation(cause, occurrence: occurrence)
        let source = SQLBindingSourceFailure(cause: .expression(cause), occurrence: occurrence,
            sourceMap: parsed.sourceMap)
        throw try sqlTextFailure(original, source: sql, bytePosition: source.position)
    } catch {
        let offset = failurePosition.stepIndex.flatMap { index in
            parsed.stepRanges.indices.contains(index) ? parsed.stepRanges[index].start : nil
        }
        throw try sqlTextFailure(error, source: sql, bytePosition: offset)
    }
}

private func sqlParserBytePosition(_ error: any Error) -> Int? {
    guard let error = error as? SQLParser.ParseError else { return nil }
    switch error {
    case .unexpectedToken(_, _, let position), .unexpectedEndOfInput(_, let position),
         .invalidSyntax(_, let position), .literalError(_, let position, _): return position
    case .unsupportedFeature: return nil
    }
}

private func sqlConstantDiagnostic(_ failure: SQLConstantFailure, message: String) throws -> SQLDiagnostic {
    switch failure {
    case .invalidRepresentation: try SQLDiagnostic(sqlState: "22P02", category: .data, message: message)
    case .outOfRange: try SQLDiagnostic(sqlState: "22003", category: .numeric, message: message)
    case .invalidTypeModifier, .invalidOptionValue: try SQLDiagnostic(sqlState: "22023", category: .data, message: message)
    case .unsupported: try SQLDiagnostic(sqlState: "0A000", category: .unsupported, message: message)
    case .resourceLimit: try SQLDiagnostic(sqlState: "54000", category: .resource, message: message)
    }
}

private func sqlTextFailure(
    _ error: any Error, source: String, bytePosition: Int?
) throws -> SQLExecutionFailure {
    let base: SQLDiagnostic
    if let parse = error as? SQLParser.ParseError {
        switch parse {
        case .unsupportedFeature:
            base = try SQLDiagnostic(sqlState: "0A000", category: .unsupported,
                message: "Unsupported SQL syntax")
        case .literalError(let message, _, let failure):
            base = try sqlConstantDiagnostic(failure, message: message)
        default:
            base = try SQLDiagnostic(sqlState: "42601", category: .syntax,
                message: "Invalid SQL syntax")
        }
    } else {
        base = SQLDiagnostics.diagnostic(for: error)
    }
    let position: UInt64?
    if let offset = bytePosition, offset >= 0, offset <= source.utf8.count {
        let byteIndex = source.utf8.index(source.utf8.startIndex, offsetBy: offset)
        if let index = String.Index(byteIndex, within: source) {
            position = UInt64(source[..<index].unicodeScalars.count) + 1
        } else { position = nil }
    } else { position = nil }
    let diagnostic = try SQLDiagnostic(sqlState: base.sqlState, category: base.category,
        message: base.message, position: base.position ?? position, schema: base.schema,
        table: base.table, column: base.column, constraint: base.constraint)
    return SQLExecutionFailure(underlyingError: error, diagnostic: diagnostic)
}
