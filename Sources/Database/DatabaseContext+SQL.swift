// DatabaseContext+SQL.swift
// Database - DatabaseContext extension for executing SQL strings with SPARQL() function support

import DatabaseKit
import QueryAST
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseRuntime
import StorageKit

// MARK: - DatabaseContext + SQL String Execution

extension DatabaseContext {
    /// Execute a SQL SELECT statement and return canonical rows.
    ///
    /// Parsing and parameter binding use the same structural limits. SPARQL
    /// rewriting and relational execution share one work meter and read snapshot.
    public func executeSQL(
        _ sql: String,
        parameters: [QueryParameter] = [],
        options: ReadExecutionOptions = .default,
        structuralLimits: QueryStructuralLimits = .default
    ) async throws -> QueryResponse {
        let workMeter = DatabaseWorkMeter(
            budget: options.budget,
            monotonicClock: container.monotonicClock
        )
        let parser = SQLParser(structuralLimits: structuralLimits)
        let parsed = try parser.parseWithSource(sql)
        let parsedStatement = parsed.statement
        try QueryParameterBinder(
            parameters: parameters,
            structuralLimits: structuralLimits
        ).validate(parsedStatement)
        guard case .select(let selectQuery) = parsedStatement else {
            throw SQLExecutionError.unsupportedStatement(
                "Only SELECT queries are supported"
            )
        }

        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: structuralLimits
        )
        let response: QueryResponse
        do {
            response = try await executeSQLSelect(
                selectQuery,
                parameters: parameters,
                execution: execution,
                workMeter: workMeter,
                occurrences: parsed.occurrences,
                occurrence: parsed.root
            )
        } catch let failure as CanonicalSQLBindingFailure {
            throw SQLBindingSourceFailure(failure, sourceMap: parsed.sourceMap)
        } catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
            throw SQLBindingSourceFailure(cause: .expression(cause), occurrence: occurrence, sourceMap: parsed.sourceMap)
        }
        guard let rowCount = UInt32(exactly: response.rows.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: options.budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(rowCount)
        return response
    }

    /// Execute a SQL query string and return typed results
    ///
    /// **SPARQL() Function Support**:
    /// This method automatically detects and executes SPARQL() functions in SQL queries.
    ///
    /// **Example**:
    /// ```swift
    /// let sql = """
    ///     SELECT * FROM "User"
    ///     WHERE id IN (SPARQL("RDFTriple", 'SELECT ?s WHERE { ?s <knows> "Alice" }'))
    /// """
    /// let users = try await context.executeSQL(sql, as: User.self)
    /// ```
    ///
    /// **Execution Flow**:
    /// 1. Parse SQL string → SelectQuery
    /// 2. Detect SPARQL() functions
    /// 3. Execute SPARQL subqueries and inline results
    /// 4. Execute the canonical SelectQuery through DatabaseEngine
    /// 5. Decode canonical rows into the requested model type
    ///
    /// - Parameters:
    ///   - sql: SQL query string
    ///   - type: The Persistable type to fetch
    /// - Returns: Array of matching models
    /// - Throws: `SQLParseError` for invalid SQL, `SPARQLFunctionError` for SPARQL errors,
    ///           `CanonicalReadError` for conversion errors, or any underlying fetch errors
    public func executeSQL<T: Persistable>(
        _ sql: String,
        as type: T.Type,
        parameters: [QueryParameter] = [],
        budget: ExecutionBudget = ExecutionBudget(),
        structuralLimits: QueryStructuralLimits = .default
    ) async throws -> [T] {
        let response = try await executeSQL(
            sql,
            parameters: parameters,
            options: ReadExecutionOptions(budget: budget),
            structuralLimits: structuralLimits
        )
        return try response.rows.map { row in
            try QueryRowCodec.decode(row, columns: response.columns, as: type)
        }
    }

    private func executeSQLSelect(
        _ selectQuery: SelectQuery,
        parameters: [QueryParameter],
        execution: ReadExecutionContext,
        workMeter: DatabaseWorkMeter,
        occurrences: QueryOccurrenceArena,
        occurrence: QueryOccurrenceID
    ) async throws -> QueryResponse {
        #if DATABASE_GRAPH_INDEXES
        if SPARQLFunctionRewriter.containsSPARQLFunction(in: selectQuery) {
            return try await indexQueryContext.withSession(
                workMeter: workMeter
            ) {
                session in
                let preparedQuery = try await self
                    .prepareSQLSelectForCanonicalExecution(
                    selectQuery,
                    workMeter: workMeter,
                    session: session,
                    structuralLimits: execution.queryStructuralLimits,
                    parameters: parameters,
                    occurrences: occurrences,
                    occurrence: occurrence
                )
                return try await preparedQuery.execute(
                    in: session,
                    execution: execution
                )
            }
        }
        #endif
        return try await query(
            selectQuery,
            execution: execution,
            parameters: parameters,
            occurrences: occurrences,
            occurrence: occurrence
        )
    }

    #if DATABASE_GRAPH_INDEXES
    // MARK: - SPARQL Function Rewriting

    /// Rewrite SelectQuery by executing SPARQL() functions
    ///
    /// - Parameter selectQuery: Query to rewrite
    /// - Returns: Prepared query retaining rewritten literals and reservations
    /// - Throws: `SPARQLFunctionError` for SPARQL execution errors
    @_spi(DatabaseExecution)
    public func prepareSQLSelectForCanonicalExecution(
        _ selectQuery: SelectQuery,
        workMeter: DatabaseWorkMeter,
        session: DatabaseReadSession,
        structuralLimits: QueryStructuralLimits,
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> DatabasePreparedSQLSelect {
        try QueryParameterBinder(parameters: parameters, structuralLimits: structuralLimits)
            .validate(.select(selectQuery))
        let metadataReservation = try reserveSQLParameterMetadata(
            parameters: parameters,
            workMeter: workMeter
        )
        let resolvedParameters = try boundQueryParameters(
            selectQuery,
            parameters: parameters,
            workMeter: workMeter,
            allowingSPARQLFunctions: true,
            occurrences: occurrences,
            occurrence: occurrence
        )
        return try await session.withCanonicalExecution(
            workMeter: workMeter
        ) { transaction in
            guard SPARQLFunctionRewriter.containsSPARQLFunction(
                in: selectQuery
            ) else {
                return DatabasePreparedSQLSelect(
                    query: selectQuery,
                    workMeter: workMeter,
                    parameters: resolvedParameters,
                    metadataReservation: metadataReservation,
                    occurrences: occurrences,
                    occurrence: occurrence
                )
            }
            let retainedStorage = try DatabasePreparedSQLSelectStorage(
                workMeter: workMeter
            )
            let rewriter = SPARQLFunctionRewriter(
                context: self,
                workMeter: workMeter,
                transaction: transaction,
                retainedStorage: retainedStorage,
                structuralLimits: structuralLimits,
                parameters: resolvedParameters
            )
            return try await rewriter.rewritePrepared(
                selectQuery,
                parameters: resolvedParameters,
                metadataReservation: metadataReservation,
                occurrences: occurrences,
                occurrence: occurrence
            )
        }
    }
    #else
    @_spi(DatabaseExecution)
    public func prepareSQLSelectForCanonicalExecution(
        _ selectQuery: SelectQuery,
        workMeter: DatabaseWorkMeter,
        session: DatabaseReadSession,
        structuralLimits: QueryStructuralLimits,
        parameters: [QueryParameter] = [],
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil
    ) async throws -> DatabasePreparedSQLSelect {
        try QueryParameterBinder(parameters: parameters, structuralLimits: structuralLimits)
            .validate(.select(selectQuery))
        let metadataReservation = try reserveSQLParameterMetadata(
            parameters: parameters,
            workMeter: workMeter
        )
        let resolvedParameters = try boundQueryParameters(
            selectQuery,
            parameters: parameters,
            workMeter: workMeter,
            occurrences: occurrences,
            occurrence: occurrence
        )
        return try await session.withCanonicalExecution(
            workMeter: workMeter
        ) { _ in
            DatabasePreparedSQLSelect(
                query: selectQuery,
                workMeter: workMeter,
                parameters: resolvedParameters,
                metadataReservation: metadataReservation,
                occurrences: occurrences,
                occurrence: occurrence
            )
        }
    }
    #endif

    private func reserveSQLParameterMetadata(
        parameters: [QueryParameter],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateReservation {
        let footprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<QueryParameter>.stride)
        ).multiplied(by: UInt64(parameters.count)).adding(
            DatabaseIntermediateFootprint(bytes: 128)
        )
        return try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
    }
}

// MARK: - Errors

/// Errors that occur during SQL string execution
public enum SQLExecutionError: Error, Sendable, CustomStringConvertible {
    /// Unsupported SQL statement type
    case unsupportedStatement(String)

    public var description: String {
        switch self {
        case .unsupportedStatement(let message):
            return "Unsupported SQL statement: \(message)"
        }
    }
}
