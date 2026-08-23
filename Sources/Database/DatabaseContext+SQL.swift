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
        let parsedStatement = try parser.parse(sql)
        let statement = try QueryParameterBinder(
            parameters: parameters,
            structuralLimits: structuralLimits
        ).bind(parsedStatement)
        guard case .select(let selectQuery) = statement else {
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
        let response = try await executeSQLSelect(
            selectQuery,
            execution: execution,
            workMeter: workMeter
        )
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
    ///     SELECT * FROM User
    ///     WHERE id IN (SPARQL(RDFTriple, 'SELECT ?s WHERE { ?s <knows> "Alice" }'))
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
            try QueryRowCodec.decode(row, as: type)
        }
    }

    private func executeSQLSelect(
        _ selectQuery: SelectQuery,
        execution: ReadExecutionContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> QueryResponse {
        #if DATABASE_GRAPH_INDEXES
        if SPARQLFunctionRewriter.containsSPARQLFunction(in: selectQuery) {
            return try await indexQueryContext.withQuerySnapshot {
                snapshot in
                let preparedQuery = try await self
                    .prepareSQLSelectForCanonicalExecution(
                    selectQuery,
                    workMeter: workMeter,
                    snapshot: snapshot,
                    structuralLimits: execution.queryStructuralLimits
                )
                return try await preparedQuery.execute(
                    in: self,
                    execution: execution
                )
            }
        }
        #endif
        return try await query(selectQuery, execution: execution)
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
        transaction: any TransactionReadAccess,
        structuralLimits: QueryStructuralLimits
    ) async throws -> DatabasePreparedSQLSelect {
        _ = transaction
        return try await indexQueryContext.withQuerySnapshot { snapshot in
            try await self.prepareSQLSelectForCanonicalExecution(
                selectQuery,
                workMeter: workMeter,
                snapshot: snapshot,
                structuralLimits: structuralLimits
            )
        }
    }

    package func prepareSQLSelectForCanonicalExecution(
        _ selectQuery: SelectQuery,
        workMeter: DatabaseWorkMeter,
        snapshot: any IndexQuerySnapshotAccess,
        structuralLimits: QueryStructuralLimits
    ) async throws -> DatabasePreparedSQLSelect {
        guard SPARQLFunctionRewriter.containsSPARQLFunction(
            in: selectQuery
        ) else {
            return DatabasePreparedSQLSelect(
                query: selectQuery,
                workMeter: workMeter
            )
        }
        let retainedStorage = try DatabasePreparedSQLSelectStorage(
            workMeter: workMeter
        )
        let rewriter = SPARQLFunctionRewriter(
            context: self,
            workMeter: workMeter,
            snapshot: snapshot,
            retainedStorage: retainedStorage,
            structuralLimits: structuralLimits
        )
        return try await rewriter.rewritePrepared(selectQuery)
    }
    #else
    @_spi(DatabaseExecution)
    public func prepareSQLSelectForCanonicalExecution(
        _ selectQuery: SelectQuery,
        workMeter: DatabaseWorkMeter,
        transaction: any TransactionReadAccess,
        structuralLimits: QueryStructuralLimits
    ) async throws -> DatabasePreparedSQLSelect {
        _ = workMeter
        _ = transaction
        _ = structuralLimits
        return DatabasePreparedSQLSelect(
            query: selectQuery,
            workMeter: workMeter
        )
    }
    #endif
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
