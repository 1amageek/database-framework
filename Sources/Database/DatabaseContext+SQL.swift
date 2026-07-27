// DatabaseContext+SQL.swift
// Database - DatabaseContext extension for executing SQL strings with SPARQL() function support

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseKit
import QueryAST
import DatabaseEngine
import DatabaseRuntime
import DatabaseWire

// MARK: - DatabaseContext + SQL String Execution

extension DatabaseContext {
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
    /// 4. Convert rewritten SelectQuery to Query<T>
    /// 5. Execute via standard fetch() path
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
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> [T] {
        let workMeter = DatabaseWorkMeter(budget: budget)
        // 1. Parse SQL string
        let parser = SQLParser()
        let statement = try parser.parse(sql)

        // 2. Extract SelectQuery
        guard case .select(let selectQuery) = statement else {
            throw SQLExecutionError.unsupportedStatement("Only SELECT queries are supported")
        }

        // 3. Rewrite SPARQL() functions if present
        let rewrittenQuery = try await rewriteSPARQLFunctions(
            selectQuery,
            workMeter: workMeter
        )

        // 4. Execute via DatabaseEngine layer
        let response = try await query(
            rewrittenQuery,
            execution: ReadExecutionContext(
                options: ReadExecutionOptions(budget: budget),
                workMeter: workMeter
            )
        )
        guard let rowCount = UInt32(exactly: response.rows.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(rowCount)
        return try response.rows.map { row in
            try QueryRowCodec.decode(row, as: type)
        }
    }

    // MARK: - SPARQL Function Rewriting

    /// Rewrite SelectQuery by executing SPARQL() functions
    ///
    /// - Parameter selectQuery: Query to rewrite
    /// - Returns: Rewritten query with SPARQL() replaced by literal values
    /// - Throws: `SPARQLFunctionError` for SPARQL execution errors
    private func rewriteSPARQLFunctions(
        _ selectQuery: SelectQuery,
        workMeter: DatabaseWorkMeter
    ) async throws -> SelectQuery {
        let rewriter = SPARQLFunctionRewriter(
            context: self,
            workMeter: workMeter
        )
        return try await rewriter.rewrite(selectQuery)
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
