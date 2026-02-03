// FDBContext+SQL.swift
// Database - FDBContext extension for executing SQL strings with SPARQL() function support

import Foundation
import Core
import QueryIR
import QueryAST
import DatabaseEngine

// MARK: - FDBContext + SQL String Execution

extension FDBContext {
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
    ///           `QueryBridgeError` for conversion errors, or any underlying fetch errors
    public func executeSQL<T: Persistable>(
        _ sql: String,
        as type: T.Type
    ) async throws -> [T] {
        // 1. Parse SQL string
        let parser = SQLParser()
        let statement = try parser.parse(sql)

        // 2. Extract SelectQuery
        guard case .select(let selectQuery) = statement else {
            throw SQLExecutionError.unsupportedStatement("Only SELECT queries are supported")
        }

        // 3. Rewrite SPARQL() functions if present
        let rewrittenQuery = try await rewriteSPARQLFunctions(selectQuery)

        // 4. Execute via DatabaseEngine layer
        return try await execute(rewrittenQuery, as: type)
    }

    // MARK: - SPARQL Function Rewriting

    /// Rewrite SelectQuery by executing SPARQL() functions
    ///
    /// - Parameter selectQuery: Query to rewrite
    /// - Returns: Rewritten query with SPARQL() replaced by literal values
    /// - Throws: `SPARQLFunctionError` for SPARQL execution errors
    private func rewriteSPARQLFunctions(_ selectQuery: QueryIR.SelectQuery) async throws -> QueryIR.SelectQuery {
        let rewriter = SPARQLFunctionRewriter(context: self)
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
