// SPARQLFunctionRewriter.swift
// Database - Rewrite SelectQuery by executing SPARQL() functions

import Foundation
import Core
import QueryIR
import QueryAST
import GraphIndex
import Graph
import DatabaseEngine
import FoundationDB

/// Rewrites SelectQuery by executing SPARQL() subqueries
///
/// **Design**: Pre-execution rewrite at FDBContext level.
/// - Finds SPARQL() function calls in Expression tree
/// - Executes SPARQL queries within parent transaction
/// - Inlines results as literal arrays
/// - Returns rewritten SelectQuery for standard execution
///
/// **Usage**:
/// ```swift
/// let rewriter = SPARQLFunctionRewriter(context: context)
/// let rewritten = try await rewriter.rewrite(selectQuery)
/// // Execute rewritten query through normal path
/// ```
internal struct SPARQLFunctionRewriter: Sendable {
    private let context: FDBContext

    /// Initialize with FDBContext
    ///
    /// - Parameter context: The context for transaction and schema access
    internal init(context: FDBContext) {
        self.context = context
    }

    // MARK: - Rewrite Entry Point

    /// Rewrite SelectQuery by executing SPARQL subqueries
    ///
    /// Recursively traverses the Expression tree in the filter clause,
    /// executing SPARQL() functions and inlining their results.
    ///
    /// - Parameter query: The SelectQuery to rewrite
    /// - Returns: Rewritten SelectQuery with SPARQL() calls replaced
    /// - Throws: `SPARQLFunctionError` for execution errors
    internal func rewrite(_ query: QueryIR.SelectQuery) async throws -> QueryIR.SelectQuery {
        guard let filter = query.filter else { return query }
        let rewrittenFilter = try await rewriteExpression(filter)

        return QueryIR.SelectQuery(
            projection: query.projection,
            source: query.source,
            filter: rewrittenFilter,
            groupBy: query.groupBy,
            having: query.having,
            orderBy: query.orderBy,
            limit: query.limit,
            offset: query.offset,
            distinct: query.distinct,
            subqueries: query.subqueries,
            reduced: query.reduced
        )
    }

    // MARK: - Expression Rewriting

    /// Recursively rewrite expressions, executing SPARQL() calls
    ///
    /// Traverses the expression tree and replaces `.function("SPARQL", ...)` nodes
    /// with `.inList(lhs, [literal values...])`.
    ///
    /// - Parameter expr: Expression to rewrite
    /// - Returns: Rewritten expression
    /// - Throws: `SPARQLFunctionError` for execution errors
    private func rewriteExpression(_ expr: QueryIR.Expression) async throws -> QueryIR.Expression {
        switch expr {
        case .inList(let lhs, let values):
            // Check if any value is a SPARQL() function
            var rewrittenValues: [QueryIR.Expression] = []
            for value in values {
                if case .function(let call) = value, call.name.uppercased() == "SPARQL" {
                    // Execute SPARQL and inline results
                    let literals = try await executeSPARQLFunctionAsArray(call)
                    rewrittenValues.append(contentsOf: literals.map { .literal($0) })
                } else {
                    rewrittenValues.append(try await rewriteExpression(value))
                }
            }
            return .inList(try await rewriteExpression(lhs), values: rewrittenValues)

        case .inSubquery(let lhs, let subquery):
            // Check if subquery contains SPARQL() - recursively rewrite
            let rewrittenSubquery = try await rewrite(subquery)
            return .inSubquery(try await rewriteExpression(lhs), subquery: rewrittenSubquery)

        // Logical operators - recurse
        case .and(let left, let right):
            return .and(try await rewriteExpression(left), try await rewriteExpression(right))

        case .or(let left, let right):
            return .or(try await rewriteExpression(left), try await rewriteExpression(right))

        case .not(let inner):
            return .not(try await rewriteExpression(inner))

        // Comparison operators - recurse on both sides
        case .equal(let left, let right):
            return .equal(try await rewriteExpression(left), try await rewriteExpression(right))

        case .notEqual(let left, let right):
            return .notEqual(try await rewriteExpression(left), try await rewriteExpression(right))

        case .lessThan(let left, let right):
            return .lessThan(try await rewriteExpression(left), try await rewriteExpression(right))

        case .lessThanOrEqual(let left, let right):
            return .lessThanOrEqual(try await rewriteExpression(left), try await rewriteExpression(right))

        case .greaterThan(let left, let right):
            return .greaterThan(try await rewriteExpression(left), try await rewriteExpression(right))

        case .greaterThanOrEqual(let left, let right):
            return .greaterThanOrEqual(try await rewriteExpression(left), try await rewriteExpression(right))

        // Arithmetic operators - recurse
        case .add(let left, let right):
            return .add(try await rewriteExpression(left), try await rewriteExpression(right))

        case .subtract(let left, let right):
            return .subtract(try await rewriteExpression(left), try await rewriteExpression(right))

        case .multiply(let left, let right):
            return .multiply(try await rewriteExpression(left), try await rewriteExpression(right))

        case .divide(let left, let right):
            return .divide(try await rewriteExpression(left), try await rewriteExpression(right))

        case .modulo(let left, let right):
            return .modulo(try await rewriteExpression(left), try await rewriteExpression(right))

        case .negate(let inner):
            return .negate(try await rewriteExpression(inner))

        // Other cases that might contain expressions
        case .between(let expr, let low, let high):
            return .between(
                try await rewriteExpression(expr),
                low: try await rewriteExpression(low),
                high: try await rewriteExpression(high)
            )

        case .isNull(let inner):
            return .isNull(try await rewriteExpression(inner))

        case .isNotNull(let inner):
            return .isNotNull(try await rewriteExpression(inner))

        case .like(let inner, let pattern):
            return .like(try await rewriteExpression(inner), pattern: pattern)

        case .regex(let inner, let pattern, let flags):
            return .regex(try await rewriteExpression(inner), pattern: pattern, flags: flags)

        case .cast(let inner, let targetType):
            return .cast(try await rewriteExpression(inner), targetType: targetType)

        case .caseWhen(let cases, let elseResult):
            var rewrittenCases: [(condition: QueryIR.Expression, result: QueryIR.Expression)] = []
            for (condition, result) in cases {
                rewrittenCases.append((
                    condition: try await rewriteExpression(condition),
                    result: try await rewriteExpression(result)
                ))
            }
            let rewrittenElse = try await elseResult.asyncMap { try await rewriteExpression($0) }
            return .caseWhen(cases: rewrittenCases, elseResult: rewrittenElse)

        case .coalesce(let exprs):
            var rewrittenExprs: [QueryIR.Expression] = []
            for expr in exprs {
                rewrittenExprs.append(try await rewriteExpression(expr))
            }
            return .coalesce(rewrittenExprs)

        case .nullIf(let left, let right):
            return .nullIf(try await rewriteExpression(left), try await rewriteExpression(right))

        // Function call - check if it's SPARQL()
        case .function(let call):
            if call.name.uppercased() == "SPARQL" {
                throw SPARQLFunctionError.invalidArguments(
                    "SPARQL() must be used in IN predicate: WHERE column IN (SPARQL(...))"
                )
            }
            // Other functions - recurse on arguments
            var rewrittenArgs: [QueryIR.Expression] = []
            for arg in call.arguments {
                rewrittenArgs.append(try await rewriteExpression(arg))
            }
            return .function(FunctionCall(name: call.name, arguments: rewrittenArgs, distinct: call.distinct))

        // Terminal cases - no recursion needed
        case .literal, .column, .variable, .bound, .aggregate:
            return expr

        // RDF/SPARQL-specific cases (no recursion needed for now)
        case .triple, .isTriple, .subject, .predicate, .object:
            return expr

        // Subquery expression cases
        case .exists(let subquery):
            return .exists(try await rewrite(subquery))

        case .subquery(let subquery):
            return .subquery(try await rewrite(subquery))
        }
    }

    // MARK: - SPARQL Execution

    /// Execute SPARQL function and return scalar values
    ///
    /// - Parameter call: The SPARQL() function call
    /// - Returns: Array of literals (single-variable projection)
    /// - Throws: `SPARQLFunctionError` for invalid arguments or execution errors
    private func executeSPARQLFunctionAsArray(_ call: FunctionCall) async throws -> [QueryIR.Literal] {
        // 1. Extract arguments (type name, query string, optional variable)
        let (typeName, sparqlQuery, extractVar) = try extractArguments(call)

        // 2. Resolve type via TypeResolver
        let resolver = TypeResolver(schema: context.container.schema)
        let entity = try resolver.resolve(typeName: typeName)
        let graphIndex = try resolver.findGraphIndex(for: entity)

        // 3. Extract graph index metadata via AnyGraphIndexKind
        guard let graphKind = graphIndex.kind as? AnyGraphIndexKind else {
            throw SPARQLFunctionError.invalidGraphIndex(entity.name)
        }

        // 4. Resolve type directory and index subspace
        let typeDirectory = try await resolveTypeDirectory(entity.persistableType)
        let indexSubspace = typeDirectory
            .subspace(SubspaceKey.indexes)
            .subspace(graphIndex.name)

        // 5. Execute SPARQL within the same transaction scope
        let result = try await executeSPARQLWithinTransaction(
            sparqlQuery: sparqlQuery,
            indexSubspace: indexSubspace,
            graphKind: graphKind,
            storedFieldNames: graphIndex.storedFieldNames
        )

        // 5. Extract single-variable values
        let varToExtract = extractVar ?? result.projectedVariables.first
        guard let variable = varToExtract else {
            throw SPARQLFunctionError.multipleVariablesNotSupported
        }

        // Validate single-variable projection
        if result.projectedVariables.count > 1 && extractVar == nil {
            throw SPARQLFunctionError.multipleVariablesNotSupported
        }

        // Convert bindings to literals
        return try result.bindings.map { binding in
            guard let fieldValue = binding[variable] else {
                throw SPARQLFunctionError.missingVariable(variable)
            }
            return try fieldValueToLiteral(fieldValue)
        }
    }

    // MARK: - Argument Extraction

    /// Extract arguments from SPARQL() function call
    ///
    /// Expected formats:
    /// - SPARQL(TypeName, 'query string')
    /// - SPARQL(TypeName, 'query string', '?variable')
    ///
    /// - Parameter call: The function call
    /// - Returns: Tuple of (typeName, sparqlQuery, optionalVariable)
    /// - Throws: `SPARQLFunctionError.invalidArguments` if format is incorrect
    private func extractArguments(_ call: FunctionCall) throws -> (typeName: String, sparqlQuery: String, variable: String?) {
        guard call.arguments.count >= 2, call.arguments.count <= 3 else {
            throw SPARQLFunctionError.invalidArguments(
                "SPARQL() requires 2-3 arguments: SPARQL(TypeName, 'query', ['?variable'])"
            )
        }

        // First argument: type name (column reference or literal)
        let typeName: String
        switch call.arguments[0] {
        case .column(let col):
            typeName = col.column
        case .literal(.string(let str)):
            typeName = str
        default:
            throw SPARQLFunctionError.invalidArguments(
                "First argument must be a type name (column or string literal)"
            )
        }

        // Second argument: SPARQL query string
        guard case .literal(.string(let sparqlQuery)) = call.arguments[1] else {
            throw SPARQLFunctionError.invalidArguments(
                "Second argument must be a SPARQL query string literal"
            )
        }

        // Third argument (optional): variable name
        let extractVar: String?
        if call.arguments.count == 3 {
            guard case .literal(.string(let varName)) = call.arguments[2] else {
                throw SPARQLFunctionError.invalidArguments(
                    "Third argument must be a variable name string literal (e.g., '?s')"
                )
            }
            extractVar = varName.hasPrefix("?") ? varName : "?\(varName)"
        } else {
            extractVar = nil
        }

        return (typeName, sparqlQuery, extractVar)
    }

    // MARK: - Directory Resolution

    /// Resolve type directory using FDBContainer
    ///
    /// - Parameter persistableType: The Persistable type to resolve
    /// - Returns: Subspace for the type
    /// - Throws: Directory resolution errors (including dynamic directory detection)
    private func resolveTypeDirectory(_ persistableType: any Persistable.Type) async throws -> Subspace {
        // Check for dynamic directory components (not supported in SPARQL function)
        let hasDynamicComponent = persistableType.directoryPathComponents.contains { component in
            component is DynamicDirectoryElement
        }

        if hasDynamicComponent {
            throw SPARQLFunctionError.invalidArguments(
                "Dynamic directory partitions not supported in SPARQL() function. " +
                "Type '\(persistableType.persistableType)' has dynamic directory components."
            )
        }

        // Use FDBContainer's directory resolution (handles caching)
        return try await context.container.resolveDirectory(for: persistableType)
    }

    // MARK: - SPARQL Execution

    /// Execute SPARQL within the current transaction scope
    ///
    /// - Parameters:
    ///   - sparqlQuery: SPARQL query string
    ///   - indexSubspace: Resolved index subspace
    ///   - graphKind: Graph index metadata
    ///   - storedFieldNames: Stored field names for the index
    /// - Returns: SPARQL result
    /// - Throws: SPARQL execution errors
    private func executeSPARQLWithinTransaction(
        sparqlQuery: String,
        indexSubspace: Subspace,
        graphKind: any AnyGraphIndexKind,
        storedFieldNames: [String]
    ) async throws -> SPARQLResult {
        // Execute using database transaction (shares snapshot with parent SQL transaction)
        return try await context.container.database.withTransaction { transaction in
            try await executeSPARQLString(
                sparqlQuery,
                database: context.container.database,
                indexSubspace: indexSubspace,
                strategy: graphKind.strategy,
                fromFieldName: graphKind.fromFieldName,
                edgeFieldName: graphKind.edgeFieldName,
                toFieldName: graphKind.toFieldName,
                graphFieldName: graphKind.graphFieldName,
                storedFieldNames: storedFieldNames
            )
        }
    }

    // MARK: - FieldValue Conversion

    /// Convert FieldValue to QueryIR.Literal
    ///
    /// - Parameter fieldValue: The FieldValue to convert
    /// - Returns: Equivalent Literal
    /// - Throws: `SPARQLFunctionError` if conversion fails (not expected for graph data)
    private func fieldValueToLiteral(_ fieldValue: FieldValue) throws -> QueryIR.Literal {
        switch fieldValue {
        case .null:
            return .null
        case .bool(let value):
            return .bool(value)
        case .int64(let value):
            return .int(value)
        case .double(let value):
            return .double(value)
        case .string(let value):
            return .string(value)
        case .data(let value):
            return .binary(value)
        case .array:
            // Array values are not supported in SPARQL function results
            throw SPARQLFunctionError.invalidArguments("Array values not supported in SPARQL() results")
        }
    }
}

// MARK: - Optional async map helper

extension Optional {
    fileprivate func asyncMap<T>(_ transform: (Wrapped) async throws -> T) async rethrows -> T? {
        switch self {
        case .some(let value):
            return try await transform(value)
        case .none:
            return nil
        }
    }
}
