// SPARQLFunctionRewriter.swift
// Database - Rewrite SelectQuery by executing SPARQL() functions

#if DATABASE_GRAPH_INDEXES
import DatabaseKit
import QueryAST
import GraphIndex
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Rewrites SelectQuery by executing SPARQL() subqueries
///
/// **Design**: Pre-execution rewrite at DatabaseContext level.
/// - Finds SPARQL() function calls in Expression tree
/// - Executes SPARQL queries within parent transaction
/// - Inlines results as literal arrays
/// - Returns rewritten SelectQuery for standard execution
///
/// **Usage**:
/// ```swift
/// let rewriter = SPARQLFunctionRewriter(
///     context: context,
///     workMeter: workMeter,
///     transaction: transaction
/// )
/// let rewritten = try await rewriter.rewrite(selectQuery)
/// // Execute rewritten query through normal path
/// ```
internal struct SPARQLFunctionRewriter: Sendable {
    private let context: DatabaseContext
    private let workMeter: DatabaseWorkMeter
    private let transaction: any TransactionAccess

    /// Initialize with DatabaseContext
    ///
    /// - Parameters:
    ///   - context: Context for schema and index access.
    ///   - workMeter: Shared resource budget for graph and SQL execution.
    ///   - transaction: Parent SQL read transaction.
    internal init(
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter,
        transaction: any TransactionAccess
    ) {
        self.context = context
        self.workMeter = workMeter
        self.transaction = transaction
    }

    /// Returns whether the filter tree contains a SPARQL SQL function that
    /// requires transaction-bound rewriting before canonical query execution.
    internal static func containsSPARQLFunction(
        in query: SelectQuery
    ) -> Bool {
        guard let filter = query.filter else { return false }

        var pending: [DatabaseKit.Expression] = [filter]
        while let expression = pending.popLast() {
            switch expression {
            case .function(let call):
                if call.name.uppercased() == "SPARQL" {
                    return true
                }
                pending.append(contentsOf: call.arguments)

            case .add(let left, let right),
                 .subtract(let left, let right),
                 .multiply(let left, let right),
                 .divide(let left, let right),
                 .modulo(let left, let right),
                 .equal(let left, let right),
                 .notEqual(let left, let right),
                 .lessThan(let left, let right),
                 .lessThanOrEqual(let left, let right),
                 .greaterThan(let left, let right),
                 .greaterThanOrEqual(let left, let right),
                 .and(let left, let right),
                 .or(let left, let right),
                 .nullIf(let left, let right):
                pending.append(left)
                pending.append(right)

            case .negate(let inner),
                 .not(let inner),
                 .isNull(let inner),
                 .isNotNull(let inner),
                 .like(let inner, _),
                 .regex(let inner, _, _),
                 .cast(let inner, _),
                 .isTriple(let inner),
                 .subject(let inner),
                 .predicate(let inner),
                 .object(let inner):
                pending.append(inner)

            case .between(let value, let low, let high):
                pending.append(value)
                pending.append(low)
                pending.append(high)

            case .inList(let value, let values),
                 .notInList(let value, let values):
                pending.append(value)
                pending.append(contentsOf: values)

            case .inSubquery(let value, let subquery):
                pending.append(value)
                if let filter = subquery.filter {
                    pending.append(filter)
                }

            case .caseWhen(let cases, let elseResult):
                for pair in cases {
                    pending.append(pair.condition)
                    pending.append(pair.result)
                }
                if let elseResult {
                    pending.append(elseResult)
                }

            case .coalesce(let expressions):
                pending.append(contentsOf: expressions)

            case .triple(let subject, let predicate, let object):
                pending.append(subject)
                pending.append(predicate)
                pending.append(object)

            case .subquery(let subquery), .exists(let subquery):
                if let filter = subquery.filter {
                    pending.append(filter)
                }

            case .aggregate(let aggregate):
                switch aggregate {
                case .count(let value, _):
                    if let value { pending.append(value) }
                case .sum(let value, _),
                     .avg(let value, _),
                     .min(let value),
                     .max(let value),
                     .sample(let value):
                    pending.append(value)
                case .groupConcat(let value, _, _):
                    pending.append(value)
                case .arrayAgg(let value, let orderBy, _):
                    pending.append(value)
                    if let orderBy {
                        for ordering in orderBy {
                            pending.append(ordering.expression)
                        }
                    }
                }

            case .literal, .column, .variable, .parameter, .bound:
                break
            }
        }
        return false
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
    internal func rewrite(_ query: SelectQuery) async throws -> SelectQuery {
        guard let filter = query.filter else { return query }
        let rewrittenFilter = try await rewriteExpression(filter)

        return SelectQuery(
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
            reduced: query.reduced,
            dataset: query.dataset
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
    private func rewriteExpression(
        _ expr: DatabaseKit.Expression
    ) async throws -> DatabaseKit.Expression {
        switch expr {
        case .inList(let lhs, let values):
            // Check if any value is a SPARQL() function
            var rewrittenValues: [DatabaseKit.Expression] = []
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

        case .notInList(let lhs, let values):
            var rewrittenValues: [DatabaseKit.Expression] = []
            for value in values {
                if case .function(let call) = value, call.name.uppercased() == "SPARQL" {
                    let literals = try await executeSPARQLFunctionAsArray(call)
                    rewrittenValues.append(contentsOf: literals.map { .literal($0) })
                } else {
                    rewrittenValues.append(try await rewriteExpression(value))
                }
            }
            return .notInList(try await rewriteExpression(lhs), values: rewrittenValues)

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
            var rewrittenCases: [CaseWhenPair] = []
            for pair in cases {
                rewrittenCases.append(CaseWhenPair(
                    condition: try await rewriteExpression(pair.condition),
                    result: try await rewriteExpression(pair.result)
                ))
            }
            let rewrittenElse = try await elseResult.asyncMap { try await rewriteExpression($0) }
            return .caseWhen(cases: rewrittenCases, elseResult: rewrittenElse)

        case .coalesce(let exprs):
            var rewrittenExprs: [DatabaseKit.Expression] = []
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
            var rewrittenArgs: [DatabaseKit.Expression] = []
            for arg in call.arguments {
                rewrittenArgs.append(try await rewriteExpression(arg))
            }
            return .function(FunctionCall(name: call.name, arguments: rewrittenArgs, distinct: call.distinct))

        // Terminal cases - no recursion needed
        case .literal, .column, .variable, .parameter, .bound, .aggregate:
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
    private func executeSPARQLFunctionAsArray(_ call: FunctionCall) async throws -> [Literal] {
        // 1. Extract arguments (type name, query string, optional variable)
        let (typeName, sparqlQuery, extractVar) = try extractArguments(call)

        // 2. Resolve type via TypeResolver
        let resolver = TypeResolver(schema: context.container.schema)
        let entity = try resolver.resolve(typeName: typeName)
        guard !entity.hasDynamicDirectory else {
            throw SPARQLFunctionError.invalidArguments(
                "SPARQL() requires an explicit partition for dynamic entity '\(entity.name)'"
            )
        }
        guard let dataset = try RDFDatasetReadResolver.resolve(entity: entity) else {
            throw SPARQLFunctionError.invalidGraphIndex(entity.name)
        }
        let graphIndex = dataset.indexDescriptor

        // 4. Admit and execute the schema-declared index in the parent read
        // transaction. The rewritten SQL query consumes the same snapshot.
        let result = try await executeSPARQL(
            sparqlQuery: sparqlQuery,
            entityName: entity.name,
            indexDescriptor: graphIndex,
            metadata: dataset.metadata,
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

    // MARK: - SPARQL Execution

    /// Execute SPARQL within the current transaction scope
    ///
    /// - Parameters:
    ///   - sparqlQuery: SPARQL query string
    ///   - indexSubspace: Resolved index subspace
    ///   - metadata: RDF dataset index metadata
    ///   - storedFieldNames: Stored field names for the index
    /// - Returns: SPARQL result
    /// - Throws: SPARQL execution errors
    private func executeSPARQL(
        sparqlQuery: String,
        entityName: String,
        indexDescriptor: IndexDescriptor,
        metadata: RDFDatasetIndexMetadata,
        storedFieldNames: [String]
    ) async throws -> SPARQLResult {
        let readableIndex = try await context.indexQueryContext
            .readableIndex(
                named: indexDescriptor.name,
                kindIdentifier: indexDescriptor.kindIdentifier,
                forEntityName: entityName,
                partitions: FieldObject(),
                transaction: transaction
            )
        let sources: [RDFDatasetSource]
        if let readableIndex {
            sources = [
                RDFDatasetSource(
                    entityName: entityName,
                    indexName: indexDescriptor.name,
                    indexSubspace: readableIndex.subspace,
                    coverage: try metadata.graphScope.sourceCoverage,
                    storedFieldNames: storedFieldNames
                )
            ]
        } else {
            sources = []
        }
        return try await _executeSPARQLString(
            sparqlQuery,
            database: context.container.engine,
            sources: sources,
            monotonicClock: context.container.monotonicClock,
            wallClock: context.container.wallClock,
            transaction: transaction,
            compilationLimits: .default,
            workMeter: workMeter
        )
    }

    // MARK: - FieldValue Conversion

    private func fieldValueToLiteral(
        _ fieldValue: FieldValue
    ) throws -> Literal {
        do {
            return try fieldValue.toLiteral()
        } catch {
            throw SPARQLFunctionError.invalidArguments(error.description)
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
#endif
