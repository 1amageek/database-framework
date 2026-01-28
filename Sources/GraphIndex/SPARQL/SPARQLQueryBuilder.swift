// SPARQLQueryBuilder.swift
// GraphIndex - SPARQL-like query builder
//
// Fluent builder for constructing SPARQL-like graph queries.

import Foundation
import Core
import DatabaseEngine
import Graph
import QueryIR

/// Builder for SPARQL-like graph queries
///
/// Supports:
/// - Multiple triple patterns (implicit join)
/// - OPTIONAL (left outer join)
/// - UNION (alternative patterns)
/// - FILTER (post-pattern filtering)
/// - SELECT projection
/// - DISTINCT, LIMIT, OFFSET modifiers
///
/// **Design**: Immutable builder pattern following existing conventions.
/// Each method returns a new builder with the modification applied.
///
/// **Usage**:
/// ```swift
/// let results = try await context.sparql(Statement.self)
///     .defaultIndex()
///     .where("Alice", "knows", "?friend")
///     .where("?friend", "name", "?name")
///     .filter("?name", startsWith: "B")
///     .select("?friend", "?name")
///     .limit(10)
///     .execute()
/// ```
public struct SPARQLQueryBuilder<T: Persistable>: Sendable {

    // MARK: - Configuration

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    // MARK: - Query State

    private var graphPattern: ExecutionPattern
    private var projectedVariables: [String]?
    private var limitCount: Int?
    private var offsetCount: Int
    private var isDistinct: Bool
    private var sortKeys: [BindingSortKey]

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
        self.graphPattern = .basic([])
        self.projectedVariables = nil
        self.limitCount = nil
        self.offsetCount = 0
        self.isDistinct = false
        self.sortKeys = []
    }

    // MARK: - Pattern Building

    /// Add a triple pattern to the WHERE clause
    ///
    /// Strings starting with "?" are interpreted as variables.
    /// Multiple calls create an implicit join (AND).
    ///
    /// **Example**:
    /// ```swift
    /// .where("?person", "knows", "Bob")
    /// .where("?person", "age", "?age")
    /// ```
    public func `where`(
        _ subject: String,
        _ predicate: String,
        _ object: String
    ) -> Self {
        `where`(
            ExecutionTerm(stringLiteral: subject),
            ExecutionTerm(stringLiteral: predicate),
            ExecutionTerm(stringLiteral: object)
        )
    }

    /// Add a triple pattern using ExecutionTerm values
    public func `where`(
        _ subject: ExecutionTerm,
        _ predicate: ExecutionTerm,
        _ object: ExecutionTerm
    ) -> Self {
        var copy = self
        let pattern = ExecutionTriple(subject: subject, predicate: predicate, object: object)

        // Add to existing basic pattern or create new one
        switch copy.graphPattern {
        case .basic(var patterns):
            patterns.append(pattern)
            copy.graphPattern = .basic(patterns)
        default:
            // Wrap existing pattern in a join
            copy.graphPattern = .join(copy.graphPattern, .basic([pattern]))
        }

        return copy
    }

    // MARK: - Property Path Patterns

    /// Add a property path pattern to the WHERE clause
    ///
    /// Property paths allow complex navigation patterns including:
    /// - Inverse paths: `^knows` (reverse direction)
    /// - Sequences: `knows/worksAt` (multi-hop)
    /// - Alternatives: `knows|friendOf` (either)
    /// - Transitive: `knows+` (one or more hops)
    /// - Optional: `knows?` (zero or one)
    /// - Closure: `knows*` (zero or more hops)
    ///
    /// **Example**:
    /// ```swift
    /// // Find all ancestors (transitive closure)
    /// .wherePath("?person", path: .oneOrMore(.iri("parentOf")), "?ancestor")
    ///
    /// // Find friends or colleagues
    /// .wherePath("?person", path: .alternative(.iri("knows"), .iri("worksAt")), "?related")
    ///
    /// // Find friends of friends
    /// .wherePath("Alice", path: .sequence(.iri("knows"), .iri("knows")), "?fof")
    /// ```
    public func wherePath(
        _ subject: String,
        path: ExecutionPropertyPath,
        _ object: String
    ) -> Self {
        wherePath(
            ExecutionTerm(stringLiteral: subject),
            path: path,
            ExecutionTerm(stringLiteral: object)
        )
    }

    /// Add a property path pattern using ExecutionTerm values
    public func wherePath(
        _ subject: ExecutionTerm,
        path: ExecutionPropertyPath,
        _ object: ExecutionTerm
    ) -> Self {
        var copy = self

        // Convert property path to graph pattern
        let pathPattern = ExecutionPattern.propertyPath(subject: subject, path: path, object: object)

        switch copy.graphPattern {
        case .basic(let patterns) where patterns.isEmpty:
            copy.graphPattern = pathPattern
        default:
            copy.graphPattern = .join(copy.graphPattern, pathPattern)
        }

        return copy
    }

    // MARK: - OPTIONAL

    /// Add an OPTIONAL pattern
    ///
    /// The optional pattern is matched if possible, but the solution
    /// is still included even if the optional part doesn't match.
    /// Unmatched variables from OPTIONAL will be unbound (nil).
    ///
    /// **Example**:
    /// ```swift
    /// .where("?person", "name", "?name")
    /// .optional { $0.where("?person", "email", "?email") }
    /// // ?email may be nil in results
    /// ```
    public func optional(
        _ configure: (SPARQLQueryBuilder<T>) -> SPARQLQueryBuilder<T>
    ) -> Self {
        var copy = self

        // Create a fresh builder for the optional part
        var optionalBuilder = SPARQLQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName
        )
        optionalBuilder = configure(optionalBuilder)

        // Combine with OPTIONAL semantics
        copy.graphPattern = .optional(copy.graphPattern, optionalBuilder.graphPattern)
        return copy
    }

    // MARK: - UNION

    /// Add a UNION alternative pattern
    ///
    /// Either the current pattern OR the union pattern can match.
    ///
    /// **Example**:
    /// ```swift
    /// .where("?person", "knows", "Alice")
    /// .union { $0.where("?person", "follows", "Alice") }
    /// // Matches people who know OR follow Alice
    /// ```
    public func union(
        _ configure: (SPARQLQueryBuilder<T>) -> SPARQLQueryBuilder<T>
    ) -> Self {
        var copy = self

        var unionBuilder = SPARQLQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName
        )
        unionBuilder = configure(unionBuilder)

        copy.graphPattern = .union(copy.graphPattern, unionBuilder.graphPattern)
        return copy
    }

    // MARK: - FILTER

    /// Add a FILTER constraint
    ///
    /// **Example**:
    /// ```swift
    /// .where("?person", "age", "?age")
    /// .filter(.custom { binding in
    ///     guard let age = binding.int("?age") else { return false }
    ///     return age >= 18
    /// })
    /// ```
    public func filter(_ expression: FilterExpression) -> Self {
        var copy = self
        copy.graphPattern = .filter(copy.graphPattern, expression)
        return copy
    }

    /// Filter: variable equals value
    ///
    /// The value is treated as a string. For typed comparisons, use
    /// `.filter(.equals("?var", .int64(42)))` directly.
    public func filter(_ variable: String, equals value: String) -> Self {
        filter(.equals(variable, .string(value)))
    }

    /// Filter: variable not equals value
    ///
    /// The value is treated as a string. For typed comparisons, use
    /// `.filter(.notEquals("?var", .int64(42)))` directly.
    public func filter(_ variable: String, notEquals value: String) -> Self {
        filter(.notEquals(variable, .string(value)))
    }

    /// Filter: variable matches regex
    public func filter(_ variable: String, matches regex: String) -> Self {
        filter(.regex(variable, regex))
    }

    /// Filter: variable contains substring
    public func filter(_ variable: String, contains substring: String) -> Self {
        filter(.contains(variable, substring))
    }

    /// Filter: variable starts with prefix
    public func filter(_ variable: String, startsWith prefix: String) -> Self {
        filter(.startsWith(variable, prefix))
    }

    /// Filter: variable ends with suffix
    public func filter(_ variable: String, endsWith suffix: String) -> Self {
        filter(.endsWith(variable, suffix))
    }

    /// Filter: variable is bound
    public func filterBound(_ variable: String) -> Self {
        filter(.bound(variable))
    }

    /// Filter: variable is not bound
    public func filterNotBound(_ variable: String) -> Self {
        filter(.notBound(variable))
    }

    /// Filter: two variables are equal
    public func filter(_ variable1: String, equalsVariable variable2: String) -> Self {
        filter(.variableEquals(variable1, variable2))
    }

    /// Filter: two variables are not equal
    public func filter(_ variable1: String, notEqualsVariable variable2: String) -> Self {
        filter(.variableNotEquals(variable1, variable2))
    }

    // MARK: - GROUP BY

    /// Start a GROUP BY query
    ///
    /// Groups results by the specified variables and allows aggregate functions.
    ///
    /// **Example**:
    /// ```swift
    /// let results = try await context.sparql(Statement.self)
    ///     .defaultIndex()
    ///     .where("?person", "knows", "?friend")
    ///     .groupBy("?person")
    ///     .count("?friend", as: "friendCount")
    ///     .execute()
    /// ```
    ///
    /// - Parameter variables: Variables to group by
    /// - Returns: A grouped query builder for adding aggregates
    public func groupBy(_ variables: String...) -> SPARQLGroupedQueryBuilder<T> {
        groupBy(variables)
    }

    /// Start a GROUP BY query (array version)
    public func groupBy(_ variables: [String]) -> SPARQLGroupedQueryBuilder<T> {
        SPARQLGroupedQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName,
            sourcePattern: graphPattern,
            groupVariables: variables
        )
    }

    // MARK: - Projection and Modifiers

    /// Select specific variables to return
    ///
    /// If not called, all variables from the patterns are returned.
    ///
    /// **Example**:
    /// ```swift
    /// .select("?person", "?name")
    /// ```
    public func select(_ variables: String...) -> Self {
        var copy = self
        copy.projectedVariables = variables
        return copy
    }

    /// Select specific variables (array version)
    public func select(_ variables: [String]) -> Self {
        var copy = self
        copy.projectedVariables = variables
        return copy
    }

    /// Return only distinct results
    public func distinct(_ enabled: Bool = true) -> Self {
        var copy = self
        copy.isDistinct = enabled
        return copy
    }

    /// Limit the number of results
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    /// Skip the first N results
    public func offset(_ count: Int) -> Self {
        var copy = self
        copy.offsetCount = count
        return copy
    }

    // MARK: - ORDER BY

    /// Add an ORDER BY sort key (ascending)
    ///
    /// **Example**:
    /// ```swift
    /// .orderBy("?name")
    /// .orderBy("?age", ascending: false)
    /// ```
    public func orderBy(_ variable: String, ascending: Bool = true) -> Self {
        var copy = self
        copy.sortKeys.append(.variable(variable, ascending: ascending))
        return copy
    }

    /// Add an ORDER BY sort key (descending)
    ///
    /// Convenience for `.orderBy(variable, ascending: false)`.
    public func orderByDesc(_ variable: String) -> Self {
        orderBy(variable, ascending: false)
    }

    // MARK: - Execution

    /// Execute the query and return results
    ///
    /// Follows W3C SPARQL 1.1 Section 15 execution order:
    /// 1. Pattern evaluation (WHERE)
    /// 2. ORDER BY
    /// 3. Projection (SELECT)
    /// 4. DISTINCT
    /// 5. OFFSET / LIMIT (Slice)
    public func execute() async throws -> SPARQLResult {
        guard !fromFieldName.isEmpty else {
            throw SPARQLQueryError.indexNotConfigured
        }

        if graphPattern.isEmpty {
            throw SPARQLQueryError.noPatterns
        }

        let executor = SPARQLQueryExecutor<T>(
            queryContext: queryContext,
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName
        )

        let allVariables = graphPattern.variables
        let projection = projectedVariables ?? Array(allVariables).sorted()

        let startTime = DispatchTime.now()

        let hasOrderBy = !sortKeys.isEmpty
        let needsAllResults = hasOrderBy || isDistinct

        // Step 1: Pattern evaluation (WHERE)
        var (bindings, stats) = try await executor.execute(
            pattern: graphPattern,
            limit: needsAllResults ? nil : limitCount,
            offset: needsAllResults ? 0 : offsetCount
        )

        // Step 2: ORDER BY (before projection, per W3C Section 15)
        if hasOrderBy {
            bindings = BindingSorter.sort(bindings, by: sortKeys)
        }

        // Step 3: Projection (SELECT)
        let projectionSet = Set(projection)
        var projected = bindings.map { $0.project(projectionSet) }

        // Step 4: DISTINCT
        if isDistinct {
            var seen = Set<VariableBinding>()
            projected = projected.filter { seen.insert($0).inserted }
        }

        // Step 5: OFFSET / LIMIT (Slice)
        if needsAllResults {
            if offsetCount > 0 {
                projected = Array(projected.dropFirst(offsetCount))
            }
            if let limit = limitCount {
                projected = Array(projected.prefix(limit))
            }
        }

        let endTime = DispatchTime.now()
        stats.durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        let resultCount = projected.count
        let isComplete = limitCount == nil || resultCount < limitCount!
        let limitReason: SPARQLLimitReason? = (limitCount != nil && resultCount >= limitCount!) ? .explicitLimit : nil

        return SPARQLResult(
            bindings: projected,
            projectedVariables: projection,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: stats
        )
    }

    /// Execute and return just the first result (or nil)
    public func first() async throws -> VariableBinding? {
        try await limit(1).execute().bindings.first
    }

    /// Execute and return count of results
    ///
    /// Note: This executes the full query. For large result sets,
    /// consider using a limit or estimating cardinality.
    public func count() async throws -> Int {
        try await execute().count
    }

    /// Check if any results exist
    public func exists() async throws -> Bool {
        try await first() != nil
    }

    // MARK: - Query Info

    /// Get all variables in the query
    public var variables: Set<String> {
        graphPattern.variables
    }

    /// Get the graph pattern (for debugging/inspection)
    public var pattern: ExecutionPattern {
        graphPattern
    }
}

// MARK: - CustomStringConvertible

extension SPARQLQueryBuilder: CustomStringConvertible {
    public var description: String {
        var parts: [String] = []

        if let vars = projectedVariables {
            parts.append("SELECT \(vars.joined(separator: ", "))")
        } else {
            parts.append("SELECT *")
        }

        parts.append("WHERE \(graphPattern)")

        if isDistinct {
            parts.append("DISTINCT")
        }

        if let limit = limitCount {
            parts.append("LIMIT \(limit)")
        }

        if offsetCount > 0 {
            parts.append("OFFSET \(offsetCount)")
        }

        return parts.joined(separator: " ")
    }
}

// MARK: - QueryIR Integration

extension SPARQLQueryBuilder {

    /// Add a triple pattern using QueryIR.SPARQLTerm values
    ///
    /// Converts QueryIR terms to ExecutionTerm for pattern matching.
    ///
    /// **Example**:
    /// ```swift
    /// .where(.var("person"), .iri("knows"), .iri("Bob"))
    /// ```
    public func `where`(
        _ subject: QueryIR.SPARQLTerm,
        _ predicate: QueryIR.SPARQLTerm,
        _ object: QueryIR.SPARQLTerm
    ) -> Self {
        `where`(
            ExecutionTerm(subject),
            ExecutionTerm(predicate),
            ExecutionTerm(object)
        )
    }

    /// Add a FILTER using a QueryIR.Expression
    ///
    /// Evaluates the expression against each binding using ExpressionEvaluator.
    /// Follows SPARQL §17.2 semantics: evaluation errors yield `false`.
    ///
    /// **Example**:
    /// ```swift
    /// .filter(.greaterThanOrEqual(.var("age"), .int(18)))
    /// ```
    public func filter(_ expression: QueryIR.Expression) -> Self {
        filter(.custom { binding in
            ExpressionEvaluator.evaluateAsBoolean(expression, binding: binding)
        })
    }
}

// MARK: - ExecutionTerm ← QueryIR.SPARQLTerm

extension ExecutionTerm {

    /// Convert a QueryIR.SPARQLTerm to an ExecutionTerm
    public init(_ sparqlTerm: QueryIR.SPARQLTerm) {
        switch sparqlTerm {
        case .variable(let name):
            self = .variable(name.hasPrefix("?") ? name : "?\(name)")
        case .iri(let value):
            self = .value(.string(value))
        case .prefixedName(let prefix, let local):
            self = .value(.string("\(prefix):\(local)"))
        case .literal(let lit):
            self = .value(lit.toFieldValue() ?? .null)
        case .blankNode(let id):
            self = .value(.string("_:\(id)"))
        case .quotedTriple(let s, let p, let o):
            // RDF-star quoted triples are stored as string representation
            self = .value(.string("<<\(s) \(p) \(o)>>"))
        }
    }
}
