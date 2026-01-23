// SPARQLQueryBuilder.swift
// GraphIndex - SPARQL-like query builder
//
// Fluent builder for constructing SPARQL-like graph queries.

import Foundation
import Core
import DatabaseEngine
import Graph

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

    private var graphPattern: GraphPattern
    private var projectedVariables: [String]?
    private var limitCount: Int?
    private var offsetCount: Int
    private var isDistinct: Bool

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
            SPARQLTerm(stringLiteral: subject),
            SPARQLTerm(stringLiteral: predicate),
            SPARQLTerm(stringLiteral: object)
        )
    }

    /// Add a triple pattern using SPARQLTerm values
    public func `where`(
        _ subject: SPARQLTerm,
        _ predicate: SPARQLTerm,
        _ object: SPARQLTerm
    ) -> Self {
        var copy = self
        let pattern = TriplePattern(subject: subject, predicate: predicate, object: object)

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
        path: PropertyPath,
        _ object: String
    ) -> Self {
        wherePath(
            SPARQLTerm(stringLiteral: subject),
            path: path,
            SPARQLTerm(stringLiteral: object)
        )
    }

    /// Add a property path pattern using SPARQLTerm values
    public func wherePath(
        _ subject: SPARQLTerm,
        path: PropertyPath,
        _ object: SPARQLTerm
    ) -> Self {
        var copy = self

        // Convert property path to graph pattern
        let pathPattern = GraphPattern.propertyPath(subject: subject, path: path, object: object)

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
    public func filter(_ variable: String, equals value: String) -> Self {
        filter(.equals(variable, value))
    }

    /// Filter: variable not equals value
    public func filter(_ variable: String, notEquals value: String) -> Self {
        filter(.notEquals(variable, value))
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

    // MARK: - Execution

    /// Execute the query and return results
    public func execute() async throws -> SPARQLResult {
        guard !fromFieldName.isEmpty else {
            throw SPARQLQueryError.indexNotConfigured
        }

        // Validate pattern is not empty
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

        var (bindings, stats) = try await executor.execute(
            pattern: graphPattern,
            limit: isDistinct ? nil : limitCount,  // Distinct needs all results first
            offset: isDistinct ? 0 : offsetCount
        )

        // Apply projection FIRST (before distinct)
        // SPARQL semantics: DISTINCT operates on projected variables only
        let projectionSet = Set(projection)
        var projected = bindings.map { $0.project(projectionSet) }

        // Apply distinct AFTER projection
        // This ensures duplicates are based on selected variables, not all variables
        if isDistinct {
            var seen = Set<VariableBinding>()
            projected = projected.filter { seen.insert($0).inserted }

            // Apply offset and limit after distinct
            if offsetCount > 0 {
                projected = Array(projected.dropFirst(offsetCount))
            }
            if let limit = limitCount {
                projected = Array(projected.prefix(limit))
            }
        }

        let endTime = DispatchTime.now()
        stats.durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        // Determine completeness based on final result count
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
    public var pattern: GraphPattern {
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
