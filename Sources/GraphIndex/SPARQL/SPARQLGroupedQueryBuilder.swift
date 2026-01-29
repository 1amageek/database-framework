// SPARQLGroupedQueryBuilder.swift
// GraphIndex - SPARQL GROUP BY query builder
//
// Fluent builder for constructing SPARQL GROUP BY queries with aggregation.

import Foundation
import Core
import DatabaseEngine
import Graph
import QueryIR

/// Builder for SPARQL GROUP BY queries with aggregation
///
/// Supports:
/// - GROUP BY variables
/// - Aggregate functions (COUNT, SUM, AVG, MIN, MAX, SAMPLE, GROUP_CONCAT)
/// - HAVING filter
/// - SELECT projection
/// - DISTINCT, LIMIT, OFFSET modifiers
///
/// **Design**: Immutable builder pattern. Each method returns a new builder.
///
/// **Usage**:
/// ```swift
/// let results = try await context.sparql(Statement.self)
///     .defaultIndex()
///     .where("?person", "knows", "?friend")
///     .groupBy("?person")
///     .count("?friend", as: "friendCount")
///     .having(.custom { $0.int("friendCount").map { $0 > 5 } ?? false })
///     .execute()
/// ```
public struct SPARQLGroupedQueryBuilder<T: Persistable>: Sendable {

    // MARK: - Configuration

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    // MARK: - Query State

    private let sourcePattern: ExecutionPattern
    private var groupVariables: [String]
    private var aggregates: [AggregateExpression]
    private var havingExpression: FilterExpression?
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
        toFieldName: String,
        sourcePattern: ExecutionPattern,
        groupVariables: [String]
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
        self.sourcePattern = sourcePattern
        self.groupVariables = groupVariables
        self.aggregates = []
        self.havingExpression = nil
        self.projectedVariables = nil
        self.limitCount = nil
        self.offsetCount = 0
        self.isDistinct = false
        self.sortKeys = []
    }

    // MARK: - Aggregate Functions

    /// Add COUNT(*) aggregate
    public func countAll(as alias: String) -> Self {
        addAggregate(.countAll(as: alias))
    }

    /// Add COUNT(DISTINCT *) aggregate
    public func countAllDistinct(as alias: String) -> Self {
        addAggregate(.countAllDistinct(as: alias))
    }

    /// Add COUNT(?var) aggregate
    public func count(_ variable: String, as alias: String) -> Self {
        addAggregate(.count(variable, as: alias))
    }

    /// Add COUNT(DISTINCT ?var) aggregate
    public func countDistinct(_ variable: String, as alias: String) -> Self {
        addAggregate(.countDistinct(variable, as: alias))
    }

    /// Add SUM(?var) aggregate
    public func sum(_ variable: String, as alias: String) -> Self {
        addAggregate(.sum(variable, as: alias))
    }

    /// Add AVG(?var) aggregate
    public func avg(_ variable: String, as alias: String) -> Self {
        addAggregate(.avg(variable, as: alias))
    }

    /// Add MIN(?var) aggregate
    public func min(_ variable: String, as alias: String) -> Self {
        addAggregate(.min(variable, as: alias))
    }

    /// Add MAX(?var) aggregate
    public func max(_ variable: String, as alias: String) -> Self {
        addAggregate(.max(variable, as: alias))
    }

    /// Add SAMPLE(?var) aggregate
    public func sample(_ variable: String, as alias: String) -> Self {
        addAggregate(.sample(variable, as: alias))
    }

    /// Add GROUP_CONCAT(?var) aggregate
    public func groupConcat(_ variable: String, separator: String = " ", as alias: String) -> Self {
        addAggregate(.groupConcat(variable, separator: separator, as: alias))
    }

    /// Add GROUP_CONCAT(DISTINCT ?var) aggregate
    public func groupConcatDistinct(_ variable: String, separator: String = " ", as alias: String) -> Self {
        addAggregate(.groupConcatDistinct(variable, separator: separator, as: alias))
    }

    /// Add a custom aggregate expression
    public func aggregate(_ expression: AggregateExpression) -> Self {
        addAggregate(expression)
    }

    private func addAggregate(_ expression: AggregateExpression) -> Self {
        var copy = self
        copy.aggregates.append(expression)
        return copy
    }

    // MARK: - HAVING

    /// Add a HAVING filter (applied after aggregation)
    ///
    /// The HAVING clause filters grouped results based on aggregate values.
    ///
    /// **Example**:
    /// ```swift
    /// .having(.custom { binding in
    ///     guard let count = binding.int("friendCount") else { return false }
    ///     return count > 5
    /// })
    /// ```
    public func having(_ expression: FilterExpression) -> Self {
        var copy = self
        copy.havingExpression = expression
        return copy
    }

    /// HAVING with variable greater than value
    public func having(_ variable: String, greaterThan value: Int) -> Self {
        having(.custom { binding in
            guard let v = binding.int(variable) else { return false }
            return v > value
        })
    }

    /// HAVING with variable less than value
    public func having(_ variable: String, lessThan value: Int) -> Self {
        having(.custom { binding in
            guard let v = binding.int(variable) else { return false }
            return v < value
        })
    }

    /// HAVING with variable equals value
    public func having(_ variable: String, equals value: Int) -> Self {
        having(.custom { binding in
            guard let v = binding.int(variable) else { return false }
            return v == value
        })
    }

    /// HAVING with variable not equals value
    public func having(_ variable: String, notEquals value: Int) -> Self {
        having(.custom { binding in
            guard let v = binding.int(variable) else { return false }
            return v != value
        })
    }

    /// HAVING with a QueryIR.Expression
    ///
    /// Evaluates the expression against each grouped binding using ExpressionEvaluator.
    /// Follows SPARQL §17.2 semantics: evaluation errors yield `false`.
    ///
    /// **Example**:
    /// ```swift
    /// .having(.greaterThan(.var("friendCount"), .int(5)))
    /// ```
    public func having(_ expression: QueryIR.Expression) -> Self {
        having(.custom { binding in
            ExpressionEvaluator.evaluateAsBoolean(expression, binding: binding)
        })
    }

    // MARK: - Projection and Modifiers

    /// Select specific variables to return
    ///
    /// If not called, all group variables and aggregate aliases are returned.
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
    /// Can sort by group variables or aggregate aliases.
    ///
    /// **Example**:
    /// ```swift
    /// .orderBy("friendCount", ascending: false) // Most friends first
    /// ```
    public func orderBy(_ variable: String, ascending: Bool = true) -> Self {
        var copy = self
        copy.sortKeys.append(.variable(variable, ascending: ascending))
        return copy
    }

    /// Add an ORDER BY sort key (descending)
    public func orderByDesc(_ variable: String) -> Self {
        orderBy(variable, ascending: false)
    }

    // MARK: - Execution

    /// Execute the grouped query and return results
    ///
    /// Follows W3C SPARQL 1.1 Section 15 execution order:
    /// 1. Pattern evaluation + GROUP BY + HAVING
    /// 2. ORDER BY
    /// 3. Projection (SELECT)
    /// 4. DISTINCT
    /// 5. OFFSET / LIMIT (Slice)
    public func execute() async throws -> SPARQLGroupedResult {
        guard !fromFieldName.isEmpty else {
            throw SPARQLQueryError.indexNotConfigured
        }

        // Validate we have at least one group variable or aggregate
        if groupVariables.isEmpty && aggregates.isEmpty {
            throw SPARQLQueryError.invalidGroupBy("GROUP BY requires at least one group variable or aggregate")
        }

        // Build the group by pattern
        let groupByPattern = ExecutionPattern.groupBy(
            sourcePattern,
            groupVariables: groupVariables,
            aggregates: aggregates,
            having: havingExpression
        )

        let indexName = "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"
        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? GraphIndexKind<T> else {
            throw SPARQLQueryError.indexNotFound(indexName)
        }

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        let executor = SPARQLQueryExecutor(
            database: queryContext.context.container.database,
            indexSubspace: indexSubspace,
            strategy: kind.strategy,
            fromFieldName: fromFieldName,
            edgeFieldName: edgeFieldName,
            toFieldName: toFieldName
        )

        let startTime = DispatchTime.now()

        // Step 1: Pattern evaluation + GROUP BY + HAVING
        var (bindings, stats) = try await executor.executeGrouped(
            pattern: groupByPattern,
            groupVariables: groupVariables,
            aggregates: aggregates,
            having: havingExpression
        )

        // Step 2: ORDER BY (before projection, per W3C Section 15)
        if !sortKeys.isEmpty {
            bindings = BindingSorter.sort(bindings, by: sortKeys)
        }

        // Step 3: Projection (SELECT)
        var outputVariables = groupVariables + aggregates.map { $0.alias }
        if let projected = projectedVariables {
            outputVariables = projected
        }
        let projectionSet = Set(outputVariables)
        var projected = bindings.map { $0.project(projectionSet) }

        // Step 4: DISTINCT
        if isDistinct {
            var seen = Set<VariableBinding>()
            projected = projected.filter { seen.insert($0).inserted }
        }

        // Step 5: OFFSET / LIMIT (Slice)
        if offsetCount > 0 {
            projected = Array(projected.dropFirst(offsetCount))
        }
        if let limit = limitCount {
            projected = Array(projected.prefix(limit))
        }

        let endTime = DispatchTime.now()
        var finalStats = stats
        finalStats.durationNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds

        return SPARQLGroupedResult(
            bindings: projected,
            groupVariables: groupVariables,
            aggregateAliases: aggregates.map { $0.alias },
            projectedVariables: outputVariables,
            isComplete: limitCount == nil || projected.count < limitCount!,
            statistics: finalStats
        )
    }

    /// Execute and return just the first result (or nil)
    public func first() async throws -> VariableBinding? {
        try await limit(1).execute().bindings.first
    }

    // MARK: - Query Info

    /// Get all output variables
    public var outputVariables: [String] {
        groupVariables + aggregates.map { $0.alias }
    }
}

// MARK: - SPARQLGroupedResult

/// Result of a SPARQL GROUP BY query
public struct SPARQLGroupedResult: Sendable {

    /// The result bindings (each represents one group)
    public let bindings: [VariableBinding]

    /// The variables used for grouping
    public let groupVariables: [String]

    /// The aggregate aliases in results
    public let aggregateAliases: [String]

    /// The projected variables
    public let projectedVariables: [String]

    /// Whether all results were returned
    public let isComplete: Bool

    /// Execution statistics
    public let statistics: ExecutionStatistics

    /// Number of groups
    public var count: Int {
        bindings.count
    }

    /// Whether results are empty
    public var isEmpty: Bool {
        bindings.isEmpty
    }

    /// Get aggregate value from first result as FieldValue
    public func firstAggregate(_ alias: String) -> FieldValue? {
        bindings.first?[alias]
    }

    /// Get aggregate value from first result as String
    public func firstAggregateString(_ alias: String) -> String? {
        bindings.first?.string(alias)
    }

    /// Get numeric aggregate from first result
    public func firstNumericAggregate(_ alias: String) -> Int? {
        guard let value = firstAggregate(alias) else { return nil }
        if let i = value.int64Value { return Int(i) }
        return nil
    }
}

// MARK: - CustomStringConvertible

extension SPARQLGroupedQueryBuilder: CustomStringConvertible {
    public var description: String {
        var parts: [String] = []

        if let vars = projectedVariables {
            parts.append("SELECT \(vars.joined(separator: ", "))")
        } else {
            let outputVars = groupVariables + aggregates.map { $0.description }
            parts.append("SELECT \(outputVars.joined(separator: ", "))")
        }

        parts.append("WHERE \(sourcePattern)")
        parts.append("GROUP BY \(groupVariables.joined(separator: ", "))")

        if let having = havingExpression {
            parts.append("HAVING \(having)")
        }

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
