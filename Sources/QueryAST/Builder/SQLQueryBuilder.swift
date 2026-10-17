/// SQLQueryBuilder.swift
/// Fluent SQL query builder
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL)
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import DatabaseKit

/// SQL Query Builder for type-safe query construction
public struct SQLQueryBuilder: Sendable {
    private var projection: [ProjectionItem]
    private var source: DataSource
    private var filter: Expression?
    private var groupByExprs: [GroupingElement]?
    private var windows: [NamedWindow] = []
    private var havingExpr: Expression?
    private var orderByKeys: [SortKey]?
    private var pagination: QueryPagination
    private var quantifier: SelectQuantifier
    private var ctes: [NamedSubquery]?
    private var recursiveCTEs = false

    private static func source(for table: TableRef) -> DataSource {
        guard let alias = table.alias else { return .table(table) }
        let reference = TableRef(schema: table.schema, table: table.table, partitions: table.partitions)
        return .aliased(.table(reference), name: alias)
    }

    /// Initialize with a table name
    public init(from tableName: String) {
        self.projection = [.wildcard()]
        self.source = .table(TableRef(tableName))
        self.filter = nil
        self.groupByExprs = nil
        self.havingExpr = nil
        self.orderByKeys = nil
        self.pagination = QueryPagination()
        self.quantifier = .all
        self.ctes = nil
    }

    /// Initialize with a table reference
    public init(from table: TableRef) {
        self.projection = [.wildcard()]
        self.source = Self.source(for: table)
        self.filter = nil
        self.groupByExprs = nil
        self.havingExpr = nil
        self.orderByKeys = nil
        self.pagination = QueryPagination()
        self.quantifier = .all
        self.ctes = nil
    }

    /// Initialize with a custom source
    public init(source: DataSource) {
        self.projection = [.wildcard()]
        self.source = source
        self.filter = nil
        self.groupByExprs = nil
        self.havingExpr = nil
        self.orderByKeys = nil
        self.pagination = QueryPagination()
        self.quantifier = .all
        self.ctes = nil
    }
}

// MARK: - SELECT Clause

extension SQLQueryBuilder {
    /// Select all columns
    public func selectAll() -> SQLQueryBuilder {
        var builder = self
        builder.projection = [.wildcard()]
        return builder
    }

    /// Select specific columns
    public func select(_ columns: String...) -> SQLQueryBuilder {
        var builder = self
        builder.projection = columns.map { ProjectionItem(.column(ColumnRef(column: $0))) }
        return builder
    }

    /// Select with expressions
    public func select(_ items: ProjectionItem...) -> SQLQueryBuilder {
        var builder = self
        builder.projection = items
        return builder
    }

    /// Select DISTINCT
    public func selectDistinct(_ columns: String...) -> SQLQueryBuilder {
        var builder = self
        builder.projection = columns.map { ProjectionItem(.column(ColumnRef(column: $0))) }
        builder.quantifier = .distinct
        return builder
    }

    /// Select with alias
    public func select(_ column: String, as alias: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = [ProjectionItem(.column(ColumnRef(column: column)), alias: alias)]
        return builder
    }
}

// MARK: - WHERE Clause

extension SQLQueryBuilder {
    /// Add WHERE condition with expression
    public func `where`(_ condition: Expression) -> SQLQueryBuilder {
        var builder = self
        if let existing = builder.filter {
            builder.filter = .and(existing, condition)
        } else {
            builder.filter = condition
        }
        return builder
    }

    /// Add WHERE condition: column = literal
    public func `where`(_ column: String, equals literal: Literal) -> SQLQueryBuilder {
        self.where(
            .equal(
                .column(ColumnRef(column: column)),
                .literal(literal)
            )
        )
    }

    /// Add WHERE condition: column = value
    public func `where`<Value: QueryLiteralConvertible>(
        _ column: String,
        equals value: Value
    ) throws(QueryLiteralConversionError) -> SQLQueryBuilder {
        self.where(column, equals: try value.queryLiteral)
    }

    /// Add WHERE condition: column operator literal
    public func `where`(
        _ column: String,
        _ op: ComparisonOperator,
        _ literal: Literal
    ) -> SQLQueryBuilder {
        let colExpr = Expression.column(ColumnRef(column: column))
        let valExpr = Expression.literal(literal)

        let condition: Expression
        switch op {
        case .equal:
            condition = .equal(colExpr, valExpr)
        case .notEqual:
            condition = .notEqual(colExpr, valExpr)
        case .lessThan:
            condition = .lessThan(colExpr, valExpr)
        case .lessThanOrEqual:
            condition = .lessThanOrEqual(colExpr, valExpr)
        case .greaterThan:
            condition = .greaterThan(colExpr, valExpr)
        case .greaterThanOrEqual:
            condition = .greaterThanOrEqual(colExpr, valExpr)
        }

        return self.where(condition)
    }

    /// Add WHERE condition: column operator value
    public func `where`<Value: QueryLiteralConvertible>(
        _ column: String,
        _ op: ComparisonOperator,
        _ value: Value
    ) throws(QueryLiteralConversionError) -> SQLQueryBuilder {
        self.where(column, op, try value.queryLiteral)
    }

    /// Add WHERE column IS NULL
    public func whereNull(_ column: String) -> SQLQueryBuilder {
        self.where(.isNull(.column(ColumnRef(column: column))))
    }

    /// Add WHERE column IS NOT NULL
    public func whereNotNull(_ column: String) -> SQLQueryBuilder {
        self.where(.isNotNull(.column(ColumnRef(column: column))))
    }

    /// Add WHERE column LIKE pattern
    public func whereLike(_ column: String, pattern: String) -> SQLQueryBuilder {
        self.where(
            .patternMatch(subject: .column(ColumnRef(column: column)), pattern: .string(pattern),
                escape: .string("\\"), kind: .like)
        )
    }

    /// Add WHERE column IN (literals...)
    public func whereIn(_ column: String, _ literals: [Literal]) -> SQLQueryBuilder {
        return self.where(.inList(
            .column(ColumnRef(column: column)),
            values: literals.map { .literal($0) }
        ))
    }

    /// Add WHERE column IN (values...)
    public func whereIn<Value: QueryLiteralConvertible>(
        _ column: String,
        _ values: [Value]
    ) throws(QueryLiteralConversionError) -> SQLQueryBuilder {
        var expressions: [Expression] = []
        expressions.reserveCapacity(values.count)
        for value in values {
            expressions.append(.literal(try value.queryLiteral))
        }
        return self.where(
            .inList(
                .column(ColumnRef(column: column)),
                values: expressions
            )
        )
    }

    /// Add WHERE column NOT IN (literals...)
    public func whereNotIn(_ column: String, _ literals: [Literal]) -> SQLQueryBuilder {
        self.where(
            .notInList(
                .column(ColumnRef(column: column)),
                values: literals.map { .literal($0) }
            )
        )
    }

    /// Add WHERE column NOT IN (values...)
    public func whereNotIn<Value: QueryLiteralConvertible>(
        _ column: String,
        _ values: [Value]
    ) throws(QueryLiteralConversionError) -> SQLQueryBuilder {
        var expressions: [Expression] = []
        expressions.reserveCapacity(values.count)
        for value in values {
            expressions.append(.literal(try value.queryLiteral))
        }
        return self.where(
            .notInList(
                .column(ColumnRef(column: column)),
                values: expressions
            )
        )
    }

    /// Add WHERE column BETWEEN low AND high literals
    public func whereBetween(
        _ column: String,
        _ low: Literal,
        _ high: Literal
    ) -> SQLQueryBuilder {
        return self.where(.between(
            .column(ColumnRef(column: column)),
            low: .literal(low),
            high: .literal(high)
        ))
    }

    /// Add WHERE column BETWEEN low AND high values of the same type
    public func whereBetween<Value: QueryLiteralConvertible>(
        _ column: String,
        _ low: Value,
        _ high: Value
    ) throws(QueryLiteralConversionError) -> SQLQueryBuilder {
        self.whereBetween(
            column,
            try low.queryLiteral,
            try high.queryLiteral
        )
    }

    /// Add OR condition
    public func orWhere(_ condition: Expression) -> SQLQueryBuilder {
        var builder = self
        if let existing = builder.filter {
            builder.filter = .or(existing, condition)
        } else {
            builder.filter = condition
        }
        return builder
    }
}

/// Comparison operators
public enum ComparisonOperator: Sendable {
    case equal
    case notEqual
    case lessThan
    case lessThanOrEqual
    case greaterThan
    case greaterThanOrEqual
}

// MARK: - JOIN Clause

extension SQLQueryBuilder {
    /// Inner join with table name
    public func join(_ tableName: String, on condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .inner,
            left: builder.source,
            right: .table(TableRef(tableName)),
            condition: .on(condition)
        ))
        return builder
    }

    /// Inner join with table reference
    public func join(_ table: TableRef, on condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .inner,
            left: builder.source,
            right: Self.source(for: table),
            condition: .on(condition)
        ))
        return builder
    }

    /// Left join with table name
    public func leftJoin(_ tableName: String, on condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .left,
            left: builder.source,
            right: .table(TableRef(tableName)),
            condition: .on(condition)
        ))
        return builder
    }

    /// Left join with table reference
    public func leftJoin(_ table: TableRef, on condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .left,
            left: builder.source,
            right: Self.source(for: table),
            condition: .on(condition)
        ))
        return builder
    }

    /// Right join with table name
    public func rightJoin(_ tableName: String, on condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .right,
            left: builder.source,
            right: .table(TableRef(tableName)),
            condition: .on(condition)
        ))
        return builder
    }

    /// Right join with table reference
    public func rightJoin(_ table: TableRef, on condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .right,
            left: builder.source,
            right: Self.source(for: table),
            condition: .on(condition)
        ))
        return builder
    }

    /// Join using columns
    public func join(_ tableName: String, using columns: String...) -> SQLQueryBuilder {
        var builder = self
        builder.source = .join(JoinClause(
            type: .inner,
            left: builder.source,
            right: .table(TableRef(tableName)),
            condition: .using(columns)
        ))
        return builder
    }
}

// MARK: - GRAPH_TABLE (SQL/PGQ)

extension SQLQueryBuilder {
    /// Add GRAPH_TABLE source
    public func graphTable(_ graphName: String, match pattern: MatchPattern) -> SQLQueryBuilder {
        var builder = self
        builder.source = .graphTable(GraphTableSource(graphName: graphName, matchPattern: pattern))
        return builder
    }

    /// Add GRAPH_TABLE source with columns
    public func graphTable(
        _ graphName: String,
        match pattern: MatchPattern,
        columns: [(Expression, String)]
    ) -> SQLQueryBuilder {
        var builder = self
        builder.source = .graphTable(GraphTableSource(
            graphName: graphName,
            matchPattern: pattern,
            columns: columns.map { GraphTableColumn(expression: $0.0, alias: $0.1) }
        ))
        return builder
    }
}

// MARK: - GROUP BY Clause

extension SQLQueryBuilder {
    /// Group by columns
    public func groupBy(_ columns: String...) -> SQLQueryBuilder {
        var builder = self
        builder.groupByExprs = columns.map { .expression(.column(ColumnRef(column: $0))) }
        return builder
    }

    /// Group by expressions
    public func groupBy(_ expressions: Expression...) -> SQLQueryBuilder {
        var builder = self
        builder.groupByExprs = expressions.map(GroupingElement.expression)
        return builder
    }

    /// Add HAVING condition
    public func having(_ condition: Expression) -> SQLQueryBuilder {
        var builder = self
        builder.havingExpr = condition
        return builder
    }
}

// MARK: - ORDER BY Clause

extension SQLQueryBuilder {
    /// Order by column (ascending)
    public func orderBy(_ column: String, _ direction: SortDirection = .ascending) -> SQLQueryBuilder {
        var builder = self
        let key = SortKey(.column(ColumnRef(column: column)), direction: direction)
        if var existing = builder.orderByKeys {
            existing.append(key)
            builder.orderByKeys = existing
        } else {
            builder.orderByKeys = [key]
        }
        return builder
    }

    /// Order by expression
    public func orderBy(_ expression: Expression, _ direction: SortDirection = .ascending) -> SQLQueryBuilder {
        var builder = self
        let key = SortKey(expression, direction: direction)
        if var existing = builder.orderByKeys {
            existing.append(key)
            builder.orderByKeys = existing
        } else {
            builder.orderByKeys = [key]
        }
        return builder
    }

    /// Order by column descending
    public func orderByDesc(_ column: String) -> SQLQueryBuilder {
        orderBy(column, .descending)
    }
}

// MARK: - LIMIT/OFFSET Clause

extension SQLQueryBuilder {
    /// Set LIMIT
    public func limit(_ count: UInt64) -> SQLQueryBuilder {
        var builder = self
        builder.pagination = QueryPagination(limit: .count(.fixed(count), withTies: false), offset: builder.pagination.offset)
        return builder
    }

    /// Set OFFSET
    public func offset(_ count: UInt64) -> SQLQueryBuilder {
        var builder = self
        builder.pagination = QueryPagination(limit: builder.pagination.limit, offset: .fixed(count))
        return builder
    }

    /// Set LIMIT and OFFSET using a zero-based page index.
    public func paginate(
        pageIndex: UInt64,
        pageSize: UInt64
    ) throws(QueryPaginationError) -> SQLQueryBuilder {
        let (offset, overflow) = pageIndex.multipliedReportingOverflow(
            by: pageSize
        )
        guard !overflow else {
            throw .offsetOverflow(
                pageIndex: pageIndex,
                pageSize: pageSize
            )
        }
        return limit(pageSize).offset(offset)
    }
}

// MARK: - Common Table Expressions (WITH)

extension SQLQueryBuilder {
    /// Add CTE
    public func with(_ name: String, as query: SelectQuery) -> SQLQueryBuilder {
        with(NamedSubquery(name: name, query: query))
    }

    /// Add a complete CTE declaration, preserving SEARCH and CYCLE metadata.
    public func with(_ cte: NamedSubquery) -> SQLQueryBuilder {
        var builder = self
        if var existing = builder.ctes {
            existing.append(cte)
            builder.ctes = existing
        } else {
            builder.ctes = [cte]
        }
        return builder
    }

    /// Select recursive visibility for the complete WITH clause.
    public func withRecursive(_ enabled: Bool = true) -> SQLQueryBuilder {
        var builder = self
        builder.recursiveCTEs = enabled
        return builder
    }
}

// MARK: - Building

extension SQLQueryBuilder {
    /// Build the SelectQuery AST
    public func buildAST() -> SelectQuery {
        SelectQuery(
            projection: projection,
            source: source,
            filter: filter,
            groupBy: groupByExprs,
            having: havingExpr,
            windows: windows,
            orderBy: orderByKeys,
            quantifier: quantifier,
            pagination: pagination,
            subqueries: ctes,
            withRecursive: recursiveCTEs
        )
    }

    /// Generate SQL, preserving typed failures for unsupported literals.
    public func toSQL() throws(SQLSerializationError) -> String {
        try SQLSerializer().serialize(buildAST())
    }
}

// MARK: - Convenience Extensions

extension SQLQueryBuilder {
    /// Count rows
    public func count() -> SQLQueryBuilder {
        var builder = self
        builder.projection = [ProjectionItem(.aggregate(.sql(SQLAggregateCall(function: .count, arguments: []))), alias: "count")]
        return builder
    }

    /// Sum of column
    public func sum(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = [ProjectionItem(
            .aggregate(.sql(SQLAggregateCall(function: .sum, arguments: [.column(ColumnRef(column: column))]))),
            alias: "sum"
        )]
        return builder
    }

    /// Average of column
    public func avg(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = [ProjectionItem(
            .aggregate(.sql(SQLAggregateCall(function: .avg, arguments: [.column(ColumnRef(column: column))]))),
            alias: "avg"
        )]
        return builder
    }

    /// Max of column
    public func max(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = [ProjectionItem(
            .aggregate(.sql(SQLAggregateCall(function: .max, arguments: [.column(ColumnRef(column: column))]))),
            alias: "max"
        )]
        return builder
    }

    /// Min of column
    public func min(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = [ProjectionItem(
            .aggregate(.sql(SQLAggregateCall(function: .min, arguments: [.column(ColumnRef(column: column))]))),
            alias: "min"
        )]
        return builder
    }
}


extension SQLQueryBuilder {
    /// Sets the semantic grouping elements, preserving compound units and empty sets.
    public func groupBy(elements: [GroupingElement]) -> SQLQueryBuilder {
        var builder = self
        builder.groupByExprs = elements
        return builder
    }

    /// Sets named window definitions without expanding their references.
    public func window(_ definitions: NamedWindow...) -> SQLQueryBuilder {
        var builder = self
        builder.windows = definitions
        return builder
    }
}
