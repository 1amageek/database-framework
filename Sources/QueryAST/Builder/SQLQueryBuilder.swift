/// SQLQueryBuilder.swift
/// Fluent SQL query builder
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL)
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import Foundation

/// SQL Query Builder for type-safe query construction
public struct SQLQueryBuilder: Sendable {
    private var projection: Projection
    private var source: DataSource
    private var filter: Expression?
    private var groupByExprs: [Expression]?
    private var havingExpr: Expression?
    private var orderByKeys: [SortKey]?
    private var limitCount: Int?
    private var offsetCount: Int?
    private var distinctFlag: Bool
    private var ctes: [NamedSubquery]?

    /// Initialize with a table name
    public init(from tableName: String) {
        self.projection = .all
        self.source = .table(TableRef(tableName))
        self.filter = nil
        self.groupByExprs = nil
        self.havingExpr = nil
        self.orderByKeys = nil
        self.limitCount = nil
        self.offsetCount = nil
        self.distinctFlag = false
        self.ctes = nil
    }

    /// Initialize with a table reference
    public init(from table: TableRef) {
        self.projection = .all
        self.source = .table(table)
        self.filter = nil
        self.groupByExprs = nil
        self.havingExpr = nil
        self.orderByKeys = nil
        self.limitCount = nil
        self.offsetCount = nil
        self.distinctFlag = false
        self.ctes = nil
    }

    /// Initialize with a custom source
    public init(source: DataSource) {
        self.projection = .all
        self.source = source
        self.filter = nil
        self.groupByExprs = nil
        self.havingExpr = nil
        self.orderByKeys = nil
        self.limitCount = nil
        self.offsetCount = nil
        self.distinctFlag = false
        self.ctes = nil
    }
}

// MARK: - SELECT Clause

extension SQLQueryBuilder {
    /// Select all columns
    public func selectAll() -> SQLQueryBuilder {
        var builder = self
        builder.projection = .all
        return builder
    }

    /// Select specific columns
    public func select(_ columns: String...) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items(columns.map { ProjectionItem(.column(ColumnRef(column: $0))) })
        return builder
    }

    /// Select with expressions
    public func select(_ items: ProjectionItem...) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items(items)
        return builder
    }

    /// Select DISTINCT
    public func selectDistinct(_ columns: String...) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .distinctItems(columns.map { ProjectionItem(.column(ColumnRef(column: $0))) })
        builder.distinctFlag = true
        return builder
    }

    /// Select with alias
    public func select(_ column: String, as alias: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(.column(ColumnRef(column: column)), alias: alias)])
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

    /// Add WHERE condition: column = value
    public func `where`(_ column: String, equals value: Any) -> SQLQueryBuilder {
        guard let lit = Literal(value) else { return self }
        return self.where(.equal(.column(ColumnRef(column: column)), .literal(lit)))
    }

    /// Add WHERE condition: column operator value
    public func `where`(_ column: String, _ op: ComparisonOperator, _ value: Any) -> SQLQueryBuilder {
        guard let lit = Literal(value) else { return self }
        let colExpr = Expression.column(ColumnRef(column: column))
        let valExpr = Expression.literal(lit)

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
        case .like:
            if case .string(let pattern) = lit {
                condition = .like(colExpr, pattern: pattern)
            } else {
                return self
            }
        case .inList:
            // For IN, value should be an array
            return self
        case .notInList:
            return self
        }

        return self.where(condition)
    }

    /// Add WHERE column IS NULL
    public func whereNull(_ column: String) -> SQLQueryBuilder {
        self.where(.isNull(.column(ColumnRef(column: column))))
    }

    /// Add WHERE column IS NOT NULL
    public func whereNotNull(_ column: String) -> SQLQueryBuilder {
        self.where(.isNotNull(.column(ColumnRef(column: column))))
    }

    /// Add WHERE column IN (values...)
    public func whereIn(_ column: String, _ values: [Any]) -> SQLQueryBuilder {
        let literals = values.compactMap { Literal($0) }
        guard literals.count == values.count else { return self }
        return self.where(.inList(
            .column(ColumnRef(column: column)),
            values: literals.map { .literal($0) }
        ))
    }

    /// Add WHERE column BETWEEN low AND high
    public func whereBetween(_ column: String, _ low: Any, _ high: Any) -> SQLQueryBuilder {
        guard let lowLit = Literal(low), let highLit = Literal(high) else { return self }
        return self.where(.between(
            .column(ColumnRef(column: column)),
            low: .literal(lowLit),
            high: .literal(highLit)
        ))
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
    case like
    case inList
    case notInList
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
            right: .table(table),
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
            right: .table(table),
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
            right: .table(table),
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
        builder.groupByExprs = columns.map { .column(ColumnRef(column: $0)) }
        return builder
    }

    /// Group by expressions
    public func groupBy(_ expressions: Expression...) -> SQLQueryBuilder {
        var builder = self
        builder.groupByExprs = expressions
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
    public func limit(_ count: Int) -> SQLQueryBuilder {
        var builder = self
        builder.limitCount = count
        return builder
    }

    /// Set OFFSET
    public func offset(_ count: Int) -> SQLQueryBuilder {
        var builder = self
        builder.offsetCount = count
        return builder
    }

    /// Set LIMIT and OFFSET (pagination)
    public func paginate(page: Int, perPage: Int) -> SQLQueryBuilder {
        limit(perPage).offset((page - 1) * perPage)
    }
}

// MARK: - Common Table Expressions (WITH)

extension SQLQueryBuilder {
    /// Add CTE
    public func with(_ name: String, as query: SelectQuery) -> SQLQueryBuilder {
        var builder = self
        let cte = NamedSubquery(name: name, query: query)
        if var existing = builder.ctes {
            existing.append(cte)
            builder.ctes = existing
        } else {
            builder.ctes = [cte]
        }
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
            orderBy: orderByKeys,
            limit: limitCount,
            offset: offsetCount,
            distinct: distinctFlag,
            subqueries: ctes
        )
    }

    /// Generate SQL string
    public func toSQL() -> String {
        let ast = buildAST()
        return generateSQL(from: ast)
    }

    private func generateSQL(from query: SelectQuery) -> String {
        var sql = ""

        // WITH clause
        if let ctes = query.subqueries, !ctes.isEmpty {
            sql += "WITH "
            sql += ctes.map { cte in
                var cteSQL = "\(cte.name)"
                if let cols = cte.columns {
                    cteSQL += " (\(cols.joined(separator: ", ")))"
                }
                cteSQL += " AS (\(generateSQL(from: cte.query)))"
                return cteSQL
            }.joined(separator: ", ")
            sql += " "
        }

        // SELECT clause
        sql += "SELECT "
        if query.distinct { sql += "DISTINCT " }

        switch query.projection {
        case .all:
            sql += "*"
        case .allFrom(let table):
            sql += "\(table).*"
        case .items(let items), .distinctItems(let items):
            sql += items.map { item in
                var s = item.expression.toSQL()
                if let alias = item.alias {
                    s += " AS \(alias)"
                }
                return s
            }.joined(separator: ", ")
        }

        // FROM clause
        sql += " FROM \(generateDataSourceSQL(query.source))"

        // WHERE clause
        if let filter = query.filter {
            sql += " WHERE \(filter.toSQL())"
        }

        // GROUP BY clause
        if let groupBy = query.groupBy, !groupBy.isEmpty {
            sql += " GROUP BY \(groupBy.map { $0.toSQL() }.joined(separator: ", "))"
        }

        // HAVING clause
        if let having = query.having {
            sql += " HAVING \(having.toSQL())"
        }

        // ORDER BY clause
        if let orderBy = query.orderBy, !orderBy.isEmpty {
            sql += " ORDER BY "
            sql += orderBy.map { key in
                var s = key.expression.toSQL()
                s += key.direction == .descending ? " DESC" : " ASC"
                if let nulls = key.nulls {
                    s += nulls == .first ? " NULLS FIRST" : " NULLS LAST"
                }
                return s
            }.joined(separator: ", ")
        }

        // LIMIT clause
        if let limit = query.limit {
            sql += " LIMIT \(limit)"
        }

        // OFFSET clause
        if let offset = query.offset {
            sql += " OFFSET \(offset)"
        }

        return sql
    }

    private func generateDataSourceSQL(_ source: DataSource) -> String {
        switch source {
        case .table(let ref):
            return ref.description

        case .subquery(let query, let alias):
            return "(\(generateSQL(from: query))) AS \(alias)"

        case .join(let clause):
            let left = generateDataSourceSQL(clause.left)
            let right = generateDataSourceSQL(clause.right)
            let joinType = joinTypeSQL(clause.type)
            var sql = "\(left) \(joinType) \(right)"
            if let cond = clause.condition {
                switch cond {
                case .on(let expr):
                    sql += " ON \(expr.toSQL())"
                case .using(let cols):
                    sql += " USING (\(cols.joined(separator: ", ")))"
                }
            }
            return sql

        case .graphTable(let gtSource):
            return gtSource.toSQL()

        case .union(let sources):
            return sources.map { generateDataSourceSQL($0) }.joined(separator: " UNION ")

        case .unionAll(let sources):
            return sources.map { generateDataSourceSQL($0) }.joined(separator: " UNION ALL ")

        default:
            return "<source>"
        }
    }

    private func joinTypeSQL(_ type: JoinType) -> String {
        switch type {
        case .inner: return "INNER JOIN"
        case .left: return "LEFT JOIN"
        case .right: return "RIGHT JOIN"
        case .full: return "FULL JOIN"
        case .cross: return "CROSS JOIN"
        case .natural: return "NATURAL JOIN"
        case .naturalLeft: return "NATURAL LEFT JOIN"
        case .naturalRight: return "NATURAL RIGHT JOIN"
        case .naturalFull: return "NATURAL FULL JOIN"
        case .lateral: return "LATERAL JOIN"
        case .leftLateral: return "LEFT LATERAL JOIN"
        }
    }
}

// MARK: - Convenience Extensions

extension SQLQueryBuilder {
    /// Count rows
    public func count() -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(.aggregate(.count(nil, distinct: false)), alias: "count")])
        return builder
    }

    /// Sum of column
    public func sum(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.sum(.column(ColumnRef(column: column)), distinct: false)),
            alias: "sum"
        )])
        return builder
    }

    /// Average of column
    public func avg(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.avg(.column(ColumnRef(column: column)), distinct: false)),
            alias: "avg"
        )])
        return builder
    }

    /// Max of column
    public func max(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.max(.column(ColumnRef(column: column)))),
            alias: "max"
        )])
        return builder
    }

    /// Min of column
    public func min(_ column: String) -> SQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.min(.column(ColumnRef(column: column)))),
            alias: "min"
        )])
        return builder
    }
}
