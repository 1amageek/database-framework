/// SelectQuery.swift
/// Unified SELECT query representation for SQL and SPARQL
///
/// Reference:
/// - ISO/IEC 9075:2023 (SELECT statement)
/// - W3C SPARQL 1.1/1.2 (SELECT query)

import Foundation

/// Unified SELECT query representation
/// This structure can represent both SQL SELECT and SPARQL SELECT queries
public struct SelectQuery: Sendable, Equatable, Hashable {
    /// Projection (SELECT clause)
    public let projection: Projection

    /// Data source (FROM clause / WHERE { } / GRAPH_TABLE)
    public let source: DataSource

    /// Filter condition (WHERE clause / FILTER)
    public let filter: Expression?

    /// Group by expressions (GROUP BY)
    public let groupBy: [Expression]?

    /// Having condition (HAVING)
    public let having: Expression?

    /// Order by keys (ORDER BY)
    public let orderBy: [SortKey]?

    /// Result limit (LIMIT)
    public let limit: Int?

    /// Result offset (OFFSET)
    public let offset: Int?

    /// DISTINCT flag
    public let distinct: Bool

    /// Common table expressions (WITH clause / SPARQL subquery)
    public let subqueries: [NamedSubquery]?

    /// REDUCED flag (SPARQL)
    public let reduced: Bool

    public init(
        projection: Projection,
        source: DataSource,
        filter: Expression? = nil,
        groupBy: [Expression]? = nil,
        having: Expression? = nil,
        orderBy: [SortKey]? = nil,
        limit: Int? = nil,
        offset: Int? = nil,
        distinct: Bool = false,
        subqueries: [NamedSubquery]? = nil,
        reduced: Bool = false
    ) {
        self.projection = projection
        self.source = source
        self.filter = filter
        self.groupBy = groupBy
        self.having = having
        self.orderBy = orderBy
        self.limit = limit
        self.offset = offset
        self.distinct = distinct
        self.subqueries = subqueries
        self.reduced = reduced
    }
}

// MARK: - Query Analysis

extension SelectQuery {
    /// Returns all variables referenced in the query (SPARQL)
    public var referencedVariables: Set<String> {
        var vars = Set<String>()
        collectVariables(from: source, into: &vars)
        if let filter = filter {
            collectVariables(from: filter, into: &vars)
        }
        if let groupBy = groupBy {
            for expr in groupBy {
                collectVariables(from: expr, into: &vars)
            }
        }
        if let having = having {
            collectVariables(from: having, into: &vars)
        }
        if let orderBy = orderBy {
            for key in orderBy {
                collectVariables(from: key.expression, into: &vars)
            }
        }
        return vars
    }

    /// Returns all columns referenced in the query (SQL)
    public var referencedColumns: Set<ColumnRef> {
        var cols = Set<ColumnRef>()
        // Collect from projection
        switch projection {
        case .all, .allFrom:
            break
        case .items(let items), .distinctItems(let items):
            for item in items {
                collectColumns(from: item.expression, into: &cols)
            }
        }
        collectColumns(from: source, into: &cols)
        if let filter = filter {
            collectColumns(from: filter, into: &cols)
        }
        if let groupBy = groupBy {
            for expr in groupBy {
                collectColumns(from: expr, into: &cols)
            }
        }
        if let having = having {
            collectColumns(from: having, into: &cols)
        }
        if let orderBy = orderBy {
            for key in orderBy {
                collectColumns(from: key.expression, into: &cols)
            }
        }
        return cols
    }

    /// Returns true if this query contains aggregation
    public var hasAggregation: Bool {
        groupBy != nil && !groupBy!.isEmpty
    }

    /// Returns true if this query is a correlated subquery
    public var isCorrelated: Bool {
        // A correlated subquery references columns from outer queries
        // This is a simplified check - full implementation would track scopes
        false
    }

    private func collectVariables(from source: DataSource, into vars: inout Set<String>) {
        switch source {
        case .graphPattern(let pattern):
            collectVariables(from: pattern, into: &vars)
        case .namedGraph(_, let pattern):
            collectVariables(from: pattern, into: &vars)
        case .service(_, let pattern, _):
            collectVariables(from: pattern, into: &vars)
        case .subquery(let query, _):
            vars.formUnion(query.referencedVariables)
        case .join(let clause):
            collectVariables(from: clause.left, into: &vars)
            collectVariables(from: clause.right, into: &vars)
        case .union(let sources), .unionAll(let sources), .intersect(let sources):
            for s in sources {
                collectVariables(from: s, into: &vars)
            }
        case .except(let left, let right):
            collectVariables(from: left, into: &vars)
            collectVariables(from: right, into: &vars)
        case .table, .values, .graphTable:
            break
        }
    }

    private func collectVariables(from pattern: GraphPattern, into vars: inout Set<String>) {
        switch pattern {
        case .basic(let triples):
            for triple in triples {
                collectVariables(from: triple, into: &vars)
            }
        case .join(let left, let right), .optional(let left, let right),
             .union(let left, let right), .minus(let left, let right):
            collectVariables(from: left, into: &vars)
            collectVariables(from: right, into: &vars)
        case .filter(let pattern, let expr):
            collectVariables(from: pattern, into: &vars)
            collectVariables(from: expr, into: &vars)
        case .graph(_, let pattern):
            collectVariables(from: pattern, into: &vars)
        case .service(_, let pattern, _):
            collectVariables(from: pattern, into: &vars)
        case .bind(let pattern, let variable, let expr):
            collectVariables(from: pattern, into: &vars)
            vars.insert(variable)
            collectVariables(from: expr, into: &vars)
        case .values(let variables, _):
            vars.formUnion(variables)
        case .subquery(let query):
            vars.formUnion(query.referencedVariables)
        case .groupBy(let pattern, _, _):
            collectVariables(from: pattern, into: &vars)
        case .propertyPath(let subject, _, let object):
            if case .variable(let v) = subject { vars.insert(v) }
            if case .variable(let v) = object { vars.insert(v) }
        }
    }

    private func collectVariables(from triple: TriplePattern, into vars: inout Set<String>) {
        if case .variable(let v) = triple.subject { vars.insert(v) }
        if case .variable(let v) = triple.predicate { vars.insert(v) }
        if case .variable(let v) = triple.object { vars.insert(v) }
    }

    private func collectVariables(from expr: Expression, into vars: inout Set<String>) {
        switch expr {
        case .variable(let v):
            vars.insert(v.name)
        case .add(let l, let r), .subtract(let l, let r), .multiply(let l, let r),
             .divide(let l, let r), .modulo(let l, let r), .equal(let l, let r),
             .notEqual(let l, let r), .lessThan(let l, let r), .lessThanOrEqual(let l, let r),
             .greaterThan(let l, let r), .greaterThanOrEqual(let l, let r),
             .and(let l, let r), .or(let l, let r):
            collectVariables(from: l, into: &vars)
            collectVariables(from: r, into: &vars)
        case .negate(let e), .not(let e), .isNull(let e), .isNotNull(let e),
             .isTriple(let e), .subject(let e), .predicate(let e), .object(let e):
            collectVariables(from: e, into: &vars)
        case .bound(let v):
            vars.insert(v.name)
        case .function(let call):
            for arg in call.arguments {
                collectVariables(from: arg, into: &vars)
            }
        case .subquery(let query), .exists(let query), .inSubquery(_, let query):
            vars.formUnion(query.referencedVariables)
        case .triple(let s, let p, let o):
            collectVariables(from: s, into: &vars)
            collectVariables(from: p, into: &vars)
            collectVariables(from: o, into: &vars)
        default:
            break
        }
    }

    private func collectColumns(from source: DataSource, into cols: inout Set<ColumnRef>) {
        switch source {
        case .table:
            break  // Table scan doesn't reference specific columns
        case .subquery(let query, _):
            cols.formUnion(query.referencedColumns)
        case .join(let clause):
            collectColumns(from: clause.left, into: &cols)
            collectColumns(from: clause.right, into: &cols)
            if case .on(let expr) = clause.condition {
                collectColumns(from: expr, into: &cols)
            }
        case .union(let sources), .unionAll(let sources), .intersect(let sources):
            for s in sources {
                collectColumns(from: s, into: &cols)
            }
        case .except(let left, let right):
            collectColumns(from: left, into: &cols)
            collectColumns(from: right, into: &cols)
        default:
            break
        }
    }

    private func collectColumns(from expr: Expression, into cols: inout Set<ColumnRef>) {
        switch expr {
        case .column(let col):
            cols.insert(col)
        case .add(let l, let r), .subtract(let l, let r), .multiply(let l, let r),
             .divide(let l, let r), .modulo(let l, let r), .equal(let l, let r),
             .notEqual(let l, let r), .lessThan(let l, let r), .lessThanOrEqual(let l, let r),
             .greaterThan(let l, let r), .greaterThanOrEqual(let l, let r),
             .and(let l, let r), .or(let l, let r):
            collectColumns(from: l, into: &cols)
            collectColumns(from: r, into: &cols)
        case .negate(let e), .not(let e), .isNull(let e), .isNotNull(let e):
            collectColumns(from: e, into: &cols)
        case .function(let call):
            for arg in call.arguments {
                collectColumns(from: arg, into: &cols)
            }
        case .subquery(let query), .exists(let query), .inSubquery(_, let query):
            cols.formUnion(query.referencedColumns)
        default:
            break
        }
    }
}
