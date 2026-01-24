/// MatchPattern.swift
/// SQL/PGQ MATCH pattern types
///
/// Reference:
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)
/// - GQL (Graph Query Language) specification

import Foundation

// Note: Core MatchPattern, PathPattern, PathElement, NodePattern, EdgePattern,
// EdgeDirection, PathQuantifier, and PathMode types are defined in DataSource.swift
// This file provides additional utilities and builder helpers for SQL/PGQ patterns.

// MARK: - MatchPattern Builders

extension MatchPattern {
    /// Create a simple path match
    public static func path(_ elements: PathElement...) -> MatchPattern {
        MatchPattern(paths: [PathPattern(elements: elements)])
    }

    /// Create a path match with a path variable
    public static func path(_ variable: String, _ elements: PathElement...) -> MatchPattern {
        MatchPattern(paths: [PathPattern(pathVariable: variable, elements: elements)])
    }

    /// Create a path match with mode
    public static func path(mode: PathMode, _ elements: PathElement...) -> MatchPattern {
        MatchPattern(paths: [PathPattern(elements: elements, mode: mode)])
    }

    /// Create a match with WHERE clause
    public func `where`(_ condition: Expression) -> MatchPattern {
        MatchPattern(paths: self.paths, where: condition)
    }
}

// MARK: - PathPattern Builders

extension PathPattern {
    /// Create a path from node-edge-node sequence
    public static func simple(
        from source: NodePattern,
        via edge: EdgePattern,
        to target: NodePattern
    ) -> PathPattern {
        PathPattern(elements: [.node(source), .edge(edge), .node(target)])
    }

    /// Create a variable-length path
    public static func variable(
        from source: NodePattern,
        via edge: EdgePattern,
        quantifier: PathQuantifier,
        to target: NodePattern
    ) -> PathPattern {
        let innerPath = PathPattern(elements: [.edge(edge)])
        return PathPattern(elements: [
            .node(source),
            .quantified(innerPath, quantifier: quantifier),
            .node(target)
        ])
    }
}

// MARK: - PathElement Helpers

extension PathElement {
    /// Create a node element
    public static func n(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .node(NodePattern(variable: variable, labels: labels, properties: properties))
    }

    /// Create a node with a single label
    public static func n(_ variable: String? = nil, label: String) -> PathElement {
        .node(NodePattern(variable: variable, labels: [label]))
    }

    /// Create an outgoing edge element
    public static func outgoing(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .outgoing))
    }

    /// Create an outgoing edge with a single label
    public static func outgoing(_ variable: String? = nil, label: String) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: [label], direction: .outgoing))
    }

    /// Create an incoming edge element
    public static func incoming(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .incoming))
    }

    /// Create an incoming edge with a single label
    public static func incoming(_ variable: String? = nil, label: String) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: [label], direction: .incoming))
    }

    /// Create an undirected edge element
    public static func undirected(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .undirected))
    }

    /// Create an any-direction edge element
    public static func anyDirection(
        _ variable: String? = nil,
        labels: [String]? = nil,
        properties: [(String, Expression)]? = nil
    ) -> PathElement {
        .edge(EdgePattern(variable: variable, labels: labels, properties: properties, direction: .any))
    }
}

// MARK: - PathQuantifier Helpers

extension PathQuantifier {
    /// Create a bounded range quantifier: {min, max}
    public static func bounded(_ min: Int, _ max: Int) -> PathQuantifier {
        .range(min: min, max: max)
    }

    /// Create a minimum-only quantifier: {min,}
    public static func atLeast(_ min: Int) -> PathQuantifier {
        .range(min: min, max: nil)
    }

    /// Create a maximum-only quantifier: {,max}
    public static func atMost(_ max: Int) -> PathQuantifier {
        .range(min: nil, max: max)
    }
}

// MARK: - Pattern Validation

extension MatchPattern {
    /// Validate that the pattern is well-formed
    public func validate() -> [PatternValidationError] {
        var errors: [PatternValidationError] = []

        for (index, path) in paths.enumerated() {
            errors.append(contentsOf: path.validate(pathIndex: index))
        }

        return errors
    }
}

extension PathPattern {
    /// Validate that the path pattern is well-formed
    public func validate(pathIndex: Int) -> [PatternValidationError] {
        var errors: [PatternValidationError] = []

        // Check that path alternates between nodes and edges
        var expectNode = true
        for (elemIndex, element) in elements.enumerated() {
            switch element {
            case .node:
                if !expectNode {
                    errors.append(.unexpectedElement(
                        path: pathIndex,
                        element: elemIndex,
                        expected: "edge",
                        found: "node"
                    ))
                }
                expectNode = false

            case .edge:
                if expectNode {
                    errors.append(.unexpectedElement(
                        path: pathIndex,
                        element: elemIndex,
                        expected: "node",
                        found: "edge"
                    ))
                }
                expectNode = true

            case .quantified(let innerPath, _):
                // Quantified patterns should be validated recursively
                errors.append(contentsOf: innerPath.validate(pathIndex: pathIndex))
                // Determine what comes after based on inner pattern's last element
                if let lastElement = innerPath.elements.last {
                    switch lastElement {
                    case .node:
                        // Inner ends with node, next should be edge
                        expectNode = false
                    case .edge:
                        // Inner ends with edge, next should be node
                        expectNode = true
                    case .quantified, .alternation:
                        // For nested patterns, assume edge-like (expect node next)
                        expectNode = true
                    }
                }
                // If empty inner pattern, don't change expectNode

            case .alternation(let alternatives):
                // All alternatives should be valid
                for alt in alternatives {
                    errors.append(contentsOf: alt.validate(pathIndex: pathIndex))
                }
                // Check first alternative to determine what comes after
                // All alternatives should have the same structure
                if let firstAlt = alternatives.first, let lastElement = firstAlt.elements.last {
                    switch lastElement {
                    case .node:
                        expectNode = false
                    case .edge:
                        expectNode = true
                    case .quantified, .alternation:
                        expectNode = true
                    }
                }
            }
        }

        // Path should start and end with nodes (or be empty)
        if !elements.isEmpty {
            if case .edge = elements.first {
                errors.append(.pathMustStartWithNode(path: pathIndex))
            }
            if case .edge = elements.last {
                errors.append(.pathMustEndWithNode(path: pathIndex))
            }
        }

        return errors
    }
}

/// Pattern validation errors
public enum PatternValidationError: Error, Sendable, Equatable {
    case unexpectedElement(path: Int, element: Int, expected: String, found: String)
    case pathMustStartWithNode(path: Int)
    case pathMustEndWithNode(path: Int)
    case invalidQuantifier(path: Int, message: String)
    case duplicateVariable(name: String)
}

// MARK: - Variable Collection

extension MatchPattern {
    /// Collect all pattern variables
    public var variables: Set<String> {
        var vars = Set<String>()
        for path in paths {
            if let pathVar = path.pathVariable {
                vars.insert(pathVar)
            }
            for element in path.elements {
                collectVariables(from: element, into: &vars)
            }
        }
        return vars
    }

    private func collectVariables(from element: PathElement, into vars: inout Set<String>) {
        switch element {
        case .node(let node):
            if let v = node.variable { vars.insert(v) }
        case .edge(let edge):
            if let v = edge.variable { vars.insert(v) }
        case .quantified(let path, _):
            for elem in path.elements {
                collectVariables(from: elem, into: &vars)
            }
        case .alternation(let alts):
            for alt in alts {
                for elem in alt.elements {
                    collectVariables(from: elem, into: &vars)
                }
            }
        }
    }
}

// MARK: - Pattern Serialization (SQL/PGQ syntax)

extension MatchPattern {
    /// Generate SQL/PGQ MATCH clause syntax
    public func toSQL() -> String {
        var result = "MATCH "
        result += paths.map { $0.toSQL() }.joined(separator: ", ")
        if let whereClause = `where` {
            result += " WHERE \(whereClause.toSQL())"
        }
        return result
    }
}

extension PathPattern {
    /// Generate SQL/PGQ path pattern syntax
    public func toSQL() -> String {
        var result = ""
        if let pathVar = pathVariable {
            result += "\(pathVar) = "
        }
        if let mode = mode {
            result += "\(mode.toSQL()) "
        }
        result += elements.map { $0.toSQL() }.joined()
        return result
    }
}

extension PathElement {
    /// Generate SQL/PGQ path element syntax
    public func toSQL() -> String {
        switch self {
        case .node(let node):
            return node.toSQL()
        case .edge(let edge):
            return edge.toSQL()
        case .quantified(let path, let quant):
            return "(\(path.toSQL()))\(quant.toSQL())"
        case .alternation(let alts):
            return "(" + alts.map { $0.toSQL() }.joined(separator: "|") + ")"
        }
    }
}

extension NodePattern {
    /// Generate SQL/PGQ node pattern syntax
    public func toSQL() -> String {
        var result = "("
        if let v = variable { result += v }
        if let labels = labels, !labels.isEmpty {
            result += ":" + labels.joined(separator: ":")
        }
        if let props = properties, !props.isEmpty {
            result += " {" + props.map { "\($0.0): \($0.1.toSQL())" }.joined(separator: ", ") + "}"
        }
        result += ")"
        return result
    }
}

extension EdgePattern {
    /// Generate SQL/PGQ edge pattern syntax
    public func toSQL() -> String {
        var inner = ""
        if let v = variable { inner += v }
        if let labels = labels, !labels.isEmpty {
            inner += ":" + labels.joined(separator: ":")
        }
        if let props = properties, !props.isEmpty {
            inner += " {" + props.map { "\($0.0): \($0.1.toSQL())" }.joined(separator: ", ") + "}"
        }

        switch direction {
        case .outgoing:
            return "-[\(inner)]->"
        case .incoming:
            return "<-[\(inner)]-"
        case .undirected:
            return "-[\(inner)]-"
        case .any:
            return "<-[\(inner)]->"
        }
    }
}

extension PathQuantifier {
    /// Generate SQL/PGQ quantifier syntax
    public func toSQL() -> String {
        switch self {
        case .exactly(let n):
            return "{\(n)}"
        case .range(let min, let max):
            let minStr = min.map(String.init) ?? ""
            let maxStr = max.map(String.init) ?? ""
            return "{\(minStr),\(maxStr)}"
        case .zeroOrMore:
            return "*"
        case .oneOrMore:
            return "+"
        case .zeroOrOne:
            return "?"
        }
    }
}

extension PathMode {
    /// Generate SQL/PGQ path mode syntax
    public func toSQL() -> String {
        switch self {
        case .walk:
            return "WALK"
        case .trail:
            return "TRAIL"
        case .acyclic:
            return "ACYCLIC"
        case .simple:
            return "SIMPLE"
        case .anyShortest:
            return "ANY SHORTEST"
        case .allShortest:
            return "ALL SHORTEST"
        case .shortestK(let k):
            return "SHORTEST \(k)"
        }
    }
}

// MARK: - Expression SQL Generation

extension Expression {
    /// Generate SQL expression syntax
    /// Complete implementation covering all expression cases
    public func toSQL() -> String {
        switch self {
        // Literals and references
        case .literal(let lit):
            return lit.toSQL()

        case .column(let col):
            return col.description

        case .variable(let v):
            // In SQL context, treat variable as parameter placeholder
            return ":\(v.name)"

        // Comparison operations
        case .equal(let l, let r):
            return "(\(l.toSQL()) = \(r.toSQL()))"

        case .notEqual(let l, let r):
            return "(\(l.toSQL()) <> \(r.toSQL()))"

        case .lessThan(let l, let r):
            return "(\(l.toSQL()) < \(r.toSQL()))"

        case .lessThanOrEqual(let l, let r):
            return "(\(l.toSQL()) <= \(r.toSQL()))"

        case .greaterThan(let l, let r):
            return "(\(l.toSQL()) > \(r.toSQL()))"

        case .greaterThanOrEqual(let l, let r):
            return "(\(l.toSQL()) >= \(r.toSQL()))"

        // Logical operations
        case .and(let l, let r):
            return "(\(l.toSQL()) AND \(r.toSQL()))"

        case .or(let l, let r):
            return "(\(l.toSQL()) OR \(r.toSQL()))"

        case .not(let e):
            return "NOT (\(e.toSQL()))"

        // Arithmetic operations
        case .add(let l, let r):
            return "(\(l.toSQL()) + \(r.toSQL()))"

        case .subtract(let l, let r):
            return "(\(l.toSQL()) - \(r.toSQL()))"

        case .multiply(let l, let r):
            return "(\(l.toSQL()) * \(r.toSQL()))"

        case .divide(let l, let r):
            return "(\(l.toSQL()) / \(r.toSQL()))"

        case .modulo(let l, let r):
            return "(\(l.toSQL()) % \(r.toSQL()))"

        case .negate(let e):
            return "-(\(e.toSQL()))"

        // NULL checks
        case .isNull(let e):
            return "(\(e.toSQL()) IS NULL)"

        case .isNotNull(let e):
            return "(\(e.toSQL()) IS NOT NULL)"

        // BOUND (SPARQL-specific, map to IS NOT NULL)
        case .bound(let v):
            return "(:\(v.name) IS NOT NULL)"

        // Pattern matching
        case .like(let e, let pattern):
            return "(\(e.toSQL()) LIKE \(SQLEscape.string(pattern)))"

        case .regex(let text, let pattern, let flags):
            // SQL doesn't have native REGEX, use LIKE approximation or REGEXP
            if let f = flags, f.contains("i") {
                return "(UPPER(\(text.toSQL())) LIKE UPPER(\(SQLEscape.string(pattern))))"
            }
            return "(REGEXP_LIKE(\(text.toSQL()), \(SQLEscape.string(pattern))))"

        // Range operations
        case .between(let e, let low, let high):
            return "(\(e.toSQL()) BETWEEN \(low.toSQL()) AND \(high.toSQL()))"

        case .inList(let e, let values):
            let vals = values.map { $0.toSQL() }.joined(separator: ", ")
            return "(\(e.toSQL()) IN (\(vals)))"

        case .inSubquery(let e, let subquery):
            return "(\(e.toSQL()) IN (\(subquery.toSQL())))"

        // Aggregates
        case .aggregate(let agg):
            return agg.toSQL()

        // Functions
        case .function(let call):
            let args = call.arguments.map { $0.toSQL() }.joined(separator: ", ")
            return "\(call.name)(\(args))"

        // Conditional
        case .caseWhen(let cases, let elseResult):
            var result = "CASE"
            for (condition, res) in cases {
                result += " WHEN \(condition.toSQL()) THEN \(res.toSQL())"
            }
            if let elseExpr = elseResult {
                result += " ELSE \(elseExpr.toSQL())"
            }
            result += " END"
            return result

        case .coalesce(let exprs):
            let args = exprs.map { $0.toSQL() }.joined(separator: ", ")
            return "COALESCE(\(args))"

        case .nullIf(let e1, let e2):
            return "NULLIF(\(e1.toSQL()), \(e2.toSQL()))"

        // Type conversion
        case .cast(let e, let targetType):
            return "CAST(\(e.toSQL()) AS \(targetType.sqlName))"

        // RDF-star operations (not standard SQL, but included for completeness)
        case .triple(let subject, let predicate, let object):
            // Represent as JSON-like structure in SQL
            return "JSON_OBJECT('s', \(subject.toSQL()), 'p', \(predicate.toSQL()), 'o', \(object.toSQL()))"

        case .isTriple(let e):
            return "(JSON_TYPE(\(e.toSQL())) = 'OBJECT')"

        case .subject(let e):
            return "JSON_VALUE(\(e.toSQL()), '$.s')"

        case .predicate(let e):
            return "JSON_VALUE(\(e.toSQL()), '$.p')"

        case .object(let e):
            return "JSON_VALUE(\(e.toSQL()), '$.o')"

        // Subqueries
        case .subquery(let query):
            return "(\(query.toSQL()))"

        case .exists(let query):
            return "EXISTS (\(query.toSQL()))"
        }
    }
}

extension AggregateFunction {
    /// Generate SQL aggregate syntax
    public func toSQL() -> String {
        switch self {
        case .count(let expr, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            if let e = expr {
                return "COUNT(\(distinctStr)\(e.toSQL()))"
            }
            return "COUNT(\(distinctStr)*)"

        case .sum(let expr, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            return "SUM(\(distinctStr)\(expr.toSQL()))"

        case .avg(let expr, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            return "AVG(\(distinctStr)\(expr.toSQL()))"

        case .min(let expr):
            return "MIN(\(expr.toSQL()))"

        case .max(let expr):
            return "MAX(\(expr.toSQL()))"

        case .groupConcat(let expr, let separator, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            if let sep = separator {
                return "GROUP_CONCAT(\(distinctStr)\(expr.toSQL()) SEPARATOR \(SQLEscape.string(sep)))"
            }
            return "GROUP_CONCAT(\(distinctStr)\(expr.toSQL()))"

        case .sample(let expr):
            // SQL doesn't have SAMPLE, use subquery with LIMIT 1
            return "(SELECT \(expr.toSQL()) LIMIT 1)"

        case .arrayAgg(let expr, let orderBy, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            var sql = "ARRAY_AGG(\(distinctStr)\(expr.toSQL())"
            if let order = orderBy {
                sql += " ORDER BY \(order.map { "\($0.expression.toSQL()) \($0.direction == .descending ? "DESC" : "ASC")" }.joined(separator: ", "))"
            }
            sql += ")"
            return sql
        }
    }
}

extension DataType {
    /// Get the SQL type name for CAST expressions
    public var sqlName: String {
        switch self {
        case .boolean:
            return "BOOLEAN"
        case .smallint:
            return "SMALLINT"
        case .integer:
            return "INTEGER"
        case .bigint:
            return "BIGINT"
        case .real:
            return "REAL"
        case .doublePrecision:
            return "DOUBLE PRECISION"
        case .decimal(let precision, let scale):
            if let p = precision, let s = scale {
                return "DECIMAL(\(p), \(s))"
            } else if let p = precision {
                return "DECIMAL(\(p))"
            }
            return "DECIMAL"
        case .char(let length):
            if let len = length {
                return "CHAR(\(len))"
            }
            return "CHAR"
        case .varchar(let length):
            if let len = length {
                return "VARCHAR(\(len))"
            }
            return "VARCHAR"
        case .text:
            return "TEXT"
        case .date:
            return "DATE"
        case .time(let withTimeZone):
            return withTimeZone ? "TIME WITH TIME ZONE" : "TIME"
        case .timestamp(let withTimeZone):
            return withTimeZone ? "TIMESTAMP WITH TIME ZONE" : "TIMESTAMP"
        case .binary(let length):
            if let len = length {
                return "BINARY(\(len))"
            }
            return "BINARY"
        case .varbinary(let length):
            if let len = length {
                return "VARBINARY(\(len))"
            }
            return "VARBINARY"
        case .blob:
            return "BLOB"
        case .uuid:
            return "UUID"
        case .json:
            return "JSON"
        case .jsonb:
            return "JSONB"
        case .interval:
            return "INTERVAL"
        case .array(let elementType):
            return "\(elementType.sqlName) ARRAY"
        case .custom(let name):
            return name
        }
    }
}

extension SelectQuery {
    /// Generate SQL SELECT query syntax
    public func toSQL() -> String {
        var sql = ""

        // WITH clause (CTEs)
        if let ctes = subqueries, !ctes.isEmpty {
            sql += "WITH "
            sql += ctes.map { cte in
                var cteSQL = SQLEscape.identifier(cte.name)
                if let cols = cte.columns {
                    cteSQL += " (\(cols.map { SQLEscape.identifier($0) }.joined(separator: ", ")))"
                }
                cteSQL += " AS (\(cte.query.toSQL()))"
                return cteSQL
            }.joined(separator: ", ")
            sql += " "
        }

        // SELECT clause
        sql += "SELECT "
        if distinct { sql += "DISTINCT " }
        if reduced { sql += "REDUCED " }

        switch projection {
        case .all:
            sql += "*"
        case .allFrom(let table):
            sql += "\(SQLEscape.identifier(table)).*"
        case .items(let items), .distinctItems(let items):
            sql += items.map { item in
                var s = item.expression.toSQL()
                if let alias = item.alias {
                    s += " AS \(SQLEscape.identifier(alias))"
                }
                return s
            }.joined(separator: ", ")
        }

        // FROM clause
        sql += " FROM \(source.toSQL())"

        // WHERE clause
        if let filter = filter {
            sql += " WHERE \(filter.toSQL())"
        }

        // GROUP BY clause
        if let groupBy = groupBy, !groupBy.isEmpty {
            sql += " GROUP BY \(groupBy.map { $0.toSQL() }.joined(separator: ", "))"
        }

        // HAVING clause
        if let having = having {
            sql += " HAVING \(having.toSQL())"
        }

        // ORDER BY clause
        if let orderBy = orderBy, !orderBy.isEmpty {
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
        if let limit = limit {
            sql += " LIMIT \(limit)"
        }

        // OFFSET clause
        if let offset = offset {
            sql += " OFFSET \(offset)"
        }

        return sql
    }
}

extension DataSource {
    /// Generate SQL data source syntax
    public func toSQL() -> String {
        switch self {
        case .table(let ref):
            return ref.description

        case .subquery(let query, let alias):
            return "(\(query.toSQL())) AS \(SQLEscape.identifier(alias))"

        case .join(let clause):
            let left = clause.left.toSQL()
            let right = clause.right.toSQL()
            var sql = "\(left) \(clause.type.toSQL()) \(right)"
            if let cond = clause.condition {
                switch cond {
                case .on(let expr):
                    sql += " ON \(expr.toSQL())"
                case .using(let cols):
                    sql += " USING (\(cols.map { SQLEscape.identifier($0) }.joined(separator: ", ")))"
                }
            }
            return sql

        case .graphTable(let gtSource):
            return gtSource.toSQL()

        case .graphPattern(let pattern):
            // SPARQL-specific, not standard SQL
            return "/* GRAPH PATTERN */ \(pattern.toSPARQL())"

        case .namedGraph(let name, let pattern):
            return "/* NAMED GRAPH \(name) */ \(pattern.toSPARQL())"

        case .service(let endpoint, let pattern, let silent):
            let silentStr = silent ? "SILENT " : ""
            return "/* SERVICE \(silentStr)<\(endpoint)> */ \(pattern.toSPARQL())"

        case .values(let rows, let columnNames):
            var sql = "(VALUES "
            sql += rows.map { row in
                "(" + row.map { $0.toSQL() }.joined(separator: ", ") + ")"
            }.joined(separator: ", ")
            sql += ")"
            if let names = columnNames {
                sql += " AS t(\(names.map { SQLEscape.identifier($0) }.joined(separator: ", ")))"
            }
            return sql

        case .union(let sources):
            return "(" + sources.map { $0.toSQL() }.joined(separator: " UNION ") + ")"

        case .unionAll(let sources):
            return "(" + sources.map { $0.toSQL() }.joined(separator: " UNION ALL ") + ")"

        case .intersect(let sources):
            return "(" + sources.map { $0.toSQL() }.joined(separator: " INTERSECT ") + ")"

        case .except(let left, let right):
            return "(\(left.toSQL()) EXCEPT \(right.toSQL()))"
        }
    }
}

extension JoinType {
    /// Generate SQL join type syntax
    public func toSQL() -> String {
        switch self {
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

extension Literal {
    /// Cached ISO8601DateFormatter for date serialization
    /// Note: ISO8601DateFormatter is not Sendable but the formatter is immutable after creation
    nonisolated(unsafe) private static let sqlDateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withFullDate]
        return formatter
    }()

    /// Cached ISO8601DateFormatter for timestamp serialization
    nonisolated(unsafe) private static let sqlTimestampFormatter = ISO8601DateFormatter()

    /// Generate SQL literal syntax
    public func toSQL() -> String {
        switch self {
        case .null:
            return "NULL"
        case .bool(let v):
            return v ? "TRUE" : "FALSE"
        case .int(let v):
            return String(v)
        case .double(let v):
            return String(v)
        case .string(let v):
            return SQLEscape.string(v)
        case .date(let v):
            return "DATE '\(Self.sqlDateFormatter.string(from: v))'"
        case .timestamp(let v):
            return "TIMESTAMP '\(Self.sqlTimestampFormatter.string(from: v))'"
        case .iri(let v):
            return SQLEscape.string(v)
        case .binary(let v):
            return "X'\(v.map { String(format: "%02X", $0) }.joined())'"
        case .blankNode(let v):
            return SQLEscape.string("_:\(v)")
        case .typedLiteral(let value, _):
            return SQLEscape.string(value)
        case .langLiteral(let value, _):
            return SQLEscape.string(value)
        case .array(let values):
            return "ARRAY[\(values.map { $0.toSQL() }.joined(separator: ", "))]"
        }
    }
}
