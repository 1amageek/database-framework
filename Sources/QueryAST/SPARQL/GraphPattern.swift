/// GraphPattern.swift
/// SPARQL Graph Pattern types
///
/// Reference:
/// - W3C SPARQL 1.1 Query Language (Algebra)
/// - W3C SPARQL 1.2 (Draft)

import Foundation

// Note: Core GraphPattern enum is defined in DataSource.swift
// This file provides additional utilities and extensions.

// MARK: - GraphPattern Builders

extension GraphPattern {
    /// Create a basic graph pattern from triple patterns
    public static func bgp(_ patterns: TriplePattern...) -> GraphPattern {
        .basic(patterns)
    }

    /// Create a basic graph pattern from an array
    public static func bgp(_ patterns: [TriplePattern]) -> GraphPattern {
        .basic(patterns)
    }

    /// Create a FILTER pattern
    public static func filtered(_ pattern: GraphPattern, _ condition: Expression) -> GraphPattern {
        .filter(pattern, condition)
    }

    /// Create an OPTIONAL pattern
    public static func leftJoin(_ left: GraphPattern, _ right: GraphPattern) -> GraphPattern {
        .optional(left, right)
    }

    /// Create a UNION of multiple patterns
    public static func unionAll(_ patterns: GraphPattern...) -> GraphPattern {
        guard !patterns.isEmpty else { return .basic([]) }
        return patterns.dropFirst().reduce(patterns[0]) { .union($0, $1) }
    }

    /// Create a UNION of two patterns
    public static func unionTwo(_ left: GraphPattern, _ right: GraphPattern) -> GraphPattern {
        .union(left, right)
    }

    /// Create a MINUS pattern
    public static func difference(_ left: GraphPattern, _ right: GraphPattern) -> GraphPattern {
        .minus(left, right)
    }

    /// Create a BIND pattern
    public static func binding(_ pattern: GraphPattern, _ variable: String, _ expression: Expression) -> GraphPattern {
        .bind(pattern, variable: variable, expression: expression)
    }

    /// Create a VALUES pattern (inline data)
    public static func inlineData(_ variables: [String], _ data: [[Literal?]]) -> GraphPattern {
        .values(variables: variables, bindings: data)
    }

    /// Create a SERVICE pattern (federation)
    public static func federated(_ endpoint: String, _ pattern: GraphPattern, silent: Bool = false) -> GraphPattern {
        .service(endpoint: endpoint, pattern: pattern, silent: silent)
    }

    /// Create a GRAPH pattern (named graph)
    public static func named(_ graphName: SPARQLTerm, _ pattern: GraphPattern) -> GraphPattern {
        .graph(name: graphName, pattern: pattern)
    }

    /// Create a property path pattern
    public static func path(
        subject: SPARQLTerm,
        path: PropertyPath,
        object: SPARQLTerm
    ) -> GraphPattern {
        .propertyPath(subject: subject, path: path, object: object)
    }
}

// MARK: - GraphPattern Analysis

extension GraphPattern {
    /// Returns all variables in scope for this pattern
    public var variables: Set<String> {
        var vars = Set<String>()
        collectVariables(into: &vars)
        return vars
    }

    private func collectVariables(into vars: inout Set<String>) {
        switch self {
        case .basic(let triples):
            for triple in triples {
                vars.formUnion(triple.variables)
            }

        case .join(let left, let right):
            left.collectVariables(into: &vars)
            right.collectVariables(into: &vars)

        case .optional(let left, let right):
            left.collectVariables(into: &vars)
            right.collectVariables(into: &vars)

        case .union(let left, let right):
            left.collectVariables(into: &vars)
            right.collectVariables(into: &vars)

        case .filter(let pattern, _):
            pattern.collectVariables(into: &vars)

        case .minus(let left, _):
            // MINUS does not project variables from the right
            left.collectVariables(into: &vars)

        case .graph(_, let pattern):
            pattern.collectVariables(into: &vars)

        case .service(_, let pattern, _):
            pattern.collectVariables(into: &vars)

        case .bind(let pattern, let variable, _):
            pattern.collectVariables(into: &vars)
            vars.insert(variable)

        case .values(let variables, _):
            vars.formUnion(variables)

        case .subquery(let query):
            // Only projected variables from subquery
            switch query.projection {
            case .items(let items), .distinctItems(let items):
                for item in items {
                    if case .variable(let v) = item.expression {
                        vars.insert(v.name)
                    }
                }
            case .all:
                // SELECT * projects all variables bound in the source pattern
                // Reference: SPARQL 1.1 §18.2.4.1 — SELECT * selects all in-scope variables
                if case .graphPattern(let pattern) = query.source {
                    pattern.collectVariables(into: &vars)
                }
            case .allFrom:
                break
            }

        case .groupBy(let pattern, _, _):
            pattern.collectVariables(into: &vars)

        case .propertyPath(let subject, _, let object):
            if case .variable(let v) = subject { vars.insert(v) }
            if case .variable(let v) = object { vars.insert(v) }
        }
    }

    /// Returns variables that must be bound (required)
    public var requiredVariables: Set<String> {
        switch self {
        case .basic(let triples):
            var vars = Set<String>()
            for triple in triples {
                vars.formUnion(triple.variables)
            }
            return vars

        case .join(let left, let right):
            return left.requiredVariables.union(right.requiredVariables)

        case .optional(let left, _):
            return left.requiredVariables

        case .union(let left, let right):
            // Intersection: variables that are required in both branches
            return left.requiredVariables.intersection(right.requiredVariables)

        case .filter(let pattern, _):
            return pattern.requiredVariables

        case .minus(let left, _):
            return left.requiredVariables

        case .graph(_, let pattern):
            return pattern.requiredVariables

        case .service(_, let pattern, let silent):
            return silent ? Set() : pattern.requiredVariables

        case .bind(let pattern, _, _):
            return pattern.requiredVariables

        case .values(let variables, _):
            return Set(variables)

        case .subquery, .groupBy:
            return Set()

        case .propertyPath(let subject, _, let object):
            var vars = Set<String>()
            if case .variable(let v) = subject { vars.insert(v) }
            if case .variable(let v) = object { vars.insert(v) }
            return vars
        }
    }

    /// Returns the number of triple patterns
    public var tripleCount: Int {
        switch self {
        case .basic(let triples):
            return triples.count
        case .join(let left, let right), .optional(let left, let right),
             .union(let left, let right), .minus(let left, let right):
            return left.tripleCount + right.tripleCount
        case .filter(let pattern, _), .bind(let pattern, _, _),
             .graph(_, let pattern), .service(_, let pattern, _):
            return pattern.tripleCount
        case .values, .subquery:
            return 0
        case .groupBy(let pattern, _, _):
            return pattern.tripleCount
        case .propertyPath:
            return 1  // Equivalent to one or more triples
        }
    }

    /// Complexity estimate for query optimization
    public var complexity: Int {
        switch self {
        case .basic(let triples):
            return triples.count

        case .join(let left, let right):
            return left.complexity * right.complexity

        case .optional(let left, let right):
            return left.complexity + right.complexity

        case .union(let left, let right):
            return left.complexity + right.complexity

        case .filter(let pattern, _):
            return pattern.complexity

        case .minus(let left, let right):
            return left.complexity + right.complexity

        case .graph(_, let pattern):
            return pattern.complexity

        case .service(_, let pattern, _):
            return pattern.complexity * 10  // Network overhead

        case .bind(let pattern, _, _):
            return pattern.complexity

        case .values(_, let bindings):
            return bindings.count

        case .subquery(_):
            // Simplified: just count source complexity
            return 10

        case .groupBy(let pattern, _, _):
            return pattern.complexity * 2

        case .propertyPath(_, let path, _):
            return path.complexity
        }
    }
}

// MARK: - GraphPattern Transformations

extension GraphPattern {
    /// Flatten nested joins into a single basic pattern where possible
    public func flattened() -> GraphPattern {
        switch self {
        case .join(let left, let right):
            let leftFlat = left.flattened()
            let rightFlat = right.flattened()

            // If both are basic patterns, merge them
            if case .basic(let leftTriples) = leftFlat,
               case .basic(let rightTriples) = rightFlat {
                return .basic(leftTriples + rightTriples)
            }

            return .join(leftFlat, rightFlat)

        case .filter(let pattern, let condition):
            return .filter(pattern.flattened(), condition)

        case .optional(let left, let right):
            return .optional(left.flattened(), right.flattened())

        case .union(let left, let right):
            return .union(left.flattened(), right.flattened())

        case .minus(let left, let right):
            return .minus(left.flattened(), right.flattened())

        case .graph(let name, let pattern):
            return .graph(name: name, pattern: pattern.flattened())

        case .service(let endpoint, let pattern, let silent):
            return .service(endpoint: endpoint, pattern: pattern.flattened(), silent: silent)

        case .bind(let pattern, let variable, let expression):
            return .bind(pattern.flattened(), variable: variable, expression: expression)

        case .groupBy(let pattern, let expressions, let aggregates):
            return .groupBy(pattern.flattened(), expressions: expressions, aggregates: aggregates)

        default:
            return self
        }
    }

    /// Push filters down to be closer to their relevant patterns
    public func optimized() -> GraphPattern {
        // This is a simplified version - full optimization is in QueryOptimizer
        flattened()
    }
}

// MARK: - SPARQL Serialization

extension GraphPattern {
    /// Generate SPARQL syntax
    public func toSPARQL(prefixes: [String: String] = [:], indent: String = "") -> String {
        switch self {
        case .basic(let triples):
            return triples.map { indent + $0.toSPARQL(prefixes: prefixes) }.joined(separator: "\n")

        case .join(let left, let right):
            return """
            \(left.toSPARQL(prefixes: prefixes, indent: indent))
            \(right.toSPARQL(prefixes: prefixes, indent: indent))
            """

        case .optional(let left, let right):
            return """
            \(left.toSPARQL(prefixes: prefixes, indent: indent))
            \(indent)OPTIONAL {
            \(right.toSPARQL(prefixes: prefixes, indent: indent + "  "))
            \(indent)}
            """

        case .union(let left, let right):
            return """
            \(indent){
            \(left.toSPARQL(prefixes: prefixes, indent: indent + "  "))
            \(indent)}
            \(indent)UNION
            \(indent){
            \(right.toSPARQL(prefixes: prefixes, indent: indent + "  "))
            \(indent)}
            """

        case .filter(let pattern, let condition):
            return """
            \(pattern.toSPARQL(prefixes: prefixes, indent: indent))
            \(indent)FILTER(\(condition.toSPARQL()))
            """

        case .minus(let left, let right):
            return """
            \(left.toSPARQL(prefixes: prefixes, indent: indent))
            \(indent)MINUS {
            \(right.toSPARQL(prefixes: prefixes, indent: indent + "  "))
            \(indent)}
            """

        case .graph(let name, let pattern):
            return """
            \(indent)GRAPH \(name.toSPARQL(prefixes: prefixes)) {
            \(pattern.toSPARQL(prefixes: prefixes, indent: indent + "  "))
            \(indent)}
            """

        case .service(let endpoint, let pattern, let silent):
            let silentStr = silent ? "SILENT " : ""
            return """
            \(indent)SERVICE \(silentStr)<\(endpoint)> {
            \(pattern.toSPARQL(prefixes: prefixes, indent: indent + "  "))
            \(indent)}
            """

        case .bind(let pattern, let variable, let expression):
            return """
            \(pattern.toSPARQL(prefixes: prefixes, indent: indent))
            \(indent)BIND(\(expression.toSPARQL()) AS ?\(variable))
            """

        case .values(let variables, let bindings):
            let varsStr = variables.map { "?\($0)" }.joined(separator: " ")
            var rows: [String] = []
            for binding in bindings {
                let vals = binding.map { $0?.toSPARQL() ?? "UNDEF" }.joined(separator: " ")
                rows.append("(\(vals))")
            }
            return """
            \(indent)VALUES (\(varsStr)) {
            \(rows.map { indent + "  " + $0 }.joined(separator: "\n"))
            \(indent)}
            """

        case .subquery(let query):
            return """
            \(indent){
            \(indent)  \(query.toSPARQL(prefixes: prefixes))
            \(indent)}
            """

        case .groupBy(let pattern, let expressions, _):
            let groupByStr = expressions.isEmpty ? "" :
                "\n\(indent)GROUP BY " + expressions.map { $0.toSPARQL() }.joined(separator: " ")
            return """
            \(pattern.toSPARQL(prefixes: prefixes, indent: indent))\(groupByStr)
            """

        case .propertyPath(let subject, let path, let object):
            return "\(indent)\(subject.toSPARQL(prefixes: prefixes)) \(path.toSPARQL(prefixes: prefixes)) \(object.toSPARQL(prefixes: prefixes)) ."
        }
    }
}

extension Expression {
    /// Generate SPARQL expression syntax
    /// Independent implementation for proper SPARQL semantics
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        switch self {
        case .literal(let lit):
            return lit.toSPARQL()

        case .variable(let v):
            return "?\(v.name)"

        case .column(let col):
            // In SPARQL context, columns are treated as variables
            return "?\(col.column)"

        // Comparison operations
        case .equal(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) = \(r.toSPARQL(prefixes: prefixes)))"

        case .notEqual(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) != \(r.toSPARQL(prefixes: prefixes)))"

        case .lessThan(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) < \(r.toSPARQL(prefixes: prefixes)))"

        case .lessThanOrEqual(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) <= \(r.toSPARQL(prefixes: prefixes)))"

        case .greaterThan(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) > \(r.toSPARQL(prefixes: prefixes)))"

        case .greaterThanOrEqual(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) >= \(r.toSPARQL(prefixes: prefixes)))"

        // Logical operations
        case .and(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) && \(r.toSPARQL(prefixes: prefixes)))"

        case .or(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) || \(r.toSPARQL(prefixes: prefixes)))"

        case .not(let e):
            return "!(\(e.toSPARQL(prefixes: prefixes)))"

        // Arithmetic operations
        case .add(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) + \(r.toSPARQL(prefixes: prefixes)))"

        case .subtract(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) - \(r.toSPARQL(prefixes: prefixes)))"

        case .multiply(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) * \(r.toSPARQL(prefixes: prefixes)))"

        case .divide(let l, let r):
            return "(\(l.toSPARQL(prefixes: prefixes)) / \(r.toSPARQL(prefixes: prefixes)))"

        case .modulo(let l, let r):
            // SPARQL doesn't have modulo, use floor division
            return "((\(l.toSPARQL(prefixes: prefixes))) - (FLOOR((\(l.toSPARQL(prefixes: prefixes))) / (\(r.toSPARQL(prefixes: prefixes)))) * (\(r.toSPARQL(prefixes: prefixes)))))"

        case .negate(let e):
            return "-(\(e.toSPARQL(prefixes: prefixes)))"

        // NULL / BOUND checks
        case .isNull(let e):
            return "(!BOUND(\(e.toSPARQL(prefixes: prefixes))))"

        case .isNotNull(let e):
            return "BOUND(\(e.toSPARQL(prefixes: prefixes)))"

        case .bound(let v):
            return "BOUND(?\(v.name))"

        // Pattern matching
        case .like(let e, let pattern):
            // Convert SQL LIKE to REGEX
            let regexPattern = pattern
                .replacingOccurrences(of: "%", with: ".*")
                .replacingOccurrences(of: "_", with: ".")
            return "REGEX(\(e.toSPARQL(prefixes: prefixes)), \"^\(regexPattern)$\", \"i\")"

        case .regex(let text, let pattern, let flags):
            if let f = flags {
                return "REGEX(\(text.toSPARQL(prefixes: prefixes)), \"\(pattern)\", \"\(f)\")"
            }
            return "REGEX(\(text.toSPARQL(prefixes: prefixes)), \"\(pattern)\")"

        // Range operations
        case .between(let e, let low, let high):
            return "((\(e.toSPARQL(prefixes: prefixes)) >= \(low.toSPARQL(prefixes: prefixes))) && (\(e.toSPARQL(prefixes: prefixes)) <= \(high.toSPARQL(prefixes: prefixes))))"

        case .inList(let e, let values):
            let vals = values.map { $0.toSPARQL(prefixes: prefixes) }.joined(separator: ", ")
            return "(\(e.toSPARQL(prefixes: prefixes)) IN (\(vals)))"

        case .inSubquery(let e, let subquery):
            return "(\(e.toSPARQL(prefixes: prefixes)) IN { \(subquery.toSPARQL(prefixes: prefixes)) })"

        // Aggregates
        case .aggregate(let agg):
            return agg.toSPARQL(prefixes: prefixes)

        // Functions
        case .function(let call):
            let args = call.arguments.map { $0.toSPARQL(prefixes: prefixes) }.joined(separator: ", ")
            return "\(call.name.uppercased())(\(args))"

        // Conditional
        case .caseWhen(let cases, let elseResult):
            // SPARQL uses IF for conditionals
            if cases.count == 1, let first = cases.first {
                let elseStr = elseResult?.toSPARQL(prefixes: prefixes) ?? "UNDEF"
                return "IF(\(first.condition.toSPARQL(prefixes: prefixes)), \(first.result.toSPARQL(prefixes: prefixes)), \(elseStr))"
            }
            // Nested IF for multiple cases
            var result = elseResult?.toSPARQL(prefixes: prefixes) ?? "UNDEF"
            for (condition, res) in cases.reversed() {
                result = "IF(\(condition.toSPARQL(prefixes: prefixes)), \(res.toSPARQL(prefixes: prefixes)), \(result))"
            }
            return result

        case .coalesce(let exprs):
            let args = exprs.map { $0.toSPARQL(prefixes: prefixes) }.joined(separator: ", ")
            return "COALESCE(\(args))"

        case .nullIf(let e1, let e2):
            return "IF(\(e1.toSPARQL(prefixes: prefixes)) = \(e2.toSPARQL(prefixes: prefixes)), UNDEF, \(e1.toSPARQL(prefixes: prefixes)))"

        // Type conversion
        case .cast(let e, let targetType):
            return "\(targetType.sparqlFunction)(\(e.toSPARQL(prefixes: prefixes)))"

        // RDF-star operations
        case .triple(let subject, let predicate, let object):
            return "<< \(subject.toSPARQL(prefixes: prefixes)) \(predicate.toSPARQL(prefixes: prefixes)) \(object.toSPARQL(prefixes: prefixes)) >>"

        case .isTriple(let e):
            return "isTRIPLE(\(e.toSPARQL(prefixes: prefixes)))"

        case .subject(let e):
            return "SUBJECT(\(e.toSPARQL(prefixes: prefixes)))"

        case .predicate(let e):
            return "PREDICATE(\(e.toSPARQL(prefixes: prefixes)))"

        case .object(let e):
            return "OBJECT(\(e.toSPARQL(prefixes: prefixes)))"

        // Subqueries
        case .subquery(let query):
            return "{ \(query.toSPARQL(prefixes: prefixes)) }"

        case .exists(let query):
            return "EXISTS { \(query.toSPARQL(prefixes: prefixes)) }"
        }
    }
}

extension AggregateFunction {
    /// Generate SPARQL aggregate syntax
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        switch self {
        case .count(let expr, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            if let e = expr {
                return "COUNT(\(distinctStr)\(e.toSPARQL(prefixes: prefixes)))"
            }
            return "COUNT(\(distinctStr)*)"

        case .sum(let expr, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            return "SUM(\(distinctStr)\(expr.toSPARQL(prefixes: prefixes)))"

        case .avg(let expr, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            return "AVG(\(distinctStr)\(expr.toSPARQL(prefixes: prefixes)))"

        case .min(let expr):
            return "MIN(\(expr.toSPARQL(prefixes: prefixes)))"

        case .max(let expr):
            return "MAX(\(expr.toSPARQL(prefixes: prefixes)))"

        case .groupConcat(let expr, let separator, let distinct):
            let distinctStr = distinct ? "DISTINCT " : ""
            if let sep = separator {
                return "GROUP_CONCAT(\(distinctStr)\(expr.toSPARQL(prefixes: prefixes)); SEPARATOR=\"\(sep)\")"
            }
            return "GROUP_CONCAT(\(distinctStr)\(expr.toSPARQL(prefixes: prefixes)))"

        case .sample(let expr):
            return "SAMPLE(\(expr.toSPARQL(prefixes: prefixes)))"

        case .arrayAgg(let expr, _, let distinct):
            // SPARQL doesn't have ARRAY_AGG, use GROUP_CONCAT as approximation
            let distinctStr = distinct ? "DISTINCT " : ""
            return "GROUP_CONCAT(\(distinctStr)\(expr.toSPARQL(prefixes: prefixes)))"
        }
    }
}

extension DataType {
    /// Get the SPARQL function name for this data type
    public var sparqlFunction: String {
        switch self {
        case .boolean:
            return "xsd:boolean"
        case .smallint, .integer, .bigint:
            return "xsd:integer"
        case .real, .doublePrecision:
            return "xsd:double"
        case .decimal:
            return "xsd:decimal"
        case .char, .varchar, .text:
            return "xsd:string"
        case .date:
            return "xsd:date"
        case .time:
            return "xsd:time"
        case .timestamp:
            return "xsd:dateTime"
        case .binary, .varbinary, .blob:
            return "xsd:base64Binary"
        case .uuid:
            return "xsd:string"
        case .json, .jsonb:
            return "xsd:string"
        case .interval:
            return "xsd:duration"
        case .array:
            return "xsd:string"
        case .custom(let name):
            return name
        }
    }
}

extension SelectQuery {
    /// Generate SPARQL SELECT query syntax
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        var result = ""

        // Prefixes
        for (prefix, iri) in prefixes {
            result += "PREFIX \(prefix): <\(iri)>\n"
        }

        // SELECT clause
        result += "SELECT "
        if distinct { result += "DISTINCT " }
        if reduced { result += "REDUCED " }

        switch projection {
        case .all:
            result += "*"
        case .allFrom(let table):
            result += "\(table).*"
        case .items(let items), .distinctItems(let items):
            result += items.map { item in
                var s = item.expression.toSPARQL()
                if let alias = item.alias {
                    s = "(\(s) AS ?\(alias))"
                }
                return s
            }.joined(separator: " ")
        }

        result += "\n"

        // WHERE clause
        if case .graphPattern(let pattern) = source {
            result += "WHERE {\n"
            result += pattern.toSPARQL(prefixes: prefixes, indent: "  ")
            result += "\n}\n"
        }

        // GROUP BY
        if let groupBy = groupBy, !groupBy.isEmpty {
            result += "GROUP BY " + groupBy.map { $0.toSPARQL() }.joined(separator: " ") + "\n"
        }

        // HAVING
        if let having = having {
            result += "HAVING(\(having.toSPARQL()))\n"
        }

        // ORDER BY
        if let orderBy = orderBy, !orderBy.isEmpty {
            result += "ORDER BY " + orderBy.map { key in
                let dir = key.direction == .descending ? "DESC" : "ASC"
                return "\(dir)(\(key.expression.toSPARQL()))"
            }.joined(separator: " ") + "\n"
        }

        // LIMIT
        if let limit = limit {
            result += "LIMIT \(limit)\n"
        }

        // OFFSET
        if let offset = offset {
            result += "OFFSET \(offset)\n"
        }

        return result
    }
}

