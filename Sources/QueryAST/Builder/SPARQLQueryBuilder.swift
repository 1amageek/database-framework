/// SPARQLQueryBuilder.swift
/// Fluent SPARQL query builder
///
/// Reference:
/// - W3C SPARQL 1.1 Query Language
/// - W3C SPARQL 1.2 (Draft)

import Foundation

/// SPARQL Query Builder for type-safe query construction
public struct SPARQLQueryBuilder: Sendable {
    private var projection: Projection
    private var pattern: GraphPattern
    private var prefixes: [String: String]
    private var groupByExprs: [Expression]
    private var havingExpr: Expression?
    private var orderByKeys: [SortKey]?
    private var limitCount: Int?
    private var offsetCount: Int?
    private var distinctFlag: Bool
    private var reducedFlag: Bool

    /// Initialize an empty SPARQL query builder
    public init() {
        self.projection = .all
        self.pattern = .basic([])
        self.prefixes = SPARQLTerm.commonPrefixes
        self.groupByExprs = []
        self.havingExpr = nil
        self.orderByKeys = nil
        self.limitCount = nil
        self.offsetCount = nil
        self.distinctFlag = false
        self.reducedFlag = false
    }
}

// MARK: - Prefixes

extension SPARQLQueryBuilder {
    /// Add a prefix declaration
    public func prefix(_ prefix: String, _ iri: String) -> SPARQLQueryBuilder {
        var builder = self
        builder.prefixes[prefix] = iri
        return builder
    }

    /// Add multiple prefix declarations
    public func prefixes(_ newPrefixes: [String: String]) -> SPARQLQueryBuilder {
        var builder = self
        builder.prefixes.merge(newPrefixes) { _, new in new }
        return builder
    }
}

// MARK: - SELECT Clause

extension SPARQLQueryBuilder {
    /// Select all variables
    public func selectAll() -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .all
        return builder
    }

    /// Select specific variables
    public func select(_ variables: String...) -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items(variables.map { v in
            ProjectionItem(.variable(Variable(v)))
        })
        return builder
    }

    /// Select with expressions
    public func select(_ items: ProjectionItem...) -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items(items)
        return builder
    }

    /// Select DISTINCT
    public func selectDistinct(_ variables: String...) -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items(variables.map { v in
            ProjectionItem(.variable(Variable(v)))
        })
        builder.distinctFlag = true
        return builder
    }

    /// Select REDUCED
    public func selectReduced(_ variables: String...) -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items(variables.map { v in
            ProjectionItem(.variable(Variable(v)))
        })
        builder.reducedFlag = true
        return builder
    }

    /// Select with alias: (expression AS ?var)
    public func select(_ expression: Expression, as variable: String) -> SPARQLQueryBuilder {
        var builder = self
        if case .items(var items) = builder.projection {
            items.append(ProjectionItem(expression, alias: variable))
            builder.projection = .items(items)
        } else {
            builder.projection = .items([ProjectionItem(expression, alias: variable)])
        }
        return builder
    }
}

// MARK: - WHERE Clause (Triple Patterns)

extension SPARQLQueryBuilder {
    /// Add triple pattern
    public func `where`(_ triple: TriplePattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = addToPattern(builder.pattern, triple: triple)
        return builder
    }

    /// Add multiple triple patterns
    public func `where`(_ triples: [TriplePattern]) -> SPARQLQueryBuilder {
        var builder = self
        for triple in triples {
            builder.pattern = addToPattern(builder.pattern, triple: triple)
        }
        return builder
    }

    /// Add triple pattern: ?s predicate ?o
    public func `where`(
        subject: String,
        predicate: String,
        object: String
    ) -> SPARQLQueryBuilder {
        let triple = TriplePattern(
            subject: .variable(subject),
            predicate: resolveTerm(predicate),
            object: .variable(object)
        )
        return self.where(triple)
    }

    /// Add triple pattern: ?s predicate literal
    public func `where`(
        subject: String,
        predicate: String,
        literal value: Any
    ) -> SPARQLQueryBuilder {
        guard let lit = Literal(value) else { return self }
        let triple = TriplePattern(
            subject: .variable(subject),
            predicate: resolveTerm(predicate),
            object: .literal(lit)
        )
        return self.where(triple)
    }

    /// Add triple pattern: IRI predicate ?o
    public func `where`(
        iri subject: String,
        predicate: String,
        object: String
    ) -> SPARQLQueryBuilder {
        let triple = TriplePattern(
            subject: .iri(subject),
            predicate: resolveTerm(predicate),
            object: .variable(object)
        )
        return self.where(triple)
    }

    private func addToPattern(_ pattern: GraphPattern, triple: TriplePattern) -> GraphPattern {
        switch pattern {
        case .basic(var triples):
            triples.append(triple)
            return .basic(triples)
        default:
            return .join(pattern, .basic([triple]))
        }
    }

    private func resolveTerm(_ term: String) -> SPARQLTerm {
        // Check if it's a prefixed name
        if let colonIndex = term.firstIndex(of: ":") {
            let prefix = String(term[..<colonIndex])
            let local = String(term[term.index(after: colonIndex)...])
            if prefixes[prefix] != nil {
                return .prefixedName(prefix: prefix, local: local)
            }
        }
        // Check if it's a full IRI
        if term.hasPrefix("<") && term.hasSuffix(">") {
            return .iri(String(term.dropFirst().dropLast()))
        }
        // Assume it's a prefixed name with empty prefix or an IRI
        return .iri(term)
    }
}

// MARK: - OPTIONAL

extension SPARQLQueryBuilder {
    /// Add OPTIONAL pattern
    public func optional(_ pattern: GraphPattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .optional(builder.pattern, pattern)
        return builder
    }

    /// Add OPTIONAL triple
    public func optional(_ triple: TriplePattern) -> SPARQLQueryBuilder {
        optional(.basic([triple]))
    }

    /// Add OPTIONAL with fluent builder
    public func optional(_ build: (SPARQLQueryBuilder) -> SPARQLQueryBuilder) -> SPARQLQueryBuilder {
        let inner = build(SPARQLQueryBuilder())
        return optional(inner.pattern)
    }
}

// MARK: - UNION

extension SPARQLQueryBuilder {
    /// Add UNION pattern
    public func union(_ other: GraphPattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .union(builder.pattern, other)
        return builder
    }

    /// Add UNION with two patterns
    public func union(_ left: GraphPattern, _ right: GraphPattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .join(builder.pattern, .union(left, right))
        return builder
    }
}

// MARK: - FILTER

extension SPARQLQueryBuilder {
    /// Add FILTER condition
    public func filter(_ condition: Expression) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .filter(builder.pattern, condition)
        return builder
    }

    /// Add FILTER: ?var = value
    public func filter(_ variable: String, equals value: Any) -> SPARQLQueryBuilder {
        guard let lit = Literal(value) else { return self }
        return filter(.equal(.variable(Variable(variable)), .literal(lit)))
    }

    /// Add FILTER: ?var > value
    public func filter(_ variable: String, _ op: ComparisonOperator, _ value: Any) -> SPARQLQueryBuilder {
        guard let lit = Literal(value) else { return self }
        let varExpr = Expression.variable(Variable(variable))
        let valExpr = Expression.literal(lit)

        let condition: Expression
        switch op {
        case .equal:
            condition = .equal(varExpr, valExpr)
        case .notEqual:
            condition = .notEqual(varExpr, valExpr)
        case .lessThan:
            condition = .lessThan(varExpr, valExpr)
        case .lessThanOrEqual:
            condition = .lessThanOrEqual(varExpr, valExpr)
        case .greaterThan:
            condition = .greaterThan(varExpr, valExpr)
        case .greaterThanOrEqual:
            condition = .greaterThanOrEqual(varExpr, valExpr)
        case .like:
            if case .string(let pattern) = lit {
                condition = .regex(varExpr, pattern: pattern, flags: nil)
            } else {
                return self
            }
        case .inList:
            return self
        case .notInList:
            return self
        }

        return filter(condition)
    }

    /// Add FILTER BOUND(?var)
    public func filterBound(_ variable: String) -> SPARQLQueryBuilder {
        filter(.bound(Variable(variable)))
    }

    /// Add FILTER !BOUND(?var)
    public func filterNotBound(_ variable: String) -> SPARQLQueryBuilder {
        filter(.not(.bound(Variable(variable))))
    }

    /// Add FILTER REGEX(?var, pattern)
    public func filterRegex(_ variable: String, pattern: String, flags: String? = nil) -> SPARQLQueryBuilder {
        filter(.regex(.variable(Variable(variable)), pattern: pattern, flags: flags))
    }
}

// MARK: - MINUS

extension SPARQLQueryBuilder {
    /// Add MINUS pattern
    public func minus(_ pattern: GraphPattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .minus(builder.pattern, pattern)
        return builder
    }

    /// Add MINUS triple
    public func minus(_ triple: TriplePattern) -> SPARQLQueryBuilder {
        minus(.basic([triple]))
    }
}

// MARK: - BIND

extension SPARQLQueryBuilder {
    /// Add BIND expression
    public func bind(_ expression: Expression, as variable: String) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .bind(builder.pattern, variable: variable, expression: expression)
        return builder
    }
}

// MARK: - VALUES

extension SPARQLQueryBuilder {
    /// Add VALUES clause
    public func values(_ variables: [String], _ data: [[Literal?]]) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .join(builder.pattern, .values(variables: variables, bindings: data))
        return builder
    }
}

// MARK: - Property Paths

extension SPARQLQueryBuilder {
    /// Add property path pattern
    public func propertyPath(
        subject: SPARQLTerm,
        path: PropertyPath,
        object: SPARQLTerm
    ) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .join(builder.pattern, .propertyPath(subject: subject, path: path, object: object))
        return builder
    }

    /// Add property path: ?s path+ ?o
    public func transitivePath(
        subject: String,
        predicate: String,
        object: String
    ) -> SPARQLQueryBuilder {
        propertyPath(
            subject: .variable(subject),
            path: .oneOrMore(.iri(predicate)),
            object: .variable(object)
        )
    }

    /// Add property path: ?s path* ?o
    public func transitiveOrSelf(
        subject: String,
        predicate: String,
        object: String
    ) -> SPARQLQueryBuilder {
        propertyPath(
            subject: .variable(subject),
            path: .zeroOrMore(.iri(predicate)),
            object: .variable(object)
        )
    }
}

// MARK: - Named Graph

extension SPARQLQueryBuilder {
    /// Add GRAPH clause
    public func graph(_ graphName: String, _ pattern: GraphPattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .join(builder.pattern, .graph(name: .iri(graphName), pattern: pattern))
        return builder
    }

    /// Add GRAPH clause with variable
    public func graph(variable: String, _ pattern: GraphPattern) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .join(builder.pattern, .graph(name: .variable(variable), pattern: pattern))
        return builder
    }
}

// MARK: - SERVICE (Federation)

extension SPARQLQueryBuilder {
    /// Add SERVICE clause
    public func service(_ endpoint: String, _ pattern: GraphPattern, silent: Bool = false) -> SPARQLQueryBuilder {
        var builder = self
        builder.pattern = .join(builder.pattern, .service(endpoint: endpoint, pattern: pattern, silent: silent))
        return builder
    }
}

// MARK: - GROUP BY

extension SPARQLQueryBuilder {
    /// Group by variables
    public func groupBy(_ variables: String...) -> SPARQLQueryBuilder {
        var builder = self
        builder.groupByExprs = variables.map { .variable(Variable($0)) }
        return builder
    }

    /// Group by expressions
    public func groupBy(_ expressions: Expression...) -> SPARQLQueryBuilder {
        var builder = self
        builder.groupByExprs = expressions
        return builder
    }

    /// Add HAVING condition
    public func having(_ condition: Expression) -> SPARQLQueryBuilder {
        var builder = self
        builder.havingExpr = condition
        return builder
    }
}

// MARK: - ORDER BY

extension SPARQLQueryBuilder {
    /// Order by variable (ascending)
    public func orderBy(_ variable: String, _ direction: SortDirection = .ascending) -> SPARQLQueryBuilder {
        var builder = self
        let key = SortKey(.variable(Variable(variable)), direction: direction)
        if var existing = builder.orderByKeys {
            existing.append(key)
            builder.orderByKeys = existing
        } else {
            builder.orderByKeys = [key]
        }
        return builder
    }

    /// Order by expression
    public func orderBy(_ expression: Expression, _ direction: SortDirection = .ascending) -> SPARQLQueryBuilder {
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

    /// Order by variable descending
    public func orderByDesc(_ variable: String) -> SPARQLQueryBuilder {
        orderBy(variable, .descending)
    }
}

// MARK: - LIMIT/OFFSET

extension SPARQLQueryBuilder {
    /// Set LIMIT
    public func limit(_ count: Int) -> SPARQLQueryBuilder {
        var builder = self
        builder.limitCount = count
        return builder
    }

    /// Set OFFSET
    public func offset(_ count: Int) -> SPARQLQueryBuilder {
        var builder = self
        builder.offsetCount = count
        return builder
    }
}

// MARK: - Building

extension SPARQLQueryBuilder {
    /// Build the SelectQuery AST
    public func buildAST() -> SelectQuery {
        var finalPattern = pattern

        // Apply GROUP BY if present
        if !groupByExprs.isEmpty {
            finalPattern = .groupBy(finalPattern, expressions: groupByExprs, aggregates: [])
        }

        return SelectQuery(
            projection: projection,
            source: .graphPattern(finalPattern),
            filter: nil,  // Filters are embedded in the pattern
            groupBy: groupByExprs.isEmpty ? nil : groupByExprs,
            having: havingExpr,
            orderBy: orderByKeys,
            limit: limitCount,
            offset: offsetCount,
            distinct: distinctFlag,
            reduced: reducedFlag
        )
    }

    /// Generate SPARQL string
    public func toSPARQL() -> String {
        let ast = buildAST()
        return ast.toSPARQL(prefixes: prefixes)
    }
}

// MARK: - Aggregate Helpers

extension SPARQLQueryBuilder {
    /// Select COUNT
    public func count(_ variable: String? = nil, distinct: Bool = false, as alias: String = "count") -> SPARQLQueryBuilder {
        var builder = self
        let countExpr: Expression = variable.map { v in
            .aggregate(.count(.variable(Variable(v)), distinct: distinct))
        } ?? .aggregate(.count(nil, distinct: distinct))
        builder.projection = .items([ProjectionItem(countExpr, alias: alias)])
        return builder
    }

    /// Select SUM
    public func sum(_ variable: String, distinct: Bool = false, as alias: String = "sum") -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.sum(.variable(Variable(variable)), distinct: distinct)),
            alias: alias
        )])
        return builder
    }

    /// Select AVG
    public func avg(_ variable: String, distinct: Bool = false, as alias: String = "avg") -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.avg(.variable(Variable(variable)), distinct: distinct)),
            alias: alias
        )])
        return builder
    }

    /// Select MIN
    public func min(_ variable: String, as alias: String = "min") -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.min(.variable(Variable(variable)))),
            alias: alias
        )])
        return builder
    }

    /// Select MAX
    public func max(_ variable: String, as alias: String = "max") -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.max(.variable(Variable(variable)))),
            alias: alias
        )])
        return builder
    }

    /// Select SAMPLE (SPARQL)
    public func sample(_ variable: String, as alias: String = "sample") -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.sample(.variable(Variable(variable)))),
            alias: alias
        )])
        return builder
    }

    /// Select GROUP_CONCAT
    public func groupConcat(
        _ variable: String,
        separator: String? = nil,
        distinct: Bool = false,
        as alias: String = "concat"
    ) -> SPARQLQueryBuilder {
        var builder = self
        builder.projection = .items([ProjectionItem(
            .aggregate(.groupConcat(.variable(Variable(variable)), separator: separator, distinct: distinct)),
            alias: alias
        )])
        return builder
    }
}

// MARK: - Result Builder Support

/// Triple pattern result builder
@resultBuilder
public struct TriplePatternBuilder {
    public static func buildBlock(_ patterns: TriplePattern...) -> [TriplePattern] {
        patterns
    }

    public static func buildArray(_ components: [[TriplePattern]]) -> [TriplePattern] {
        components.flatMap { $0 }
    }

    public static func buildOptional(_ component: [TriplePattern]?) -> [TriplePattern] {
        component ?? []
    }

    public static func buildEither(first component: [TriplePattern]) -> [TriplePattern] {
        component
    }

    public static func buildEither(second component: [TriplePattern]) -> [TriplePattern] {
        component
    }
}

extension SPARQLQueryBuilder {
    /// Add triple patterns using result builder
    public func `where`(@TriplePatternBuilder _ build: () -> [TriplePattern]) -> SPARQLQueryBuilder {
        self.where(build())
    }
}
