// GraphPatternConverter.swift
// Database - Converts QueryIR graph types to GraphIndex execution types
//
// Bridges the parsed SPARQL AST (QueryIR) to the GraphIndex execution engine.

import QueryIR
import GraphIndex
import Core

/// Converts QueryIR graph types to GraphIndex execution types.
///
/// This is the bridge between the SPARQL parser output (QueryIR types)
/// and the GraphIndex query executor (Execution types).
///
/// **Supported conversions**:
/// - `GraphPattern` → `ExecutionPattern`
/// - `TriplePattern` → `ExecutionTriple`
/// - `SPARQLTerm` → `ExecutionTerm`
/// - `PropertyPath` → `ExecutionPropertyPath`
/// - `Expression` (filter) → `FilterExpression`
/// - `AggregateBinding` → `AggregateExpression`
public struct GraphPatternConverter: Sendable {

    private init() {}

    // MARK: - GraphPattern → ExecutionPattern

    /// Convert a QueryIR.GraphPattern to a GraphIndex.ExecutionPattern
    ///
    /// - Parameters:
    ///   - pattern: The QueryIR graph pattern from the parser
    ///   - prefixes: Prefix map for expanding prefixed names (e.g., ["ex": "http://example.org/"])
    /// - Returns: An ExecutionPattern ready for the GraphIndex executor
    public static func convert(
        _ pattern: QueryIR.GraphPattern,
        prefixes: [String: String] = [:]
    ) -> ExecutionPattern {
        switch pattern {
        case .basic(let triples):
            return .basic(triples.map { convertTriple($0, prefixes: prefixes) })

        case .join(let left, let right):
            return .join(convert(left, prefixes: prefixes), convert(right, prefixes: prefixes))

        case .optional(let left, let right):
            return .optional(convert(left, prefixes: prefixes), convert(right, prefixes: prefixes))

        case .union(let left, let right):
            return .union(convert(left, prefixes: prefixes), convert(right, prefixes: prefixes))

        case .filter(let inner, let expression):
            return .filter(convert(inner, prefixes: prefixes), convertFilter(expression))

        case .minus(let left, let right):
            return .minus(convert(left, prefixes: prefixes), convert(right, prefixes: prefixes))

        case .graph(let name, let inner):
            let graphTerm = convertTerm(name, prefixes: prefixes)
            return convert(inner, prefixes: prefixes).withGraph(graphTerm)

        case .service:
            // Federation not supported — return empty pattern
            return .basic([])

        case .bind(let inner, _, _):
            // BIND not directly supported — return the inner pattern
            return convert(inner, prefixes: prefixes)

        case .values:
            // VALUES (inline data) not directly supported
            return .basic([])

        case .subquery:
            // Subquery not directly supported
            return .basic([])

        case .groupBy(let inner, let expressions, let aggregates):
            let innerConverted = convert(inner, prefixes: prefixes)
            let groupVars = expressions.compactMap { expr -> String? in
                if case .variable(let v) = expr {
                    return v.name.hasPrefix("?") ? v.name : "?\(v.name)"
                }
                return nil
            }
            let aggExprs = aggregates.map { convertAggregate($0) }
            return .groupBy(innerConverted, groupVariables: groupVars, aggregates: aggExprs, having: nil)

        case .propertyPath(let subject, let path, let object):
            return .propertyPath(
                subject: convertTerm(subject, prefixes: prefixes),
                path: convertPropertyPath(path),
                object: convertTerm(object, prefixes: prefixes)
            )

        case .lateral(let left, let right):
            return .lateral(convert(left, prefixes: prefixes), convert(right, prefixes: prefixes))
        }
    }

    // MARK: - TriplePattern → ExecutionTriple

    /// Convert a QueryIR.TriplePattern to a GraphIndex.ExecutionTriple
    public static func convertTriple(
        _ triple: QueryIR.TriplePattern,
        prefixes: [String: String] = [:]
    ) -> ExecutionTriple {
        ExecutionTriple(
            subject: convertTerm(triple.subject, prefixes: prefixes),
            predicate: convertTerm(triple.predicate, prefixes: prefixes),
            object: convertTerm(triple.object, prefixes: prefixes)
        )
    }

    // MARK: - SPARQLTerm → ExecutionTerm

    /// Convert a QueryIR.SPARQLTerm to a GraphIndex.ExecutionTerm
    ///
    /// - Parameters:
    ///   - term: The QueryIR SPARQL term
    ///   - prefixes: Prefix map for expanding prefixed names
    /// - Returns: An ExecutionTerm for the GraphIndex executor
    public static func convertTerm(
        _ term: QueryIR.SPARQLTerm,
        prefixes: [String: String] = [:]
    ) -> ExecutionTerm {
        switch term {
        case .variable(let name):
            return .variable(name.hasPrefix("?") ? name : "?\(name)")
        case .iri(let value):
            return .value(.string(value))
        case .prefixedName(let prefix, let local):
            if let base = prefixes[prefix] {
                return .value(.string(base + local))
            }
            return .value(.string("\(prefix):\(local)"))
        case .literal(let lit):
            return .value(lit.toSPARQLFieldValue())
        case .blankNode(let id):
            return .value(.string("_:\(id)"))
        case .quotedTriple(let s, let p, let o):
            return .quotedTriple(
                subject: convertTerm(s, prefixes: prefixes),
                predicate: convertTerm(p, prefixes: prefixes),
                object: convertTerm(o, prefixes: prefixes)
            )
        case .reifiedTriple(let s, let p, let o, _):
            return .quotedTriple(
                subject: convertTerm(s, prefixes: prefixes),
                predicate: convertTerm(p, prefixes: prefixes),
                object: convertTerm(o, prefixes: prefixes)
            )
        }
    }

    // MARK: - PropertyPath → ExecutionPropertyPath

    /// Convert a QueryIR.PropertyPath to a GraphIndex.ExecutionPropertyPath
    public static func convertPropertyPath(
        _ path: QueryIR.PropertyPath
    ) -> ExecutionPropertyPath {
        switch path {
        case .iri(let value):
            return .iri(value)
        case .inverse(let inner):
            return .inverse(convertPropertyPath(inner))
        case .sequence(let p1, let p2):
            return .sequence(convertPropertyPath(p1), convertPropertyPath(p2))
        case .alternative(let p1, let p2):
            return .alternative(convertPropertyPath(p1), convertPropertyPath(p2))
        case .zeroOrMore(let inner):
            return .zeroOrMore(convertPropertyPath(inner))
        case .oneOrMore(let inner):
            return .oneOrMore(convertPropertyPath(inner))
        case .zeroOrOne(let inner):
            return .zeroOrOne(convertPropertyPath(inner))
        case .negation(let iris):
            return .negatedPropertySet(iris)
        case .range(let inner, let min, let max):
            // Range quantifiers: approximate with the inner path
            // {1,} = oneOrMore, {0,} = zeroOrMore, {0,1} = zeroOrOne
            if min == 0 && max == nil {
                return .zeroOrMore(convertPropertyPath(inner))
            } else if min == 1 && max == nil {
                return .oneOrMore(convertPropertyPath(inner))
            } else if min == 0 && max == 1 {
                return .zeroOrOne(convertPropertyPath(inner))
            }
            // Default: treat as oneOrMore for bounded ranges
            return .oneOrMore(convertPropertyPath(inner))
        }
    }

    // MARK: - Expression → FilterExpression

    /// Convert a QueryIR.Expression to a GraphIndex.FilterExpression
    ///
    /// Extracts referenced variables for filter pushdown optimization,
    /// then wraps the expression evaluation in a closure that uses
    /// ExpressionEvaluator for runtime evaluation against each VariableBinding.
    public static func convertFilter(
        _ expression: QueryIR.Expression
    ) -> FilterExpression {
        let variables = extractExpressionVariables(expression)
        return .customWithVariables({ binding in
            ExpressionEvaluator.evaluateAsBoolean(expression, binding: binding)
        }, variables: variables)
    }

    /// Extract all variables referenced in an expression
    ///
    /// Recursively traverses the expression tree to find all variable references.
    /// This enables proper filter pushdown optimization.
    private static func extractExpressionVariables(
        _ expr: QueryIR.Expression
    ) -> Set<String> {
        var vars = Set<String>()
        collectExpressionVariables(from: expr, into: &vars)
        return vars
    }

    /// Recursively collect variables from an expression
    private static func collectExpressionVariables(
        from expr: QueryIR.Expression,
        into vars: inout Set<String>
    ) {
        switch expr {
        case .variable(let v):
            let name = v.name.hasPrefix("?") ? v.name : "?\(v.name)"
            vars.insert(name)

        case .bound(let v):
            let name = v.name.hasPrefix("?") ? v.name : "?\(v.name)"
            vars.insert(name)

        // Binary operators
        case .add(let l, let r), .subtract(let l, let r), .multiply(let l, let r),
             .divide(let l, let r), .modulo(let l, let r), .equal(let l, let r),
             .notEqual(let l, let r), .lessThan(let l, let r), .lessThanOrEqual(let l, let r),
             .greaterThan(let l, let r), .greaterThanOrEqual(let l, let r),
             .and(let l, let r), .or(let l, let r):
            collectExpressionVariables(from: l, into: &vars)
            collectExpressionVariables(from: r, into: &vars)

        // Unary operators
        case .negate(let e), .not(let e), .isNull(let e), .isNotNull(let e):
            collectExpressionVariables(from: e, into: &vars)

        // RDF-star
        case .isTriple(let e), .subject(let e), .predicate(let e), .object(let e):
            collectExpressionVariables(from: e, into: &vars)

        case .triple(let s, let p, let o):
            collectExpressionVariables(from: s, into: &vars)
            collectExpressionVariables(from: p, into: &vars)
            collectExpressionVariables(from: o, into: &vars)

        // Functions
        case .function(let call):
            for arg in call.arguments {
                collectExpressionVariables(from: arg, into: &vars)
            }

        case .aggregate(let agg):
            switch agg {
            case .count(let e, _):
                if let e = e { collectExpressionVariables(from: e, into: &vars) }
            case .sum(let e, _), .avg(let e, _), .min(let e), .max(let e), .sample(let e):
                collectExpressionVariables(from: e, into: &vars)
            case .groupConcat(let e, _, _), .arrayAgg(let e, _, _):
                collectExpressionVariables(from: e, into: &vars)
            }

        // Conditional
        case .coalesce(let exprs):
            for e in exprs { collectExpressionVariables(from: e, into: &vars) }

        case .nullIf(let l, let r):
            collectExpressionVariables(from: l, into: &vars)
            collectExpressionVariables(from: r, into: &vars)

        // Between, In, Like, Regex
        case .between(let e, let low, let high):
            collectExpressionVariables(from: e, into: &vars)
            collectExpressionVariables(from: low, into: &vars)
            collectExpressionVariables(from: high, into: &vars)

        case .inList(let e, let list):
            collectExpressionVariables(from: e, into: &vars)
            for item in list { collectExpressionVariables(from: item, into: &vars) }
        case .notInList(let e, let list):
            collectExpressionVariables(from: e, into: &vars)
            for item in list { collectExpressionVariables(from: item, into: &vars) }

        case .like(let e, _):
            collectExpressionVariables(from: e, into: &vars)

        case .regex(let e, _, _):
            collectExpressionVariables(from: e, into: &vars)

        // Cast
        case .cast(let e, _):
            collectExpressionVariables(from: e, into: &vars)

        // Case/When
        case .caseWhen(let cases, let elseResult):
            for pair in cases {
                collectExpressionVariables(from: pair.condition, into: &vars)
                collectExpressionVariables(from: pair.result, into: &vars)
            }
            if let el = elseResult { collectExpressionVariables(from: el, into: &vars) }

        // Subqueries (variables from subquery are internal, not exposed)
        case .subquery, .exists, .inSubquery:
            break

        // Literals and constants (no variables)
        case .literal, .column:
            break
        }
    }

    // MARK: - AggregateBinding → AggregateExpression

    /// Convert a QueryIR.AggregateBinding to a GraphIndex.AggregateExpression
    public static func convertAggregate(
        _ binding: QueryIR.AggregateBinding
    ) -> AggregateExpression {
        let alias = binding.variable
        switch binding.aggregate {
        case .count(let expr, let distinct):
            if let expr = expr, case .variable(let v) = expr {
                let varName = v.name.hasPrefix("?") ? v.name : "?\(v.name)"
                return distinct
                    ? .countDistinct(varName, as: alias)
                    : .count(varName, as: alias)
            }
            return distinct
                ? .countAllDistinct(as: alias)
                : .countAll(as: alias)

        case .sum(let expr, _):
            let varName = extractVariableName(expr)
            return .sum(varName, as: alias)

        case .avg(let expr, _):
            let varName = extractVariableName(expr)
            return .avg(varName, as: alias)

        case .min(let expr):
            let varName = extractVariableName(expr)
            return .min(varName, as: alias)

        case .max(let expr):
            let varName = extractVariableName(expr)
            return .max(varName, as: alias)

        case .sample(let expr):
            let varName = extractVariableName(expr)
            return .sample(varName, as: alias)

        case .groupConcat(let expr, let separator, let distinct):
            let varName = extractVariableName(expr)
            return distinct
                ? .groupConcatDistinct(varName, separator: separator ?? " ", as: alias)
                : .groupConcat(varName, separator: separator ?? " ", as: alias)

        case .arrayAgg(let expr, _, _):
            // arrayAgg is SQL-specific; approximate with groupConcat
            let varName = extractVariableName(expr)
            return .groupConcat(varName, separator: ", ", as: alias)
        }
    }

    // MARK: - Helpers

    /// Extract a variable name from an expression
    private static func extractVariableName(_ expr: QueryIR.Expression) -> String {
        switch expr {
        case .variable(let v):
            return v.name.hasPrefix("?") ? v.name : "?\(v.name)"
        case .column(let col):
            return "?\(col.column)"
        default:
            return "?_expr"
        }
    }
}
