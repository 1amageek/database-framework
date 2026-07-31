// ExecutionPattern.swift
// GraphIndex - SPARQL-like graph pattern algebra
//
// Represents composed graph patterns following SPARQL algebra.


/// Represents a graph pattern that can be composed
///
/// **Design**: Algebraic representation of SPARQL graph patterns
/// following the SPARQL Algebra specification. Patterns can be composed
/// to create complex queries with joins, optionals, unions, and filters.
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 18.2 (SPARQL Algebra)
public indirect enum ExecutionPattern: Sendable {

    /// Basic Graph Pattern (BGP): a set of triple patterns
    ///
    /// Multiple patterns in a BGP are implicitly joined.
    /// This is the fundamental building block.
    case basic([ExecutionTriple])

    /// Join of two graph patterns (AND semantics)
    ///
    /// Both patterns must match. Variables shared between patterns
    /// are joined (must have equal values).
    case join(ExecutionPattern, ExecutionPattern)

    /// Left outer join (OPTIONAL semantics)
    ///
    /// Left pattern must match; right pattern is optional.
    /// If right pattern doesn't match, variables from right
    /// will be unbound in the result.
    case optional(ExecutionPattern, ExecutionPattern)

    /// Union of two graph patterns (OR semantics)
    ///
    /// Either pattern can match. Results from both branches
    /// are combined.
    case union(ExecutionPattern, ExecutionPattern)

    /// Filter applied to a graph pattern
    ///
    /// Pattern must match AND filter expression must evaluate to true.
    case filter(ExecutionPattern, FilterExpression)

    /// Extend every input solution with the value of a canonical SPARQL
    /// expression. Expression errors leave the target variable unbound while
    /// resource and runtime failures abort execution.
    case extend(ExecutionPattern, variable: String, expression: SPARQLExpressionPlan)

    /// A compiled inline solution relation from SPARQL VALUES.
    case values(SPARQLValuesTable)

    /// Evaluate a pattern with a named graph as its active graph.
    case graph(ExecutionGraphSelector, ExecutionPattern)

    /// Group by pattern with aggregation
    ///
    /// Groups results by specified variables and applies aggregate functions.
    /// Optionally includes a HAVING filter applied after aggregation.
    ///
    /// - Parameters:
    ///   - pattern: The source pattern to group
    ///   - grouping: Explicit keys or the implicit aggregate grouping mode
    ///   - aggregates: Aggregate expressions to compute
    ///   - having: Optional filter on aggregate results
    case groupBy(
        ExecutionPattern,
        grouping: SPARQLGroupingPlan,
        aggregates: [AggregateExpression],
        having: FilterExpression?
    )

    /// Property path pattern
    ///
    /// Matches paths between subject and object using property path expression.
    /// Supports transitive closure, inverse paths, sequences, and alternatives.
    ///
    /// - Parameters:
    ///   - subject: The subject term (variable or value)
    ///   - path: The property path expression
    ///   - object: The object term (variable or value)
    /// Difference of two graph patterns (MINUS semantics)
    ///
    /// Keep left bindings that have no compatible solution in right.
    /// Compatible = agree on all shared variables AND share at least one variable.
    /// If no shared variables, left binding is always kept.
    ///
    /// **Reference**: W3C SPARQL 1.1, Section 18.5
    case minus(ExecutionPattern, ExecutionPattern)

    case propertyPath(subject: ExecutionTerm, path: ExecutionPropertyPath, object: ExecutionTerm)

    /// LATERAL join (correlated subquery — SPARQL 1.2)
    ///
    /// For each solution from the left pattern, the right pattern is evaluated
    /// with the left's variable bindings injected. Results are unioned.
    case lateral(ExecutionPattern, ExecutionPattern)

    /// An independently compiled Select algebra boundary.
    case subquery(SPARQLSubqueryExecutionPlan)

    // MARK: - Variables

    /// Variables made visible to the enclosing algebra node.
    public var outputVariables: Set<String> {
        switch self {
        case .basic(let patterns):
            return patterns.reduce(into: Set<String>()) { result, pattern in
                result.formUnion(pattern.variables)
            }
        case .join(let left, let right):
            return left.outputVariables.union(right.outputVariables)
        case .optional(let left, let right):
            return left.outputVariables.union(right.outputVariables)
        case .union(let left, let right):
            return left.outputVariables.union(right.outputVariables)
        case .filter(let pattern, _):
            return pattern.outputVariables
        case .extend(let pattern, let variable, _):
            return pattern.outputVariables.union([variable])
        case .values(let table):
            return Set(table.variables)
        case .graph(let selector, let pattern):
            return selector.variables.union(pattern.outputVariables)
        case .groupBy(_, let grouping, let aggregates, _):
            var result = Set(grouping.keys.lazy.map { $0.outputVariable })
            for agg in aggregates {
                result.insert(agg.alias)
            }
            return result
        case .minus(let left, _):
            return left.outputVariables
        case .propertyPath(let subject, _, let object):
            var result = Set<String>()
            if case .variable(let name) = subject { result.insert(name) }
            if case .variable(let name) = object { result.insert(name) }
            return result
        case .lateral(let left, let right):
            return left.outputVariables.union(right.outputVariables)
        case .subquery(let plan):
            return Set(plan.select.projectionVariables)
        }
    }

    /// Every variable mentioned by this algebra tree, including expression-only
    /// references that are not projected by the node.
    public var referencedVariables: Set<String> {
        switch self {
        case .basic(let patterns):
            return patterns.reduce(into: Set<String>()) { result, pattern in
                result.formUnion(pattern.variables)
            }
        case .join(let left, let right), .optional(let left, let right),
             .union(let left, let right), .minus(let left, let right),
             .lateral(let left, let right):
            return left.referencedVariables.union(right.referencedVariables)
        case .filter(let pattern, let expression):
            return pattern.referencedVariables.union(expression.variables)
        case .extend(let pattern, _, let expression):
            return pattern.referencedVariables.union(
                expression.referencedVariables
            )
        case .values(let table):
            return Set(table.variables)
        case .graph(let selector, let pattern):
            return selector.variables.union(pattern.referencedVariables)
        case .groupBy(let pattern, let grouping, let aggregates, let having):
            var result = pattern.referencedVariables
            for key in grouping.keys {
                result.formUnion(key.expression.referencedVariables)
            }
            for aggregate in aggregates {
                if let expression = aggregate.inputExpression {
                    result.formUnion(expression.referencedVariables)
                }
            }
            if let having {
                result.formUnion(having.variables)
            }
            return result
        case .propertyPath(let subject, _, let object):
            var result = Set<String>()
            if case .variable(let name) = subject { result.insert(name) }
            if case .variable(let name) = object { result.insert(name) }
            return result
        case .subquery(let plan):
            return plan.select.ordered.algebra.referencedVariables
        }
    }

    /// Variables that must be bound (appear in required patterns)
    ///
    /// For OPTIONAL, only variables from the left side are required.
    public var requiredOutputVariables: Set<String> {
        switch self {
        case .basic(let patterns):
            return patterns.reduce(into: Set<String>()) { result, pattern in
                result.formUnion(pattern.variables)
            }
        case .join(let left, let right):
            return left.requiredOutputVariables.union(
                right.requiredOutputVariables
            )
        case .optional(let left, _):
            return left.requiredOutputVariables
        case .union(let left, let right):
            return left.requiredOutputVariables.intersection(
                right.requiredOutputVariables
            )
        case .filter(let pattern, _):
            return pattern.requiredOutputVariables
        case .extend(let pattern, _, _):
            return pattern.requiredOutputVariables
        case .values(let table):
            return table.alwaysBoundVariables
        case .graph(let selector, let pattern):
            return selector.variables.union(pattern.requiredOutputVariables)
        case .groupBy:
            // Group expressions and aggregate evaluation can leave individual
            // outputs unbound under SPARQL expression-error semantics.
            return []
        case .minus(let left, _):
            return left.requiredOutputVariables
        case .propertyPath(let subject, _, let object):
            var result = Set<String>()
            if case .variable(let name) = subject { result.insert(name) }
            if case .variable(let name) = object { result.insert(name) }
            return result
        case .lateral(let left, let right):
            return left.requiredOutputVariables.union(
                right.requiredOutputVariables
            )
        case .subquery(let plan):
            return Set(plan.select.projectionVariables)
        }
    }

    /// Variables that might be unbound (from OPTIONAL or UNION)
    public var optionalVariables: Set<String> {
        outputVariables.subtracting(requiredOutputVariables)
    }

    // MARK: - Pattern Analysis

    /// Whether this is an empty pattern
    public var isEmpty: Bool {
        switch self {
        case .basic(let patterns):
            return patterns.isEmpty
        case .join(let left, let right):
            return left.isEmpty && right.isEmpty
        case .optional(let left, _):
            return left.isEmpty
        case .union(let left, let right):
            return left.isEmpty && right.isEmpty
        case .filter(let pattern, _):
            return pattern.isEmpty
        case .extend(let pattern, _, _):
            return pattern.isEmpty
        case .values(let table):
            return table.rowCount == 0
        case .graph(_, let pattern):
            return pattern.isEmpty
        case .groupBy(let pattern, _, _, _):
            return pattern.isEmpty
        case .minus(let left, _):
            return left.isEmpty
        case .propertyPath:
            return false  // Property paths are never empty
        case .lateral(let left, _):
            return left.isEmpty
        case .subquery:
            return false
        }
    }

    /// Number of atomic triple patterns contained in this algebra tree.
    public var patternCount: Int {
        switch self {
        case .basic(let patterns):
            return patterns.count
        case .join(let left, let right):
            return left.patternCount + right.patternCount
        case .optional(let left, let right):
            return left.patternCount + right.patternCount
        case .union(let left, let right):
            return left.patternCount + right.patternCount
        case .filter(let pattern, _):
            return pattern.patternCount
        case .extend(let pattern, _, _):
            return pattern.patternCount
        case .values:
            return 1
        case .graph(_, let pattern):
            return pattern.patternCount
        case .groupBy(let pattern, _, _, _):
            return pattern.patternCount
        case .minus(let left, let right):
            return left.patternCount + right.patternCount
        case .propertyPath:
            return 1
        case .lateral(let left, let right):
            return left.patternCount + right.patternCount
        case .subquery(let plan):
            return plan.select.ordered.algebra.patternCount
        }
    }

    // MARK: - Convenience Constructors

    /// Create a basic pattern from a single triple pattern
    public static func single(_ pattern: ExecutionTriple) -> ExecutionPattern {
        .basic([pattern])
    }

    /// Create a basic pattern from string literals
    public static func triple(_ subject: String, _ predicate: String, _ object: String) -> ExecutionPattern {
        .basic([ExecutionTriple(subject, predicate, object)])
    }

    /// Create an empty pattern
    public static var empty: ExecutionPattern {
        .basic([])
    }

}

// MARK: - CustomStringConvertible

extension ExecutionPattern: CustomStringConvertible {
    public var description: String {
        switch self {
        case .basic(let patterns):
            let pats = patterns.map { $0.description }.joined(separator: " . ")
            return "{ \(pats) }"
        case .join(let left, let right):
            return "\(left) JOIN \(right)"
        case .optional(let left, let right):
            return "\(left) OPTIONAL \(right)"
        case .union(let left, let right):
            return "\(left) UNION \(right)"
        case .filter(let pattern, let expr):
            return "\(pattern) FILTER(\(expr))"
        case .extend(let pattern, let variable, let expression):
            return "\(pattern) EXTEND(\(variable) := \(expression))"
        case .values(let table):
            return "VALUES[\(table.rowCount)x\(table.variables.count)]"
        case .graph(let selector, let pattern):
            return "GRAPH \(selector) \(pattern)"
        case .groupBy(let pattern, let grouping, let aggregates, let having):
            let keys = grouping.keys.map {
                "\($0.expression) AS \($0.outputVariable)"
            }
            var result = "\(pattern) GROUP BY \(keys.joined(separator: ", "))"
            if !aggregates.isEmpty {
                result += " AGGREGATES(\(aggregates.map { $0.description }.joined(separator: ", ")))"
            }
            if let having = having {
                result += " HAVING(\(having))"
            }
            return result
        case .minus(let left, let right):
            return "\(left) MINUS \(right)"
        case .propertyPath(let subject, let path, let object):
            return "{ \(subject) \(path) \(object) }"
        case .lateral(let left, let right):
            return "\(left) LATERAL \(right)"
        case .subquery(let plan):
            return "SUBQUERY[\(plan.occurrenceIdentifier)](\(plan.select.ordered.algebra))"
        }
    }
}
