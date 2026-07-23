// GraphPatternConverter.swift
// GraphIndex - Converts QueryIR graph types to GraphIndex execution types
//
// Converts the parsed SPARQL AST into GraphIndex execution structures.

import QueryIR
import Core
import Graph

/// Converts QueryIR graph types to GraphIndex execution types.
///
/// Converts SPARQL parser output (`QueryIR` types) into the graph query model.
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

    private struct BlankNodeVariableScope {
        let identifier: UInt64
        private var variables: [String: String] = [:]

        mutating func variable(for label: String) -> String {
            if let variable = variables[label] {
                return variable
            }
            let variable = SPARQLInternalVariable.blankNode(
                label: label,
                scope: identifier
            )
            variables[label] = variable
            return variable
        }
    }

    // MARK: - GraphPattern → ExecutionPattern

    /// Convert a QueryIR.GraphPattern to a GraphIndex.ExecutionPattern
    ///
    /// - Parameters:
    ///   - pattern: The QueryIR graph pattern from the parser
    ///   - prefixes: Prefix map for expanding prefixed names (e.g., ["ex": "http://example.org/"])
    /// - Returns: An ExecutionPattern ready for the GraphIndex executor
    public static func convert(
        _ pattern: QueryIR.GraphPattern,
        prefixes: [String: String] = [:],
        structuralLimits: QueryStructuralLimits = .default
    ) throws -> ExecutionPattern {
        try SPARQLSemanticValidator.validate(
            pattern,
            limits: structuralLimits
        )
        var context = SPARQLAlgebraCompilationContext()
        return try convert(
            pattern,
            prefixes: prefixes,
            context: &context,
            subqueryInputPolicy: .isolated,
            inputVariables: []
        )
    }

    package static func convert(
        _ pattern: QueryIR.GraphPattern,
        prefixes: [String: String],
        context: inout SPARQLAlgebraCompilationContext,
        subqueryInputPolicy: SPARQLSubqueryInputPolicy,
        inputVariables: Set<String>
    ) throws -> ExecutionPattern {
        switch pattern {
        case .basic(let basicGraphPattern):
            return try convertBasicGraphPattern(
                basicGraphPattern,
                prefixes: prefixes,
                blankNodeScopeIdentifier: context.takeBlankNodeScope()
            )

        case .join(let left, let right):
            let convertedLeft = try convert(
                left,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let convertedRight = try convert(
                right,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables.union(
                    convertedLeft.outputVariables
                )
            )
            return .join(convertedLeft, convertedRight)

        case .optional(let left, let right):
            let convertedLeft = try convert(
                left,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let convertedRight = try convert(
                right,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables.union(
                    convertedLeft.outputVariables
                )
            )
            return .optional(convertedLeft, convertedRight)

        case .union(let left, let right):
            let convertedLeft = try convert(
                left,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let convertedRight = try convert(
                right,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            return .union(convertedLeft, convertedRight)

        case .filter(let inner, let expression):
            let convertedInner = try convert(
                inner,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let convertedFilter = try convertFilter(expression)
            return .filter(
                convertedInner,
                convertedFilter
            )

        case .minus(let left, let right):
            let convertedLeft = try convert(
                left,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let convertedRight = try convert(
                right,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            return .minus(convertedLeft, convertedRight)

        case .graph(let name, let inner):
            let selector = try convertGraphSelector(name, prefixes: prefixes)
            let convertedInner = try convert(
                inner,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables.union(selector.variables)
            )
            return .graph(selector, convertedInner)

        case .service:
            throw GraphPatternConversionError.unsupportedGraphPattern("SERVICE")

        case .bind(let inner, let variable, let expression):
            let target = "?\(variable)"
            let converted = try convert(
                inner,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let availableVariables = converted.outputVariables.union(
                inputVariables
            )
            guard !availableVariables.contains(target) else {
                throw GraphPatternConversionError.variableAlreadyInScope(target)
            }
            let plan = try SPARQLExpressionPlan(expression)
            return .extend(
                converted,
                variable: target,
                expression: plan
            )

        case .values(let variables, let bindings):
            return try convertValues(
                variables: variables,
                bindings: bindings
            )

        case .subquery(let query):
            return .subquery(
                try SPARQLSelectPlanCompiler.compileSubquery(
                    query,
                    inputPolicy: subqueryInputPolicy,
                    inputVariables: inputVariables,
                    context: &context
                )
            )

        case .groupBy(let inner, let expressions, let aggregates):
            let innerConverted = try convert(
                inner,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            var groupKeys: [SPARQLGroupKeyPlan] = []
            groupKeys.reserveCapacity(expressions.count)
            for (index, expression) in expressions.enumerated() {
                let plan = try SPARQLExpressionPlan(expression)
                let outputVariable: String
                if case .variable(let variable) = expression {
                    outputVariable = "?\(variable.name)"
                } else {
                    outputVariable = SPARQLInternalVariable.executionName(
                        forRawName: SPARQLInternalVariable.groupKeyRaw(
                            UInt64(index)
                        )
                    )
                }
                groupKeys.append(
                    SPARQLGroupKeyPlan(
                        outputVariable: outputVariable,
                        expression: plan
                    )
                )
            }
            var aggExprs: [AggregateExpression] = []
            aggExprs.reserveCapacity(aggregates.count)
            for binding in aggregates {
                let aggregate = try convertAggregate(binding)
                aggExprs.append(aggregate)
            }
            return .groupBy(
                innerConverted,
                grouping: .explicit(consume groupKeys),
                aggregates: aggExprs,
                having: nil
            )

        case .lateral(let left, let right):
            let convertedLeft = try convert(
                left,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: subqueryInputPolicy,
                inputVariables: inputVariables
            )
            let convertedRight = try convert(
                right,
                prefixes: prefixes,
                context: &context,
                subqueryInputPolicy: .lateral,
                inputVariables: inputVariables.union(
                    convertedLeft.outputVariables
                )
            )
            return .lateral(convertedLeft, convertedRight)
        }
    }

    private static func convertBasicGraphPattern(
        _ pattern: borrowing QueryIR.BasicGraphPattern,
        prefixes: [String: String],
        blankNodeScopeIdentifier: UInt64
    ) throws -> ExecutionPattern {
        let expansionPlan = try SPARQLReifiedTripleExpansionPlan
            .basicGraphPattern(pattern)
        var loweredTriples: [ExecutionTriple] = []
        loweredTriples.reserveCapacity(expansionPlan.totalTripleCount)
        var loweredPaths: [ExecutionPattern] = []
        var blankNodeScope = BlankNodeVariableScope(
            identifier: blankNodeScopeIdentifier
        )

        for element in pattern.elements {
            switch element {
            case .triple(let triple):
                try appendConvertedTriple(
                    triple,
                    prefixes: prefixes,
                    blankNodeScope: &blankNodeScope,
                    to: &loweredTriples
                )
            case .propertyPath(let pathPattern):
                let subject = try lowerTerm(
                    pathPattern.subject,
                    prefixes: prefixes,
                    blankNodeScope: &blankNodeScope,
                    supplementalTriples: &loweredTriples
                )
                let object = try lowerTerm(
                    pathPattern.object,
                    prefixes: prefixes,
                    blankNodeScope: &blankNodeScope,
                    supplementalTriples: &loweredTriples
                )
                loweredPaths.append(
                    .propertyPath(
                        subject: subject,
                        path: convertPropertyPath(pathPattern.path),
                        object: object
                    )
                )
            }
        }

        var result: ExecutionPattern?
        if !loweredTriples.isEmpty {
            result = .basic(loweredTriples)
        }
        for path in loweredPaths {
            if let existing = result {
                result = .join(existing, path)
            } else {
                result = path
            }
        }
        return result ?? .basic([])
    }

    private static func convertValues(
        variables: [String],
        bindings: [[QueryIR.Literal?]]
    ) throws -> ExecutionPattern {
        var normalizedVariables: [String] = []
        normalizedVariables.reserveCapacity(variables.count)
        var seen = Set<String>()
        for variable in variables {
            let normalized = normalizedVariable(variable)
            guard seen.insert(normalized).inserted else {
                throw GraphPatternConversionError.duplicateValuesVariable(
                    normalized
                )
            }
            normalizedVariables.append(normalized)
        }

        let (cellCount, overflow) = bindings.count.multipliedReportingOverflow(
            by: normalizedVariables.count
        )
        guard !overflow else {
            throw GraphPatternConversionError.valuesCellCountOverflow(
                rows: bindings.count,
                columns: normalizedVariables.count
            )
        }

        var cells: [FieldValue?] = []
        cells.reserveCapacity(cellCount)
        for (rowIndex, row) in bindings.enumerated() {
            guard row.count == normalizedVariables.count else {
                throw GraphPatternConversionError.valuesRowWidth(
                    row: rowIndex,
                    expected: normalizedVariables.count,
                    actual: row.count
                )
            }
            for literal in row {
                if let literal {
                    cells.append(try literal.toSPARQLFieldValue())
                } else {
                    cells.append(nil)
                }
            }
        }

        return .values(
            SPARQLValuesTable(
                variables: consume normalizedVariables,
                rowCount: bindings.count,
                cells: consume cells
            )
        )
    }

    private static func convertGraphSelector(
        _ term: QueryIR.SPARQLTerm,
        prefixes: [String: String]
    ) throws -> ExecutionGraphSelector {
        if case .variable(let name) = term {
            return .variable("?\(name)")
        }

        let iri: String
        switch term {
        case .iri(let value):
            iri = value
        case .variable, .literal, .blankNode, .tripleTerm, .reifiedTriple:
            throw GraphPatternConversionError.graphSelectorMustBeIRIOrVariable
        }

        do {
            return .named(try RDFGraphName(iri: iri))
        } catch {
            throw GraphPatternConversionError.invalidGraphIRI(iri)
        }
    }

    private static func appendConvertedTriple(
        _ triple: QueryIR.TriplePattern,
        prefixes: [String: String],
        blankNodeScope: inout BlankNodeVariableScope,
        to triples: inout [ExecutionTriple]
    ) throws {
        let subject = try lowerTerm(
            triple.subject,
            prefixes: prefixes,
            blankNodeScope: &blankNodeScope,
            supplementalTriples: &triples
        )
        let predicate = try lowerTerm(
            triple.predicate,
            prefixes: prefixes,
            blankNodeScope: &blankNodeScope,
            supplementalTriples: &triples
        )
        let object = try lowerTerm(
            triple.object,
            prefixes: prefixes,
            blankNodeScope: &blankNodeScope,
            supplementalTriples: &triples
        )
        triples.append(
            ExecutionTriple(
                subject: subject,
                predicate: predicate,
                object: object
            )
        )
    }

    private static func lowerTerm(
        _ term: QueryIR.SPARQLTerm,
        prefixes: [String: String],
        blankNodeScope: inout BlankNodeVariableScope,
        supplementalTriples: inout [ExecutionTriple]
    ) throws -> ExecutionTerm {
        switch term {
        case .variable(let name):
            return .variable("?\(name)")
        case .iri(let value):
            return .value(.rdfTerm(.iri(value)))
        case .literal(let literal):
            return .value(try literal.toSPARQLFieldValue())
        case .blankNode(let identifier):
            return .variable(blankNodeScope.variable(for: identifier))
        case .tripleTerm(let subject, let predicate, let object):
            let loweredSubject = try lowerTerm(
                subject,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            let loweredPredicate = try lowerTerm(
                predicate,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            let loweredObject = try lowerTerm(
                object,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            return .tripleTerm(
                subject: loweredSubject,
                predicate: loweredPredicate,
                object: loweredObject
            )
        case .reifiedTriple(let subject, let predicate, let object, let reifier):
            let loweredSubject = try lowerTerm(
                subject,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            let loweredPredicate = try lowerTerm(
                predicate,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            let loweredObject = try lowerTerm(
                object,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            let loweredReifier = try lowerTerm(
                reifier,
                prefixes: prefixes,
                blankNodeScope: &blankNodeScope,
                supplementalTriples: &supplementalTriples
            )
            let tripleTerm = ExecutionTerm.tripleTerm(
                subject: loweredSubject,
                predicate: loweredPredicate,
                object: loweredObject
            )
            let reifyingTriple = ExecutionTriple(
                subject: loweredReifier,
                predicate: .value(
                    .rdfTerm(
                        .iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies")
                    )
                ),
                object: tripleTerm
            )
            supplementalTriples.append(reifyingTriple)
            return loweredReifier
        }
    }

    private static func convertPropertyPath(
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
        case .negatedPropertySet(let exclusions):
            return .negatedPropertySet(exclusions)
        case .range(let inner, let bounds):
            return .range(convertPropertyPath(inner), bounds)
        }
    }

    // MARK: - Expression → FilterExpression

    /// Convert a QueryIR.Expression to a GraphIndex.FilterExpression
    ///
    /// Preserves QueryIR and expression traits for the execution layer.
    package static func convertFilter(
        _ expression: QueryIR.Expression
    ) throws -> FilterExpression {
        .query(try SPARQLExpressionPlan(expression))
    }

    /// Adds SPARQL projection expressions as Extend algebra nodes. Aggregate
    /// expressions are evaluated by Group and are therefore not extended here.
    package static func applyingProjectionExpressions(
        _ projection: QueryIR.Projection,
        to pattern: ExecutionPattern,
        inputVariables: Set<String> = [],
        reservedTargetVariables: Set<String> = [],
        restrictExpressionReferencesToScope: Bool = false
    ) throws -> ExecutionPattern {
        let items: [QueryIR.ProjectionItem]
        switch projection {
        case .items(let values), .distinctItems(let values):
            items = values
        case .all, .allFrom:
            return pattern
        }

        var aliases = Set<String>()
        for item in items {
            guard let alias = item.alias else { continue }
            let target = normalizedVariable(alias)
            guard aliases.insert(target).inserted else {
                throw GraphPatternConversionError.duplicateProjectionAlias(target)
            }
        }

        var result = pattern
        for item in items {
            guard let alias = item.alias else {
                switch item.expression {
                case .variable, .column:
                    continue
                default:
                    throw GraphPatternConversionError.projectionExpressionRequiresAlias
                }
            }

            let target = normalizedVariable(alias)
            if case .aggregate = item.expression { continue }

            let plan = try SPARQLExpressionPlan(item.expression)
            let expressionScope = result.outputVariables.union(inputVariables)
            if restrictExpressionReferencesToScope,
               let missingVariable = plan.referencedVariables
                    .subtracting(expressionScope)
                    .sorted()
                    .first {
                throw GraphPatternConversionError
                    .projectionVariableNotInScope(missingVariable)
            }
            guard !expressionScope.union(reservedTargetVariables)
                .contains(target) else {
                throw GraphPatternConversionError.variableAlreadyInScope(target)
            }
            result = .extend(result, variable: target, expression: plan)
        }
        return result
    }

    // MARK: - AggregateBinding → AggregateExpression

    /// Convert a QueryIR.AggregateBinding to a GraphIndex.AggregateExpression
    package static func convertAggregate(
        _ binding: QueryIR.AggregateBinding
    ) throws -> AggregateExpression {
        let alias = "?\(binding.variable)"
        switch binding.aggregate {
        case .count(let expr, let distinct):
            return .count(
                expression: try expr.map { try SPARQLExpressionPlan($0) },
                distinct: distinct,
                alias: alias
            )

        case .sum(let expr, let distinct):
            return .sum(
                expression: try SPARQLExpressionPlan(expr),
                distinct: distinct,
                alias: alias
            )

        case .avg(let expr, let distinct):
            return .avg(
                expression: try SPARQLExpressionPlan(expr),
                distinct: distinct,
                alias: alias
            )

        case .min(let expr):
            return .min(
                expression: try SPARQLExpressionPlan(expr),
                alias: alias
            )

        case .max(let expr):
            return .max(
                expression: try SPARQLExpressionPlan(expr),
                alias: alias
            )

        case .sample(let expr):
            return .sample(
                expression: try SPARQLExpressionPlan(expr),
                alias: alias
            )

        case .groupConcat(let expr, let separator, let distinct):
            return .groupConcat(
                expression: try SPARQLExpressionPlan(expr),
                separator: separator ?? " ",
                distinct: distinct,
                alias: alias
            )

        case .arrayAgg:
            throw GraphPatternConversionError.unsupportedAggregateExpression("ARRAY_AGG")
        }
    }

    // MARK: - Helpers

    private static func normalizedVariable(_ variable: String) -> String {
        return "?\(variable)"
    }

}
