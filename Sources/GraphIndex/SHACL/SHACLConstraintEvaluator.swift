// SHACLConstraintEvaluator.swift
// GraphIndex - Evaluate SHACL constraints against value nodes
//
// Delegates to existing components:
// - OWLDatatypeValidator for XSD facet/datatype constraints
// - SPARQLQueryExecutor for cardinality and property pair constraints
// - OWLReasoner for class hierarchy (when OWL entailment is enabled)
//
// Reference: W3C SHACL §4 (Core Constraint Components)
// https://www.w3.org/TR/shacl/#core-components

import Foundation
import FoundationDB
import Graph
import Core
import DatabaseEngine

/// Evaluates individual SHACL constraints against focus nodes and value nodes
struct SHACLConstraintEvaluator: Sendable {

    private let executor: SPARQLQueryExecutor
    private let datatypeValidator: OWLDatatypeValidator
    private let reasoner: OWLReasoner?

    init(
        executor: SPARQLQueryExecutor,
        datatypeValidator: OWLDatatypeValidator = OWLDatatypeValidator(),
        reasoner: OWLReasoner? = nil
    ) {
        self.executor = executor
        self.datatypeValidator = datatypeValidator
        self.reasoner = reasoner
    }

    // MARK: - Main Evaluation

    /// Evaluate a constraint against collected value nodes
    ///
    /// - Parameters:
    ///   - constraint: The SHACL constraint to evaluate
    ///   - focusNode: The focus node IRI
    ///   - valueNodes: The value nodes collected via path evaluation
    ///   - path: The property path (for result reporting)
    ///   - severity: The severity for generated results
    ///   - messages: Custom messages from the shape
    ///   - validator: Reference to the parent validator for recursive evaluation
    /// - Returns: Array of validation results (empty if constraint satisfied)
    func evaluate(
        constraint: SHACLConstraint,
        focusNode: String,
        valueNodes: [RDFTerm],
        path: SHACLPath?,
        severity: SHACLSeverity,
        messages: [String],
        sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        switch constraint {
        // §4.1 Value Type Constraints
        case .class_(let classIRI):
            return try await evaluateClass(classIRI, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .datatype(let datatypeIRI):
            return evaluateDatatype(datatypeIRI, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .nodeKind(let kind):
            return evaluateNodeKind(kind, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.2 Cardinality Constraints
        case .minCount(let min):
            return evaluateMinCount(min, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxCount(let max):
            return evaluateMaxCount(max, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.3 Value Range Constraints
        case .minExclusive(let bound):
            return evaluateValueRange(.minExclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxExclusive(let bound):
            return evaluateValueRange(.maxExclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .minInclusive(let bound):
            return evaluateValueRange(.minInclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxInclusive(let bound):
            return evaluateValueRange(.maxInclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.4 String-based Constraints
        case .minLength(let len):
            return evaluateStringLength(min: len, max: nil, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxLength(let len):
            return evaluateStringLength(min: nil, max: len, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .pattern(let regex, let flags):
            return evaluatePattern(regex, flags: flags, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .languageIn(let langs):
            return evaluateLanguageIn(langs, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .uniqueLang:
            return evaluateUniqueLang(focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.5 Property Pair Constraints
        case .equals(let otherPath):
            return try await evaluatePropertyPair(.equals, otherPath: otherPath, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .disjoint(let otherPath):
            return try await evaluatePropertyPair(.disjoint, otherPath: otherPath, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .lessThan(let otherPath):
            return try await evaluatePropertyPair(.lessThan, otherPath: otherPath, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .lessThanOrEquals(let otherPath):
            return try await evaluatePropertyPair(.lessThanOrEquals, otherPath: otherPath, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.6 Logical Constraints
        case .not(let shape):
            return try await evaluateNot(shape, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape, validator: validator)
        case .and(let shapes):
            return try await evaluateAnd(shapes, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape, validator: validator)
        case .or(let shapes):
            return try await evaluateOr(shapes, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape, validator: validator)
        case .xone(let shapes):
            return try await evaluateXone(shapes, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape, validator: validator)

        // §4.7 Shape-based Constraints
        case .node(let nodeShape):
            return try await evaluateNodeConstraint(nodeShape, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape, validator: validator)
        case .qualifiedValueShape(let shape, let min, let max):
            return try await evaluateQualifiedValueShape(shape, min: min, max: max, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape, validator: validator)

        // §4.8 Other Constraints
        case .closed(let ignoredProperties):
            return try await evaluateClosed(ignoredProperties: ignoredProperties, focusNode: focusNode, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .hasValue(let expected):
            return evaluateHasValue(expected, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .in_(let allowedValues):
            return evaluateIn(allowedValues, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        }
    }

    // MARK: - §4.1 Value Type

    private func evaluateClass(
        _ classIRI: String,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            guard case .iri(let nodeIRI) = value else {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.class_(classIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not an IRI, cannot be instance of \(classIRI)", severity: severity))
                continue
            }

            let isInstance: Bool
            if let reasoner = reasoner {
                let result = reasoner.isInstanceOf(individual: nodeIRI, classExpr: .named(classIRI))
                isInstance = result.value
            } else {
                // Fallback: SPARQL rdf:type query
                let pattern = ExecutionPattern.basic([
                    ExecutionTriple(
                        subject: .value(.string(nodeIRI)),
                        predicate: .value(.string("rdf:type")),
                        object: .value(.string(classIRI))
                    )
                ])
                let (bindings, _) = try await executor.execute(pattern: pattern, limit: 1, offset: 0)
                isInstance = !bindings.isEmpty
            }

            if !isInstance {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.class_(classIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "\(nodeIRI) is not an instance of \(classIRI)", severity: severity))
            }
        }
        return results
    }

    private func evaluateDatatype(
        _ datatypeIRI: String,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            guard case .literal(let literal) = value else {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.datatype(datatypeIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not a literal", severity: severity))
                continue
            }
            // Check datatype matches
            if literal.datatype != datatypeIRI {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.datatype(datatypeIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Expected datatype \(datatypeIRI), got \(literal.datatype)", severity: severity))
                continue
            }
            // Validate lexical form via OWLDatatypeValidator
            if let error = datatypeValidator.validateLexicalForm(literal) {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.datatype(datatypeIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Invalid lexical form: \(error)", severity: severity))
            }
        }
        return results
    }

    private func evaluateNodeKind(
        _ kind: SHACLNodeKind,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            let matches: Bool
            switch (kind, value) {
            case (.iri, .iri): matches = true
            case (.literal, .literal): matches = true
            case (.blankNode, .blankNode): matches = true
            case (.blankNodeOrIRI, .iri), (.blankNodeOrIRI, .blankNode): matches = true
            case (.blankNodeOrLiteral, .blankNode), (.blankNodeOrLiteral, .literal): matches = true
            case (.iriOrLiteral, .iri), (.iriOrLiteral, .literal): matches = true
            default: matches = false
            }
            if !matches {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.nodeKind(kind).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not match node kind \(kind)", severity: severity))
            }
        }
        return results
    }

    // MARK: - §4.2 Cardinality

    private func evaluateMinCount(
        _ min: Int,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        if valueNodes.count < min {
            return [makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.minCount(min).componentIRI, sourceShape: sourceShape,
                message: messages.first ?? "Expected at least \(min) values, got \(valueNodes.count)", severity: severity)]
        }
        return []
    }

    private func evaluateMaxCount(
        _ max: Int,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        if valueNodes.count > max {
            return [makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.maxCount(max).componentIRI, sourceShape: sourceShape,
                message: messages.first ?? "Expected at most \(max) values, got \(valueNodes.count)", severity: severity)]
        }
        return []
    }

    // MARK: - §4.3 Value Range

    private enum RangeComparison {
        case minExclusive, maxExclusive, minInclusive, maxInclusive
    }

    private func evaluateValueRange(
        _ comparison: RangeComparison,
        bound: RDFTerm,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        guard case .literal(let boundLiteral) = bound else { return [] }

        var results: [SHACLValidationResult] = []
        let constraint: SHACLConstraint
        switch comparison {
        case .minExclusive: constraint = .minExclusive(bound)
        case .maxExclusive: constraint = .maxExclusive(bound)
        case .minInclusive: constraint = .minInclusive(bound)
        case .maxInclusive: constraint = .maxInclusive(bound)
        }

        let facet: XSDFacet
        switch comparison {
        case .minExclusive: facet = .minExclusive
        case .maxExclusive: facet = .maxExclusive
        case .minInclusive: facet = .minInclusive
        case .maxInclusive: facet = .maxInclusive
        }

        for value in valueNodes {
            guard case .literal(let literal) = value else {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: constraint.componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not a literal", severity: severity))
                continue
            }
            let restriction = FacetRestriction(facet: facet, value: boundLiteral)
            if let error = datatypeValidator.validateFacets(literal, facets: [restriction]) {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: constraint.componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "\(error)", severity: severity))
            }
        }
        return results
    }

    // MARK: - §4.4 String-based

    private func evaluateStringLength(
        min: Int?, max: Int?,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        let component: String
        if let min = min {
            component = SHACLConstraint.minLength(min).componentIRI
        } else if let max = max {
            component = SHACLConstraint.maxLength(max).componentIRI
        } else {
            return []
        }

        for value in valueNodes {
            let str: String
            switch value {
            case .literal(let lit): str = lit.lexicalForm
            case .iri(let iri): str = iri
            case .blankNode(let id): str = id
            }

            if let min = min, str.count < min {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: component, sourceShape: sourceShape,
                    message: messages.first ?? "String length \(str.count) < minimum \(min)", severity: severity))
            }
            if let max = max, str.count > max {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: component, sourceShape: sourceShape,
                    message: messages.first ?? "String length \(str.count) > maximum \(max)", severity: severity))
            }
        }
        return results
    }

    private func evaluatePattern(
        _ regex: String, flags: String?,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        var options: NSRegularExpression.Options = []
        if let flags = flags {
            if flags.contains("i") { options.insert(.caseInsensitive) }
            if flags.contains("m") { options.insert(.anchorsMatchLines) }
            if flags.contains("s") { options.insert(.dotMatchesLineSeparators) }
            if flags.contains("x") { options.insert(.allowCommentsAndWhitespace) }
        }

        guard let nsRegex = try? NSRegularExpression(pattern: regex, options: options) else {
            return []
        }

        for value in valueNodes {
            let str: String
            switch value {
            case .literal(let lit): str = lit.lexicalForm
            case .iri(let iri): str = iri
            case .blankNode(let id): str = id
            }

            let range = NSRange(str.startIndex..<str.endIndex, in: str)
            if nsRegex.firstMatch(in: str, range: range) == nil {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.pattern(regex, flags: flags).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not match pattern \(regex)", severity: severity))
            }
        }
        return results
    }

    private func evaluateLanguageIn(
        _ langs: [String],
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            guard case .literal(let literal) = value else {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.languageIn(langs).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not a literal", severity: severity))
                continue
            }
            guard let lang = literal.language else {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.languageIn(langs).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Literal has no language tag", severity: severity))
                continue
            }
            let matches = langs.contains { allowed in
                lang.lowercased() == allowed.lowercased() ||
                lang.lowercased().hasPrefix(allowed.lowercased() + "-")
            }
            if !matches {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.languageIn(langs).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Language tag '\(lang)' not in \(langs)", severity: severity))
            }
        }
        return results
    }

    private func evaluateUniqueLang(
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var seenLangs = Set<String>()
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            if case .literal(let literal) = value, let lang = literal.language {
                let normalized = lang.lowercased()
                if seenLangs.contains(normalized) {
                    results.append(makeResult(focusNode: focusNode, path: path, value: value,
                        component: SHACLConstraint.uniqueLang.componentIRI, sourceShape: sourceShape,
                        message: messages.first ?? "Duplicate language tag: \(lang)", severity: severity))
                }
                seenLangs.insert(normalized)
            }
        }
        return results
    }

    // MARK: - §4.5 Property Pair

    private enum PairComparison {
        case equals, disjoint, lessThan, lessThanOrEquals
    }

    private func evaluatePropertyPair(
        _ comparison: PairComparison,
        otherPath: SHACLPath,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) async throws -> [SHACLValidationResult] {
        // Collect values from the other path
        let otherValues = try await collectValueNodes(from: focusNode, path: otherPath)
        let otherFieldValues = otherValues.compactMap { $0.toFieldValue() }
        let myFieldValues = valueNodes.compactMap { $0.toFieldValue() }

        let constraint: SHACLConstraint
        switch comparison {
        case .equals: constraint = .equals(otherPath)
        case .disjoint: constraint = .disjoint(otherPath)
        case .lessThan: constraint = .lessThan(otherPath)
        case .lessThanOrEquals: constraint = .lessThanOrEquals(otherPath)
        }

        var results: [SHACLValidationResult] = []
        switch comparison {
        case .equals:
            let mySet = Set(myFieldValues)
            let otherSet = Set(otherFieldValues)
            if mySet != otherSet {
                results.append(makeResult(focusNode: focusNode, path: path, value: nil,
                    component: constraint.componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value sets are not equal", severity: severity))
            }
        case .disjoint:
            for val in myFieldValues {
                if otherFieldValues.contains(val) {
                    results.append(makeResult(focusNode: focusNode, path: path,
                        value: fieldValueToRDFTerm(val),
                        component: constraint.componentIRI, sourceShape: sourceShape,
                        message: messages.first ?? "Value \(val) appears in both property sets", severity: severity))
                }
            }
        case .lessThan:
            for myVal in myFieldValues {
                for otherVal in otherFieldValues {
                    if myVal >= otherVal {
                        results.append(makeResult(focusNode: focusNode, path: path,
                            value: fieldValueToRDFTerm(myVal),
                            component: constraint.componentIRI, sourceShape: sourceShape,
                            message: messages.first ?? "Value \(myVal) is not less than \(otherVal)", severity: severity))
                    }
                }
            }
        case .lessThanOrEquals:
            for myVal in myFieldValues {
                for otherVal in otherFieldValues {
                    if myVal > otherVal {
                        results.append(makeResult(focusNode: focusNode, path: path,
                            value: fieldValueToRDFTerm(myVal),
                            component: constraint.componentIRI, sourceShape: sourceShape,
                            message: messages.first ?? "Value \(myVal) is not <= \(otherVal)", severity: severity))
                    }
                }
            }
        }
        return results
    }

    // MARK: - §4.6 Logical

    /// Evaluate whether a value node conforms to a shape.
    /// For IRI nodes: delegates to validator.validateNode() (graph-based lookup).
    /// For literal/blank node values: evaluates the shape's constraints directly.
    /// Returns empty array if the value conforms, non-empty if violations found.
    /// Reference: W3C SHACL §4.6, §4.7
    private func evaluateValueConformance(
        value: RDFTerm,
        against shape: SHACLShape,
        focusNode: String,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        if case .iri(let nodeIRI) = value {
            return try await validator.validateNode(nodeIRI, against: shape)
        }

        // For literal/blank node values: evaluate constraints directly
        var results: [SHACLValidationResult] = []

        switch shape {
        case .node(let ns):
            // 1. Evaluate node-level constraints
            for constraint in ns.constraints {
                let constraintResults = try await evaluate(
                    constraint: constraint,
                    focusNode: focusNode,
                    valueNodes: [value],
                    path: nil,
                    severity: ns.severity,
                    messages: ns.messages,
                    sourceShape: ns.iri,
                    validator: validator
                )
                results.append(contentsOf: constraintResults)
            }
            // 2. Evaluate property shape constraints (literals have no properties → empty value nodes)
            for ps in ns.propertyShapes {
                let emptyValueNodes: [RDFTerm] = []
                for constraint in ps.constraints {
                    let constraintResults = try await evaluate(
                        constraint: constraint,
                        focusNode: focusNode,
                        valueNodes: emptyValueNodes,
                        path: ps.path,
                        severity: ps.severity,
                        messages: ps.messages,
                        sourceShape: ps.iri,
                        validator: validator
                    )
                    results.append(contentsOf: constraintResults)
                }
            }
        case .property(let ps):
            for constraint in ps.constraints {
                let constraintResults = try await evaluate(
                    constraint: constraint,
                    focusNode: focusNode,
                    valueNodes: [value],
                    path: ps.path,
                    severity: ps.severity,
                    messages: ps.messages,
                    sourceShape: ps.iri,
                    validator: validator
                )
                results.append(contentsOf: constraintResults)
            }
        }

        return results
    }

    private func evaluateNot(
        _ shape: SHACLShape,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            let innerResults = try await evaluateValueConformance(
                value: value, against: shape, focusNode: focusNode, validator: validator
            )
            // sh:not — conformance means FAILURE
            if innerResults.isEmpty {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.not(shape).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value conforms to negated shape", severity: severity))
            }
        }
        return results
    }

    private func evaluateAnd(
        _ shapes: [SHACLShape],
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            for shape in shapes {
                let innerResults = try await evaluateValueConformance(
                    value: value, against: shape, focusNode: focusNode, validator: validator
                )
                if !innerResults.isEmpty {
                    results.append(makeResult(focusNode: focusNode, path: path, value: value,
                        component: SHACLConstraint.and(shapes).componentIRI, sourceShape: sourceShape,
                        message: messages.first ?? "Value does not conform to all shapes in sh:and", severity: severity))
                    break
                }
            }
        }
        return results
    }

    private func evaluateOr(
        _ shapes: [SHACLShape],
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            var anyConforms = false
            for shape in shapes {
                let innerResults = try await evaluateValueConformance(
                    value: value, against: shape, focusNode: focusNode, validator: validator
                )
                if innerResults.isEmpty {
                    anyConforms = true
                    break
                }
            }
            if !anyConforms {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.or(shapes).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not conform to any shape in sh:or", severity: severity))
            }
        }
        return results
    }

    private func evaluateXone(
        _ shapes: [SHACLShape],
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            var conformCount = 0
            for shape in shapes {
                let innerResults = try await evaluateValueConformance(
                    value: value, against: shape, focusNode: focusNode, validator: validator
                )
                if innerResults.isEmpty {
                    conformCount += 1
                }
            }
            if conformCount != 1 {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.xone(shapes).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value conforms to \(conformCount) shapes (expected exactly 1)", severity: severity))
            }
        }
        return results
    }

    // MARK: - §4.7 Shape-based

    private func evaluateNodeConstraint(
        _ nodeShape: NodeShape,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        for value in valueNodes {
            let innerResults = try await evaluateValueConformance(
                value: value, against: .node(nodeShape), focusNode: focusNode, validator: validator
            )
            if !innerResults.isEmpty {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.node(nodeShape).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not conform to node shape", severity: severity))
            }
        }
        return results
    }

    private func evaluateQualifiedValueShape(
        _ shape: SHACLShape,
        min: Int?, max: Int?,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?,
        validator: SHACLValidator
    ) async throws -> [SHACLValidationResult] {
        var conformingCount = 0
        for value in valueNodes {
            let innerResults = try await evaluateValueConformance(
                value: value, against: shape, focusNode: focusNode, validator: validator
            )
            if innerResults.isEmpty {
                conformingCount += 1
            }
        }

        var results: [SHACLValidationResult] = []
        if let min = min, conformingCount < min {
            results.append(makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.qualifiedValueShape(shape: shape, min: min, max: max).componentIRI,
                sourceShape: sourceShape,
                message: messages.first ?? "Qualified value count \(conformingCount) < minimum \(min)", severity: severity))
        }
        if let max = max, conformingCount > max {
            results.append(makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.qualifiedValueShape(shape: shape, min: min, max: max).componentIRI,
                sourceShape: sourceShape,
                message: messages.first ?? "Qualified value count \(conformingCount) > maximum \(max)", severity: severity))
        }
        return results
    }

    // MARK: - §4.8 Other

    private func evaluateClosed(
        ignoredProperties: [String],
        focusNode: String, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) async throws -> [SHACLValidationResult] {
        // Query all predicates from the focus node
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.string(focusNode)),
                predicate: .variable("?p"),
                object: .wildcard
            )
        ])
        let (bindings, _) = try await executor.execute(pattern: pattern, limit: nil, offset: 0)

        let ignoredSet = Set(ignoredProperties)
        var results: [SHACLValidationResult] = []
        for binding in bindings {
            if let pValue = binding["?p"], let predicate = pValue.stringValue {
                if !ignoredSet.contains(predicate) {
                    results.append(makeResult(focusNode: focusNode, path: .predicate(predicate), value: nil,
                        component: SHACLConstraint.closed(ignoredProperties: ignoredProperties).componentIRI,
                        sourceShape: sourceShape,
                        message: messages.first ?? "Unexpected property: \(predicate)", severity: severity))
                }
            }
        }
        return results
    }

    private func evaluateHasValue(
        _ expected: RDFTerm,
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        if !valueNodes.contains(expected) {
            return [makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.hasValue(expected).componentIRI, sourceShape: sourceShape,
                message: messages.first ?? "Expected value \(expected) not found", severity: severity)]
        }
        return []
    }

    private func evaluateIn(
        _ allowedValues: [RDFTerm],
        focusNode: String, valueNodes: [RDFTerm], path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: String?
    ) -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []
        let allowedSet = Set(allowedValues)
        for value in valueNodes {
            if !allowedSet.contains(value) {
                results.append(makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.in_(allowedValues).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value \(value) not in allowed set", severity: severity))
            }
        }
        return results
    }

    // MARK: - Value Node Collection

    /// Collect value nodes via a SHACL path from a focus node
    func collectValueNodes(
        from focusNode: String,
        path: SHACLPath
    ) async throws -> [RDFTerm] {
        let executionPath = path.toExecutionPropertyPath()
        let pattern = ExecutionPattern.propertyPath(
            subject: .value(.string(focusNode)),
            path: executionPath,
            object: .variable("?value")
        )

        let (bindings, _) = try await executor.execute(pattern: pattern, limit: nil, offset: 0)

        return bindings.compactMap { binding -> RDFTerm? in
            guard let fieldValue = binding["?value"] else { return nil }
            return fieldValueToRDFTerm(fieldValue)
        }
    }

    // MARK: - Helpers

    private func makeResult(
        focusNode: String,
        path: SHACLPath?,
        value: RDFTerm?,
        component: String,
        sourceShape: String?,
        message: String,
        severity: SHACLSeverity
    ) -> SHACLValidationResult {
        SHACLValidationResult(
            focusNode: focusNode,
            resultPath: path,
            value: value,
            sourceConstraintComponent: component,
            sourceShape: sourceShape,
            resultMessage: [message],
            resultSeverity: severity
        )
    }

    private func fieldValueToRDFTerm(_ fv: FieldValue) -> RDFTerm? {
        switch fv {
        case .string(let s):
            return RDFTerm.decode(s)
        case .int64(let i):
            return .integer(Int(i))
        case .double(let d):
            return .decimal(d)
        case .bool(let b):
            return .boolean(b)
        case .data, .null, .array:
            return nil
        }
    }
}

// MARK: - SHACLPath → ExecutionPropertyPath Conversion

extension SHACLPath {
    /// Convert to SPARQL execution property path
    func toExecutionPropertyPath() -> ExecutionPropertyPath {
        switch self {
        case .predicate(let iri):
            return .iri(iri)
        case .inverse(let inner):
            return .inverse(inner.toExecutionPropertyPath())
        case .sequence(let paths):
            guard let first = paths.first else { return .empty }
            return paths.dropFirst().reduce(first.toExecutionPropertyPath()) { acc, next in
                .sequence(acc, next.toExecutionPropertyPath())
            }
        case .alternative(let paths):
            guard let first = paths.first else { return .empty }
            return paths.dropFirst().reduce(first.toExecutionPropertyPath()) { acc, next in
                .alternative(acc, next.toExecutionPropertyPath())
            }
        case .zeroOrMore(let inner):
            return .zeroOrMore(inner.toExecutionPropertyPath())
        case .oneOrMore(let inner):
            return .oneOrMore(inner.toExecutionPropertyPath())
        case .zeroOrOne(let inner):
            return .zeroOrOne(inner.toExecutionPropertyPath())
        }
    }
}

// MARK: - RDFTerm → FieldValue Conversion

extension RDFTerm {
    /// Convert to FieldValue for comparison
    func toFieldValue() -> FieldValue? {
        switch self {
        case .iri(let s):
            return .string(s)
        case .literal(let lit):
            if let i = lit.intValue { return .int64(Int64(i)) }
            if let d = lit.doubleValue { return .double(d) }
            if let b = lit.boolValue { return .bool(b) }
            return .string(lit.lexicalForm)
        case .blankNode(let id):
            return .string("_:\(id)")
        }
    }
}
