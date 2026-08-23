// SHACLConstraintEvaluator.swift
// GraphIndex - Evaluate SHACL constraints against value nodes
//
// Delegates to existing components:
// - SPARQLValueComparator for datatype and value-range semantics
// - SPARQLQueryExecutor for cardinality and property pair constraints
// - SHACLEntailmentContext for selected graph reasoning
//
// Reference: W3C SHACL §4 (Core Constraint Components)
// https://www.w3.org/TR/shacl/#core-components

import DatabaseTypes
import StorageKit
import DatabaseKit
import DatabaseEngine
import OntologyIndex
/// Evaluates individual SHACL constraints against focus nodes and value nodes
public struct SHACLConstraintEvaluator: Sendable {

    private let executor: SPARQLQueryExecutor
    private let transaction: any TransactionReadAccess
    private let dataGraph: SHACLDataGraphTarget
    private let valueComparator: SPARQLValueComparator
    private let entailmentContext: (any SHACLEntailmentContext)?
    private let budget: SHACLValidationWorkBudget

    public init(
        executor: SPARQLQueryExecutor,
        transaction: any TransactionReadAccess,
        dataGraph: SHACLDataGraphTarget,
        xsdValidationLimits: XSDValidationLimits = .default,
        entailmentContext: (any SHACLEntailmentContext)? = nil,
        budget: SHACLValidationWorkBudget
    ) {
        self.executor = executor
        self.transaction = transaction
        self.dataGraph = dataGraph
        self.valueComparator = SPARQLValueComparator(
            limits: xsdValidationLimits
        )
        self.entailmentContext = entailmentContext
        self.budget = budget
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
        focusNode: RDFTerm,
        valueNodes: SHACLRetainedTerms,
        path: SHACLPath?,
        severity: SHACLSeverity,
        messages: [String],
        sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        try budget.consume(
            UInt64(max(1, valueNodes.count)),
            at: .bindingCandidate
        )
        switch constraint {
        // §4.1 Value Type Constraints
        case .class_(let classIRI):
            return try await evaluateClass(classIRI, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .datatype(let datatypeIRI):
            return try evaluateDatatype(datatypeIRI, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .nodeKind(let kind):
            return try evaluateNodeKind(kind, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.2 Cardinality Constraints
        case .minCount(let min):
            return try evaluateMinCount(min, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxCount(let max):
            return try evaluateMaxCount(max, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.3 Value Range Constraints
        case .minExclusive(let bound):
            return try evaluateValueRange(.minExclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxExclusive(let bound):
            return try evaluateValueRange(.maxExclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .minInclusive(let bound):
            return try evaluateValueRange(.minInclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxInclusive(let bound):
            return try evaluateValueRange(.maxInclusive, bound: bound, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

        // §4.4 String-based Constraints
        case .minLength(let len):
            return try evaluateStringLength(min: len, max: nil, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .maxLength(let len):
            return try evaluateStringLength(min: nil, max: len, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .pattern(let regex, let flags):
            return try evaluatePattern(regex, flags: flags, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .languageIn(let langs):
            return try evaluateLanguageIn(langs, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .uniqueLang:
            return try evaluateUniqueLang(focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)

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
            return try evaluateHasValue(expected, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        case .in_(let allowedValues):
            return try evaluateIn(allowedValues, focusNode: focusNode, valueNodes: valueNodes, path: path, severity: severity, messages: messages, sourceShape: sourceShape)
        }
    }

    // MARK: - §4.1 Value Type

    private func evaluateClass(
        _ classIRI: String,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) async throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            guard case .iri(let nodeIRI) = value else {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.class_(classIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not an IRI, cannot be instance of \(classIRI)", severity: severity))
                continue
            }

            let isInstance = try await isInstance(
                nodeIRI,
                of: classIRI
            )

            if !isInstance {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.class_(classIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "\(nodeIRI) is not an instance of \(classIRI)", severity: severity))
            }
        }
        return try results.finish()
    }

    private func isInstance(
        _ individual: RDFIRI,
        of classIRI: String
    ) async throws -> Bool {
        if let entailmentContext,
           entailmentContext.contains(.iri(individual), in: classIRI) {
            return true
        }

        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.rdfTerm(.iri(individual))),
                predicate: .value(
                    .rdfTerm(
                        .iri(
                            try RDFIRI(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                            )
                        )
                    )
                ),
                object: .variable("?type")
            )
        ])
        let bindings = try await executor.executeRetainedInTransaction(
            pattern: dataGraph.apply(to: pattern),
            transaction: transaction,
            limit: nil,
            offset: 0,
            workMeter: budget.workMeter
        )
        for index in 0..<bindings.count {
            let matches = try bindings.withBinding(
                at: index,
                workMeter: budget.workMeter
            ) { binding in
                guard let value = binding["?type"],
                      case .rdfTerm(.iri(let assertedType)) = value else {
                    return false
                }
                if assertedType.rawValue == classIRI { return true }
                return entailmentContext?.subsumes(
                    superClass: classIRI,
                    subClass: assertedType.rawValue
                ) ?? false
            }
            if matches {
                return true
            }
        }
        return false
    }

    private func evaluateDatatype(
        _ datatypeIRI: String,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            guard case .literal(let literal) = value else {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.datatype(datatypeIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not a literal", severity: severity))
                continue
            }
            // Check datatype matches
            if literal.datatypeIRI.rawValue != datatypeIRI {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.datatype(datatypeIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Expected datatype \(datatypeIRI), got \(literal.datatypeIRI.rawValue)", severity: severity))
                continue
            }
            if try !valueComparator.validateLexicalForm(literal) {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.datatype(datatypeIRI).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Literal is ill-typed for \(datatypeIRI)", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluateNodeKind(
        _ kind: SHACLNodeKind,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
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
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.nodeKind(kind).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not match node kind \(kind)", severity: severity))
            }
        }
        return try results.finish()
    }

    // MARK: - §4.2 Cardinality

    private func evaluateMinCount(
        _ min: Int,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        if valueNodes.count < min {
            return try SHACLRetainedValidationResults.retaining(
                makeResult(focusNode: focusNode, path: path, value: nil,
                    component: SHACLConstraint.minCount(min).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Expected at least \(min) values, got \(valueNodes.count)", severity: severity),
                workMeter: budget.workMeter
            )
        }
        return try SHACLRetainedValidationResults.empty(
            workMeter: budget.workMeter
        )
    }

    private func evaluateMaxCount(
        _ max: Int,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        if valueNodes.count > max {
            return try SHACLRetainedValidationResults.retaining(
                makeResult(focusNode: focusNode, path: path, value: nil,
                    component: SHACLConstraint.maxCount(max).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Expected at most \(max) values, got \(valueNodes.count)", severity: severity),
                workMeter: budget.workMeter
            )
        }
        return try SHACLRetainedValidationResults.empty(
            workMeter: budget.workMeter
        )
    }

    // MARK: - §4.3 Value Range

    private enum RangeComparison {
        case minExclusive, maxExclusive, minInclusive, maxInclusive
    }

    private func evaluateValueRange(
        _ comparison: RangeComparison,
        bound: RDFTerm,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        guard case .literal(let boundLiteral) = bound else {
            throw SHACLError.invalidConstraint(
                "A SHACL value-range bound must be an RDF literal"
            )
        }

        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        let constraint: SHACLConstraint
        switch comparison {
        case .minExclusive: constraint = .minExclusive(bound)
        case .maxExclusive: constraint = .maxExclusive(bound)
        case .minInclusive: constraint = .minInclusive(bound)
        case .maxInclusive: constraint = .maxInclusive(bound)
        }

        for value in valueNodes {
            guard case .literal(let literal) = value else {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: constraint.componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not a literal", severity: severity))
                continue
            }
            let order = try valueComparator.compare(boundLiteral, literal)
            let satisfies: Bool
            switch comparison {
            case .minExclusive:
                satisfies = order == .less
            case .minInclusive:
                satisfies = order == .less || order == .equal
            case .maxExclusive:
                satisfies = order == .greater
            case .maxInclusive:
                satisfies = order == .greater || order == .equal
            }
            if !satisfies {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: constraint.componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not satisfy the SPARQL range comparison", severity: severity))
            }
        }
        return try results.finish()
    }

    // MARK: - §4.4 String-based

    private func evaluateStringLength(
        min: Int?, max: Int?,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        let component: String
        if let min = min {
            component = SHACLConstraint.minLength(min).componentIRI
        } else if let max = max {
            component = SHACLConstraint.maxLength(max).componentIRI
        } else {
            return try SHACLRetainedValidationResults.empty(
                workMeter: budget.workMeter
            )
        }

        for value in valueNodes {
            let str: String
            switch value {
            case .literal(let lit): str = lit.lexicalForm
            case .iri(let iri): str = iri.rawValue
            case .blankNode(let id): str = id.rawValue
            case .tripleTerm: str = value.description
            }

            if let min = min, str.count < min {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: component, sourceShape: sourceShape,
                    message: messages.first ?? "String length \(str.count) < minimum \(min)", severity: severity))
            }
            if let max = max, str.count > max {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: component, sourceShape: sourceShape,
                    message: messages.first ?? "String length \(str.count) > maximum \(max)", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluatePattern(
        _ regex: String, flags: String?,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        let expression = try SHACLRegularExpression(
            pattern: regex,
            flags: flags
        )
        try budget.consume(
            UInt64(regex.utf8.count),
            at: .filterEvaluation
        )

        for value in valueNodes {
            let str: String
            switch value {
            case .literal(let lit): str = lit.lexicalForm
            case .iri(let iri): str = iri.rawValue
            case .blankNode(let id): str = id.rawValue
            case .tripleTerm: str = value.description
            }

            try budget.consume(
                UInt64(str.utf8.count),
                at: .filterEvaluation
            )
            let matches = try expression.matches(str)
            if !matches {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.pattern(regex, flags: flags).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not match pattern \(regex)", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluateLanguageIn(
        _ langs: [String],
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            guard case .literal(let literal) = value else {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.languageIn(langs).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value is not a literal", severity: severity))
                continue
            }
            guard let lang = literal.languageTag?.rawValue else {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.languageIn(langs).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Literal has no language tag", severity: severity))
                continue
            }
            let normalizedLanguage = lang.lowercased()
            var matches = false
            for allowed in langs {
                try budget.consume(at: .filterEvaluation)
                let normalizedAllowed = allowed.lowercased()
                if normalizedLanguage == normalizedAllowed ||
                    normalizedLanguage.hasPrefix(normalizedAllowed + "-") {
                    matches = true
                    break
                }
            }
            if !matches {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.languageIn(langs).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Language tag '\(lang)' not in \(langs)", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluateUniqueLang(
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var seenLangs = Set<String>()
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            if case .literal(let literal) = value,
               let lang = literal.languageTag?.rawValue {
                try budget.consume(at: .deduplication)
                let normalized = lang.lowercased()
                if seenLangs.contains(normalized) {
                    try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                        component: SHACLConstraint.uniqueLang.componentIRI, sourceShape: sourceShape,
                        message: messages.first ?? "Duplicate language tag: \(lang)", severity: severity))
                }
                seenLangs.insert(normalized)
            }
        }
        return try results.finish()
    }

    // MARK: - §4.5 Property Pair

    private enum PairComparison {
        case equals, disjoint, lessThan, lessThanOrEquals
    }

    private func evaluatePropertyPair(
        _ comparison: PairComparison,
        otherPath: SHACLPath,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) async throws -> SHACLRetainedValidationResults {
        // Collect values from the other path
        let otherValues = try await collectValueNodes(from: focusNode, path: otherPath)

        let constraint: SHACLConstraint
        switch comparison {
        case .equals: constraint = .equals(otherPath)
        case .disjoint: constraint = .disjoint(otherPath)
        case .lessThan: constraint = .lessThan(otherPath)
        case .lessThanOrEquals: constraint = .lessThanOrEquals(otherPath)
        }

        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        switch comparison {
        case .equals:
            try budget.consume(UInt64(valueNodes.count), at: .deduplication)
            try budget.consume(UInt64(otherValues.count), at: .deduplication)
            if !valueNodes.hasSameMembers(as: otherValues) {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: nil,
                    component: constraint.componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value sets are not equal", severity: severity))
            }
        case .disjoint:
            try budget.consume(UInt64(otherValues.count), at: .deduplication)
            for value in valueNodes {
                try budget.consume(at: .filterEvaluation)
                if otherValues.contains(value) {
                    try results.append(try makeResult(focusNode: focusNode, path: path,
                        value: value,
                        component: constraint.componentIRI, sourceShape: sourceShape,
                        message: messages.first ?? "Value \(value) appears in both property sets", severity: severity))
                }
            }
        case .lessThan:
            for value in valueNodes {
                for otherValue in otherValues {
                    try budget.consume(at: .joinCandidate)
                    let satisfies: Bool
                    if case .literal(let literal) = value,
                       case .literal(let otherLiteral) = otherValue {
                        satisfies = try valueComparator.compare(
                            literal,
                            otherLiteral
                        ) == .less
                    } else {
                        satisfies = false
                    }
                    if !satisfies {
                        try results.append(try makeResult(focusNode: focusNode, path: path,
                            value: value,
                            component: constraint.componentIRI, sourceShape: sourceShape,
                            message: messages.first ?? "Value \(value) is not less than \(otherValue)", severity: severity))
                    }
                }
            }
        case .lessThanOrEquals:
            for value in valueNodes {
                for otherValue in otherValues {
                    try budget.consume(at: .joinCandidate)
                    let satisfies: Bool
                    if case .literal(let literal) = value,
                       case .literal(let otherLiteral) = otherValue {
                        let order = try valueComparator.compare(
                            literal,
                            otherLiteral
                        )
                        satisfies = order == .less || order == .equal
                    } else {
                        satisfies = false
                    }
                    if !satisfies {
                        try results.append(try makeResult(focusNode: focusNode, path: path,
                            value: value,
                            component: constraint.componentIRI, sourceShape: sourceShape,
                            message: messages.first ?? "Value \(value) is not <= \(otherValue)", severity: severity))
                    }
                }
            }
        }
        return try results.finish()
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
        focusNode: RDFTerm,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        try await validator.validateNodeRetained(value, against: shape)
    }

    private func evaluateNot(
        _ shape: SHACLShape,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            try budget.consume(at: .bindingCandidate)
            let innerResults = try await evaluateValueConformance(
                value: value, against: shape, focusNode: focusNode, validator: validator
            )
            // sh:not — conformance means FAILURE
            if innerResults.isEmpty {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.not(shape).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value conforms to negated shape", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluateAnd(
        _ shapes: [SHACLShape],
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            for shape in shapes {
                try budget.consume(at: .joinCandidate)
                let innerResults = try await evaluateValueConformance(
                    value: value, against: shape, focusNode: focusNode, validator: validator
                )
                if !innerResults.isEmpty {
                    try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                        component: SHACLConstraint.and(shapes).componentIRI, sourceShape: sourceShape,
                        message: messages.first ?? "Value does not conform to all shapes in sh:and", severity: severity))
                    break
                }
            }
        }
        return try results.finish()
    }

    private func evaluateOr(
        _ shapes: [SHACLShape],
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            var anyConforms = false
            for shape in shapes {
                try budget.consume(at: .joinCandidate)
                let innerResults = try await evaluateValueConformance(
                    value: value, against: shape, focusNode: focusNode, validator: validator
                )
                if innerResults.isEmpty {
                    anyConforms = true
                    break
                }
            }
            if !anyConforms {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.or(shapes).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not conform to any shape in sh:or", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluateXone(
        _ shapes: [SHACLShape],
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            var conformCount = 0
            for shape in shapes {
                try budget.consume(at: .joinCandidate)
                let innerResults = try await evaluateValueConformance(
                    value: value, against: shape, focusNode: focusNode, validator: validator
                )
                if innerResults.isEmpty {
                    conformCount += 1
                }
            }
            if conformCount != 1 {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.xone(shapes).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value conforms to \(conformCount) shapes (expected exactly 1)", severity: severity))
            }
        }
        return try results.finish()
    }

    // MARK: - §4.7 Shape-based

    private func evaluateNodeConstraint(
        _ nodeShape: NodeShape,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for value in valueNodes {
            try budget.consume(at: .bindingCandidate)
            let innerResults = try await evaluateValueConformance(
                value: value, against: .node(nodeShape), focusNode: focusNode, validator: validator
            )
            if !innerResults.isEmpty {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.node(nodeShape).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value does not conform to node shape", severity: severity))
            }
        }
        return try results.finish()
    }

    private func evaluateQualifiedValueShape(
        _ shape: SHACLShape,
        min: Int?, max: Int?,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?,
        validator: SHACLValidator
    ) async throws -> SHACLRetainedValidationResults {
        var conformingCount = 0
        for value in valueNodes {
            try budget.consume(at: .bindingCandidate)
            let innerResults = try await evaluateValueConformance(
                value: value, against: shape, focusNode: focusNode, validator: validator
            )
            if innerResults.isEmpty {
                conformingCount += 1
            }
        }

        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        if let min = min, conformingCount < min {
            try results.append(try makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.qualifiedValueShape(shape: shape, min: min, max: max).componentIRI,
                sourceShape: sourceShape,
                message: messages.first ?? "Qualified value count \(conformingCount) < minimum \(min)", severity: severity))
        }
        if let max = max, conformingCount > max {
            try results.append(try makeResult(focusNode: focusNode, path: path, value: nil,
                component: SHACLConstraint.qualifiedValueShape(shape: shape, min: min, max: max).componentIRI,
                sourceShape: sourceShape,
                message: messages.first ?? "Qualified value count \(conformingCount) > maximum \(max)", severity: severity))
        }
        return try results.finish()
    }

    // MARK: - §4.8 Other

    private func evaluateClosed(
        ignoredProperties: [String],
        focusNode: RDFTerm, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) async throws -> SHACLRetainedValidationResults {
        // Query all predicates from the focus node
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .value(.rdfTerm(focusNode)),
                predicate: .variable("?p"),
                object: .wildcard
            )
        ])
        let bindings = try await executor.executeRetainedInTransaction(
            pattern: dataGraph.apply(to: pattern),
            transaction: transaction,
            limit: nil,
            offset: 0,
            workMeter: budget.workMeter
        )
        try budget.consume(
            UInt64(bindings.count),
            at: .resultMaterialization
        )

        try budget.consume(UInt64(ignoredProperties.count), at: .deduplication)
        let ignoredSet = Set(ignoredProperties)
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        for index in 0..<bindings.count {
            try budget.consume(at: .filterEvaluation)
            try bindings.withBinding(
                at: index,
                workMeter: budget.workMeter
            ) { binding in
                guard let pValue = binding["?p"],
                      case .rdfTerm(.iri(let predicate)) = pValue else {
                    return
                }
                if !ignoredSet.contains(predicate.rawValue) {
                    try results.append(try makeResult(focusNode: focusNode, path: .predicate(RDFPredicateIRI(predicate)), value: nil,
                        component: SHACLConstraint.closed(ignoredProperties: ignoredProperties).componentIRI,
                        sourceShape: sourceShape,
                        message: messages.first ?? "Unexpected property: \(predicate)", severity: severity))
                }
            }
        }
        return try results.finish()
    }

    private func evaluateHasValue(
        _ expected: RDFTerm,
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var found = false
        for value in valueNodes {
            try budget.consume(at: .filterEvaluation)
            if value == expected {
                found = true
                break
            }
        }
        if !found {
            return try SHACLRetainedValidationResults.retaining(
                makeResult(focusNode: focusNode, path: path, value: nil,
                    component: SHACLConstraint.hasValue(expected).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Expected value \(expected) not found", severity: severity),
                workMeter: budget.workMeter
            )
        }
        return try SHACLRetainedValidationResults.empty(
            workMeter: budget.workMeter
        )
    }

    private func evaluateIn(
        _ allowedValues: [RDFTerm],
        focusNode: RDFTerm, valueNodes: SHACLRetainedTerms, path: SHACLPath?,
        severity: SHACLSeverity, messages: [String], sourceShape: RDFTerm?
    ) throws -> SHACLRetainedValidationResults {
        var results = try SHACLValidationResultLeafBuilder(workMeter: budget.workMeter)
        try budget.consume(UInt64(allowedValues.count), at: .deduplication)
        let allowedSet = Set(allowedValues)
        for value in valueNodes {
            try budget.consume(at: .filterEvaluation)
            if !allowedSet.contains(value) {
                try results.append(try makeResult(focusNode: focusNode, path: path, value: value,
                    component: SHACLConstraint.in_(allowedValues).componentIRI, sourceShape: sourceShape,
                    message: messages.first ?? "Value \(value) not in allowed set", severity: severity))
            }
        }
        return try results.finish()
    }

    // MARK: - Value Node Collection

    /// Collect value nodes via a SHACL path from a focus node
    func collectValueNodes(
        from focusNode: RDFTerm,
        path: SHACLPath
    ) async throws -> SHACLRetainedTerms {
        let executionPath = try path.toExecutionPropertyPath()
        let pattern = ExecutionPattern.propertyPath(
            subject: .value(.rdfTerm(focusNode)),
            path: executionPath,
            object: .variable("?value")
        )

        let bindings = try await executor.executeRetainedInTransaction(
            pattern: dataGraph.apply(to: pattern),
            transaction: transaction,
            limit: nil,
            offset: 0,
            workMeter: budget.workMeter
        )
        try budget.consume(
            UInt64(bindings.count),
            at: .resultMaterialization
        )

        var values = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter,
            expectedCount: bindings.count
        )
        for index in 0..<bindings.count {
            try bindings.withBinding(
                at: index,
                workMeter: budget.workMeter
            ) { binding in
                guard let fieldValue = binding["?value"] else {
                    throw SHACLError.resultBindingMissing(variable: "?value")
                }
                guard case .rdfTerm(let term) = fieldValue else {
                    throw SHACLError.resultBindingTypeMismatch(variable: "?value")
                }
                try values.insert(term)
            }
        }
        return try values.finish()
    }

    // MARK: - Helpers

    private func makeResult(
        focusNode: RDFTerm,
        path: SHACLPath?,
        value: RDFTerm?,
        component: String,
        sourceShape: RDFTerm?,
        message: String,
        severity: SHACLSeverity
    ) throws -> SHACLValidationResult {
        try budget.consume(at: .resultMaterialization)
        return SHACLValidationResult(
            focusNode: focusNode,
            resultPath: path,
            value: value,
            sourceConstraintComponent: component,
            sourceShape: sourceShape,
            resultMessage: [message],
            resultSeverity: severity
        )
    }

}

// MARK: - SHACLPath → ExecutionPropertyPath Conversion

extension SHACLPath {
    /// Convert to SPARQL execution property path
    func toExecutionPropertyPath() throws -> ExecutionPropertyPath {
        switch self {
        case .predicate(let iri):
            return .iri(iri)
        case .inverse(let inner):
            return .inverse(try inner.toExecutionPropertyPath())
        case .sequence(let paths):
            guard let first = paths.elements.first else { return .empty }
            var result = try first.toExecutionPropertyPath()
            for path in paths.elements.dropFirst() {
                result = .sequence(
                    result,
                    try path.toExecutionPropertyPath()
                )
            }
            return result
        case .alternative(let paths):
            guard let first = paths.elements.first else { return .empty }
            var result = try first.toExecutionPropertyPath()
            for path in paths.elements.dropFirst() {
                result = .alternative(
                    result,
                    try path.toExecutionPropertyPath()
                )
            }
            return result
        case .zeroOrMore(let inner):
            return .zeroOrMore(try inner.toExecutionPropertyPath())
        case .oneOrMore(let inner):
            return .oneOrMore(try inner.toExecutionPropertyPath())
        case .zeroOrOne(let inner):
            return .zeroOrOne(try inner.toExecutionPropertyPath())
        }
    }
}
