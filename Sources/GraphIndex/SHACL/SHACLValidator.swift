// SHACLValidator.swift
// GraphIndex - Main SHACL validation orchestrator
//
// Coordinates target resolution and constraint evaluation
// to validate a data graph against a shapes graph.
//
// Reference: W3C SHACL §3 (Validation)
// https://www.w3.org/TR/shacl/#validation

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import DatabaseKit
import DatabaseEngine

/// SHACL Validation Engine
///
/// Orchestrates the validation of a data graph against a shapes graph
/// following the W3C SHACL §3.4 algorithm:
///
/// 1. For each active shape, resolve targets to focus nodes
/// 2. For each focus node, evaluate node constraints
/// 3. For each property shape, collect value nodes via path evaluation
/// 4. Evaluate property constraints against value nodes
/// 5. Collect all results into a validation report
///
/// Reference: W3C SHACL §3.4
public struct SHACLValidator: Sendable {

    private let shapesGraph: SHACLShapesGraph
    private let targetResolver: SHACLTargetResolver
    private let constraintEvaluator: SHACLConstraintEvaluator
    private let budget: SHACLValidationWorkBudget

    public init(
        shapesGraph: SHACLShapesGraph,
        targetResolver: SHACLTargetResolver,
        constraintEvaluator: SHACLConstraintEvaluator,
        budget: SHACLValidationWorkBudget
    ) {
        self.shapesGraph = shapesGraph
        self.targetResolver = targetResolver
        self.constraintEvaluator = constraintEvaluator
        self.budget = budget
    }

    // MARK: - Full Validation

    /// Validate the entire data graph against the shapes graph
    ///
    /// Implements W3C SHACL §3.4 validation algorithm:
    /// ```
    /// for each shape S in shapesGraph where !S.deactivated:
    ///     focusNodes = resolve(S.targets)
    ///     for each focusNode in focusNodes:
    ///         results += evaluateShape(S, focusNode)
    /// ```
    ///
    /// - Returns: Validation report with all results
    public func validate() async throws -> SHACLValidationReport {
        var allResults: [SHACLValidationResult] = []

        for shape in shapesGraph.activeShapes {
            try budget.consume(at: .projection)
            let results = try await validateShape(shape)
            allResults.append(contentsOf: results)
        }

        return SHACLValidationReport(results: allResults)
    }

    /// Validate explicit focus nodes against every active shape.
    public func validate(
        focusNodes: [RDFTerm]
    ) async throws -> SHACLValidationReport {
        let canonicalFocusNodes = Array(Set(focusNodes)).sorted()
        var allResults: [SHACLValidationResult] = []
        for shape in shapesGraph.activeShapes {
            for focusNode in canonicalFocusNodes {
                try budget.consume(at: .projection)
                let results = try await evaluateShapeOnFocusNode(
                    shape,
                    focusNode: focusNode
                )
                allResults.append(contentsOf: results)
            }
        }
        return SHACLValidationReport(results: allResults)
    }

    // MARK: - Shape-Level Validation

    /// Validate a single shape against all its target focus nodes
    private func validateShape(_ shape: SHACLShape) async throws -> [SHACLValidationResult] {
        let focusNodes = try await targetResolver.resolve(
            shape.targets,
            shapeIdentifier: shape.identifier
        )

        var results: [SHACLValidationResult] = []
        for focusNode in focusNodes {
            try budget.consume(at: .projection)
            let nodeResults = try await evaluateShapeOnFocusNode(shape, focusNode: focusNode)
            results.append(contentsOf: nodeResults)
        }
        return results
    }

    /// Evaluate a shape against a single focus node
    private func evaluateShapeOnFocusNode(
        _ shape: SHACLShape,
        focusNode: RDFTerm
    ) async throws -> [SHACLValidationResult] {
        switch shape {
        case .node(let nodeShape):
            return try await evaluateNodeShape(nodeShape, focusNode: focusNode)
        case .property(let propertyShape):
            return try await evaluatePropertyShape(propertyShape, focusNode: focusNode)
        }
    }

    // MARK: - NodeShape Evaluation

    /// Evaluate a node shape against a focus node
    ///
    /// 1. Evaluate node-level constraints against the focus node itself
    /// 2. Evaluate each property shape
    ///
    /// For `sh:closed`, the allowed properties are automatically augmented
    /// with the predicate IRIs from the shape's `sh:property` declarations
    /// (W3C SHACL §4.8.1).
    private func evaluateNodeShape(
        _ nodeShape: NodeShape,
        focusNode: RDFTerm
    ) async throws -> [SHACLValidationResult] {
        var results: [SHACLValidationResult] = []

        // Collect declared property IRIs for sh:closed augmentation (§4.8.1)
        let declaredPropertyIRIs = nodeShape.propertyShapes.compactMap {
            $0.path.predicateIRI?.rawValue
        }

        // Evaluate node-level constraints (focus node is the value node)
        let focusAsValue: [RDFTerm] = [focusNode]
        for constraint in nodeShape.constraints {
            try budget.consume(at: .projection)
            // Augment sh:closed with declared property paths (§4.8.1)
            let effectiveConstraint: SHACLConstraint
            if case .closed(let ignoredProperties) = constraint {
                let allAllowed = ignoredProperties + declaredPropertyIRIs
                effectiveConstraint = .closed(ignoredProperties: allAllowed)
            } else {
                effectiveConstraint = constraint
            }

            let constraintResults = try await constraintEvaluator.evaluate(
                constraint: effectiveConstraint,
                focusNode: focusNode,
                valueNodes: focusAsValue,
                path: nil,
                severity: nodeShape.severity,
                messages: nodeShape.messages,
                sourceShape: nodeShape.identifier,
                validator: self
            )
            results.append(contentsOf: constraintResults)
        }

        // Evaluate property shapes
        for propertyShape in nodeShape.propertyShapes {
            try budget.consume(at: .projection)
            if propertyShape.deactivated { continue }
            let propResults = try await evaluatePropertyShape(
                propertyShape,
                focusNode: focusNode
            )
            results.append(contentsOf: propResults)
        }

        return results
    }

    // MARK: - PropertyShape Evaluation

    /// Evaluate a property shape against a focus node
    ///
    /// 1. Collect value nodes via path evaluation
    /// 2. Evaluate constraints against value nodes
    /// 3. Evaluate nested property shapes
    private func evaluatePropertyShape(
        _ propertyShape: PropertyShape,
        focusNode: RDFTerm
    ) async throws -> [SHACLValidationResult] {
        // Collect value nodes via SPARQL property path
        let valueNodes = try await constraintEvaluator.collectValueNodes(
            from: focusNode,
            path: propertyShape.path
        )

        var results: [SHACLValidationResult] = []

        // Evaluate constraints against value nodes
        for constraint in propertyShape.constraints {
            try budget.consume(at: .projection)
            let constraintResults = try await constraintEvaluator.evaluate(
                constraint: constraint,
                focusNode: focusNode,
                valueNodes: valueNodes,
                path: propertyShape.path,
                severity: propertyShape.severity,
                messages: propertyShape.messages,
                sourceShape: propertyShape.identifier,
                validator: self
            )
            results.append(contentsOf: constraintResults)
        }

        // Evaluate nested property shapes
        for nestedShape in propertyShape.propertyShapes {
            try budget.consume(at: .projection)
            if nestedShape.deactivated { continue }
            // For nested property shapes, each value node becomes a focus node
            for value in valueNodes {
                try budget.consume(at: .projection)
                let nestedResults = try await evaluatePropertyShape(
                    nestedShape,
                    focusNode: value
                )
                results.append(contentsOf: nestedResults)
            }
        }

        return results
    }

    // MARK: - Single Node Validation

    /// Validate a specific node against a specific shape
    ///
    /// Used for recursive validation (sh:not, sh:and, sh:or, sh:xone, sh:node)
    /// and for targeted validation of individual nodes.
    ///
    /// - Parameters:
    ///   - nodeIRI: The node IRI to validate
    ///   - shape: The shape to validate against
    /// - Returns: Array of validation results (empty if the node conforms)
    public func validateNode(
        _ node: RDFTerm,
        against shape: SHACLShape
    ) async throws -> [SHACLValidationResult] {
        try await evaluateShapeOnFocusNode(shape, focusNode: node)
    }
}
