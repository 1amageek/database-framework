// FDBContext+SHACL.swift
// GraphIndex - FDBContext extension for SHACL validation
//
// Provides high-level API for SHACL shapes management and validation.
// Follows the OntologyContextAPI pattern (FDBContext+Ontology.swift).
//
// Reference: W3C SHACL https://www.w3.org/TR/shacl/

import Foundation
import FoundationDB
import Graph
import Core
import DatabaseEngine

// MARK: - FDBContext Extension

extension FDBContext {
    /// Access SHACL validation API
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Load a shapes graph
    /// try await context.shacl.loadShapes(shapesGraph)
    ///
    /// // Validate data graph against shapes
    /// let report = try await context.shacl.validate(
    ///     Statement.self,
    ///     against: "ex:PersonShapes"
    /// )
    ///
    /// if !report.conforms {
    ///     for violation in report.violations {
    ///         print("\(violation.focusNode): \(violation.resultMessage)")
    ///     }
    /// }
    ///
    /// // List shapes graphs
    /// let graphs = try await context.shacl.listShapesGraphs()
    /// ```
    ///
    /// - Returns: SHACLContextAPI for SHACL operations
    public var shacl: SHACLContextAPI {
        SHACLContextAPI(context: self)
    }
}

// MARK: - SHACLContextAPI

/// High-level API for SHACL validation operations
///
/// Provides CRUD operations for shapes graphs and validation
/// of data graphs against shapes graphs.
///
/// All operations are performed within transactions managed by the context.
public struct SHACLContextAPI: Sendable {

    private let context: FDBContext

    /// SHACL subspace key prefix
    private static let shaclPrefix: [UInt8] = Array("S".utf8)

    internal init(context: FDBContext) {
        self.context = context
    }

    // MARK: - Store Access

    private func store() -> SHACLShapesStore {
        let baseSubspace = Subspace(prefix: Self.shaclPrefix)
        return SHACLShapesStore(subspace: baseSubspace)
    }

    // MARK: - Load Operations

    /// Load a SHACL shapes graph into the store
    ///
    /// If a shapes graph with the same IRI already exists, it will be replaced.
    ///
    /// - Parameter graph: The SHACL shapes graph to load
    ///
    /// **Example**:
    /// ```swift
    /// let shapesGraph = SHACLShapesGraph(
    ///     iri: "ex:PersonShapes",
    ///     shapes: [.node(personShape)],
    ///     prefixes: .standard
    /// )
    /// try await context.shacl.loadShapes(shapesGraph)
    /// ```
    public func loadShapes(_ graph: SHACLShapesGraph) async throws {
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            // Delete existing if present
            store.delete(iri: graph.iri, transaction: transaction)
            // Save new shapes graph
            try store.save(graph, transaction: transaction)
        }
    }

    // MARK: - Validation

    /// Validate a data graph against a shapes graph
    ///
    /// Executes the W3C SHACL §3.4 validation algorithm:
    /// 1. Load shapes graph from the store
    /// 2. For each active shape, resolve targets to focus nodes
    /// 3. Evaluate constraints against focus nodes and their value nodes
    /// 4. Return validation report
    ///
    /// - Parameters:
    ///   - type: The Persistable type that holds the graph index
    ///   - shapesGraphIRI: IRI of the shapes graph to validate against
    ///   - entailment: Entailment regime (default: .none)
    ///   - ontologyIRI: Ontology IRI for OWL entailment (required when entailment is .owl)
    /// - Returns: SHACL validation report
    ///
    /// **Example**:
    /// ```swift
    /// let report = try await context.shacl.validate(
    ///     Statement.self,
    ///     against: "ex:PersonShapes"
    /// )
    /// if report.conforms {
    ///     print("Data graph conforms to all shapes")
    /// }
    /// ```
    public func validate<T: Persistable>(
        _ type: T.Type,
        against shapesGraphIRI: String,
        entailment: SHACLEntailment = .none,
        ontologyIRI: String? = nil
    ) async throws -> SHACLValidationReport {
        // Load shapes graph
        guard let shapesGraph = try await getShapesGraph(iri: shapesGraphIRI) else {
            throw SHACLError.shapesGraphNotFound(shapesGraphIRI)
        }

        // Build SPARQLQueryExecutor for the type's graph index
        let executor = try await buildExecutor(for: type)

        // Build OWL reasoner if entailment requires it
        let reasoner: OWLReasoner?
        if entailment == .owl, let ontIRI = ontologyIRI {
            guard let ontology = try await context.ontology.get(iri: ontIRI) else {
                throw SHACLError.ontologyNotFound(ontIRI)
            }
            reasoner = OWLReasoner(ontology: ontology)
        } else {
            reasoner = nil
        }

        // Construct validation components
        let targetResolver = SHACLTargetResolver(executor: executor)
        let constraintEvaluator = SHACLConstraintEvaluator(
            executor: executor,
            reasoner: reasoner
        )
        let validator = SHACLValidator(
            shapesGraph: shapesGraph,
            targetResolver: targetResolver,
            constraintEvaluator: constraintEvaluator
        )

        return try await validator.validate()
    }

    /// Validate a specific node against a specific shape
    ///
    /// - Parameters:
    ///   - type: The Persistable type that holds the graph index
    ///   - nodeIRI: The node IRI to validate
    ///   - shapeIRI: The shape IRI to validate against
    ///   - shapesGraphIRI: The shapes graph containing the shape
    /// - Returns: SHACL validation report for the specific node
    ///
    /// **Example**:
    /// ```swift
    /// let report = try await context.shacl.validateNode(
    ///     Statement.self,
    ///     nodeIRI: "ex:Alice",
    ///     against: "ex:PersonShape",
    ///     in: "ex:PersonShapes"
    /// )
    /// ```
    public func validateNode<T: Persistable>(
        _ type: T.Type,
        nodeIRI: String,
        against shapeIRI: String,
        in shapesGraphIRI: String
    ) async throws -> SHACLValidationReport {
        guard let shapesGraph = try await getShapesGraph(iri: shapesGraphIRI) else {
            throw SHACLError.shapesGraphNotFound(shapesGraphIRI)
        }

        guard let shape = shapesGraph.findShape(iri: shapeIRI) else {
            throw SHACLError.shapeNotFound(shapeIRI)
        }

        let executor = try await buildExecutor(for: type)
        let targetResolver = SHACLTargetResolver(executor: executor)
        let constraintEvaluator = SHACLConstraintEvaluator(executor: executor)
        let validator = SHACLValidator(
            shapesGraph: shapesGraph,
            targetResolver: targetResolver,
            constraintEvaluator: constraintEvaluator
        )

        let results = try await validator.validateNode(nodeIRI, against: shape)
        return SHACLValidationReport(results: results)
    }

    // MARK: - Shapes Graph CRUD

    /// List all shapes graph IRIs
    ///
    /// - Returns: Array of shapes graph IRIs
    public func listShapesGraphs() async throws -> [String] {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.listGraphIRIs(transaction: transaction)
        }
    }

    /// Get a shapes graph by IRI
    ///
    /// - Parameter iri: The shapes graph IRI
    /// - Returns: The shapes graph, or nil if not found
    public func getShapesGraph(iri: String) async throws -> SHACLShapesGraph? {
        let store = store()
        return try await context.indexQueryContext.withTransaction { transaction in
            try await store.get(iri: iri, transaction: transaction)
        }
    }

    /// Delete a shapes graph by IRI
    ///
    /// - Parameter iri: The shapes graph IRI to delete
    public func deleteShapesGraph(iri: String) async throws {
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            store.delete(iri: iri, transaction: transaction)
        }
    }

    /// Delete all shapes graphs
    public func deleteAllShapesGraphs() async throws {
        let store = store()
        try await context.indexQueryContext.withTransaction { transaction in
            store.deleteAll(transaction: transaction)
        }
    }

    // MARK: - Private

    /// Build a SPARQLQueryExecutor for the given Persistable type's graph index
    private func buildExecutor<T: Persistable>(
        for type: T.Type
    ) async throws -> SPARQLQueryExecutor {
        guard let descriptor = T.indexDescriptors.first(where: {
            $0.kindIdentifier == GraphIndexKind<T>.identifier
        }), let kind = descriptor.kind as? GraphIndexKind<T> else {
            throw SHACLError.graphIndexNotFound(String(describing: T.self))
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(descriptor.name)

        return SPARQLQueryExecutor(
            database: context.container.database,
            indexSubspace: indexSubspace,
            strategy: kind.strategy,
            fromFieldName: kind.fromField,
            edgeFieldName: kind.edgeField,
            toFieldName: kind.toField,
            graphFieldName: kind.graphField,
            storedFieldNames: descriptor.storedFieldNames
        )
    }
}

// MARK: - SHACLError

/// Errors for SHACL operations
public enum SHACLError: Error, CustomStringConvertible {
    /// Shapes graph not found
    case shapesGraphNotFound(String)

    /// Shape not found in shapes graph
    case shapeNotFound(String)

    /// Ontology not found (required for OWL entailment)
    case ontologyNotFound(String)

    /// Graph index not configured on the Persistable type
    case graphIndexNotFound(String)

    public var description: String {
        switch self {
        case .shapesGraphNotFound(let iri):
            return "SHACL shapes graph not found: \(iri)"
        case .shapeNotFound(let iri):
            return "SHACL shape not found: \(iri)"
        case .ontologyNotFound(let iri):
            return "Ontology not found (required for OWL entailment): \(iri)"
        case .graphIndexNotFound(let typeName):
            return "GraphIndexKind not configured on type \(typeName)"
        }
    }
}
