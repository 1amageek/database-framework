// DatabaseContext+SHACL.swift
// GraphIndex - DatabaseContext extension for SHACL validation
//
// Provides high-level API for SHACL shapes management and validation.
// Follows the OntologyContextAPI pattern (DatabaseContext+Ontology.swift).
//
// Reference: W3C SHACL https://www.w3.org/TR/shacl/

@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import OntologyIndex
import StorageKit

// MARK: - DatabaseContext Extension

extension DatabaseContext {
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

    private let context: DatabaseContext

    /// SHACL subspace key prefix
    private static let shaclPrefix = ByteString(utf8: "S")

    internal init(context: DatabaseContext) {
        self.context = context
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
        try await context.indexQueryContext.withAuxiliaryWriteStorage(
            namespace: Self.shaclPrefix,
            requiredAccess: .write
        ) { subspace, transaction in
                let store = SHACLShapesStore(subspace: subspace)
                // Delete existing if present
                try store.delete(iri: graph.iri, transaction: transaction)
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
        ontologyIRI: String? = nil,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SHACLValidationReport {
        let workBudget = SHACLValidationWorkBudget(
            budget: budget,
            monotonicClock: context.container.monotonicClock
        )
        return try await context.indexQueryContext.withQuerySnapshot { snapshot in
            guard let retainedShapesGraph = try await getRetainedShapesGraph(
                iri: shapesGraphIRI,
                snapshot: snapshot,
                workMeter: workBudget.workMeter
            ) else {
                throw SHACLError.shapesGraphNotFound(shapesGraphIRI)
            }
            let shapesGraph = retainedShapesGraph.graph
            defer { retainedShapesGraph.reservation.release() }

            let configuredEntailmentContext: (any SHACLEntailmentContext)?
            let configuredOntologyContext: OntologyContext?
            let ontologyReservation: DatabaseIntermediateReservation?
            switch entailment {
            case .owl:
                guard let ontIRI = ontologyIRI else {
                    throw SHACLError.ontologyIdentifierRequired
                }
                guard let retainedOntology = try await getRetainedOntology(
                    iri: ontIRI,
                    snapshot: snapshot,
                    workMeter: workBudget.workMeter
                ) else {
                    throw SHACLError.ontologyNotFound(ontIRI)
                }
                let ontology = retainedOntology.ontology
                ontologyReservation = retainedOntology.reservation
                configuredEntailmentContext = OWLGraphEntailment(
                    reasoner: OWLReasoner(
                        ontology: ontology,
                        clock: context.container.monotonicClock
                    )
                )
                configuredOntologyContext = OntologyContext(
                    ontology: ontology
                )
            case .none, .rdfs:
                ontologyReservation = nil
                configuredEntailmentContext = nil
                configuredOntologyContext = nil
            }
            defer { ontologyReservation?.release() }
            return try await withExecutor(for: type, snapshot: snapshot) {
                executor, transaction in
            let entailmentContext: (any SHACLEntailmentContext)?
            let ontologyContext: OntologyContext?
            if entailment == .rdfs {
                let rdfs = try await RDFSGraphEntailment.resolve(
                    executor: executor,
                    dataGraph: .defaultGraph,
                    transaction: transaction,
                    budget: workBudget
                )
                entailmentContext = rdfs
                ontologyContext = rdfs.ontologyContext
            } else {
                entailmentContext = configuredEntailmentContext
                ontologyContext = configuredOntologyContext
            }
            let validationExecutor = executor.withOntology(
                ontologyContext
            )
            let targetResolver = SHACLTargetResolver(
                executor: validationExecutor,
                transaction: transaction,
                dataGraph: .defaultGraph,
                entailmentContext: entailmentContext,
                budget: workBudget
            )
            let constraintEvaluator = SHACLConstraintEvaluator(
                executor: validationExecutor,
                transaction: transaction,
                dataGraph: .defaultGraph,
                entailmentContext: entailmentContext,
                budget: workBudget
            )
            let validator = SHACLValidator(
                shapesGraph: shapesGraph,
                targetResolver: targetResolver,
                constraintEvaluator: constraintEvaluator,
                budget: workBudget
            )
            return try await validator.validate()
            }
        }
    }

    /// Validate a specific node against a specific shape
    ///
    /// - Parameters:
    ///   - type: The Persistable type that holds the graph index
    ///   - node: The RDF node to validate
    ///   - shapeIdentifier: The canonical RDF node identifying the shape
    ///   - shapesGraphIRI: The shapes graph containing the shape
    /// - Returns: SHACL validation report for the specific node
    ///
    /// **Example**:
    /// ```swift
    /// let report = try await context.shacl.validateNode(
    ///     Statement.self,
    ///     node: .iri("https://example.com/Alice"),
    ///     against: .iri("ex:PersonShape"),
    ///     in: "ex:PersonShapes"
    /// )
    /// ```
    public func validateNode<T: Persistable>(
        _ type: T.Type,
        node: RDFTerm,
        against shapeIdentifier: RDFTerm,
        in shapesGraphIRI: String,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> SHACLValidationReport {
        let workBudget = SHACLValidationWorkBudget(
            budget: budget,
            monotonicClock: context.container.monotonicClock
        )
        return try await context.indexQueryContext.withQuerySnapshot { snapshot in
            guard let retainedShapesGraph = try await getRetainedShapesGraph(
                iri: shapesGraphIRI,
                snapshot: snapshot,
                workMeter: workBudget.workMeter
            ) else {
                throw SHACLError.shapesGraphNotFound(shapesGraphIRI)
            }
            let shapesGraph = retainedShapesGraph.graph
            defer { retainedShapesGraph.reservation.release() }
            guard let shape = shapesGraph.findShape(
                identifier: shapeIdentifier
            ) else {
                throw SHACLError.shapeNotFound(shapeIdentifier)
            }
            return try await withExecutor(for: type, snapshot: snapshot) {
                executor, transaction in
            let targetResolver = SHACLTargetResolver(
                executor: executor,
                transaction: transaction,
                dataGraph: .defaultGraph,
                budget: workBudget
            )
            let constraintEvaluator = SHACLConstraintEvaluator(
                executor: executor,
                transaction: transaction,
                dataGraph: .defaultGraph,
                budget: workBudget
            )
            let validator = SHACLValidator(
                shapesGraph: shapesGraph,
                targetResolver: targetResolver,
                constraintEvaluator: constraintEvaluator,
                budget: workBudget
            )
            let results = try await validator.validateNode(
                node,
                against: shape
            )
            return SHACLValidationReport(results: results)
            }
        }
    }

    // MARK: - Shapes Graph CRUD

    /// List all shapes graph IRIs
    ///
    /// - Returns: Array of shapes graph IRIs
    public func listShapesGraphs() async throws -> [String] {
        try await context.indexQueryContext.withAuxiliaryReadStorage(
            namespace: Self.shaclPrefix
        ) { subspace, transaction in
            try await SHACLShapesStore(subspace: subspace)
                .listGraphIRIs(transaction: transaction)
        }
    }

    /// Get a shapes graph by IRI
    ///
    /// - Parameter iri: The shapes graph IRI
    /// - Returns: The shapes graph, or nil if not found
    public func getShapesGraph(iri: String) async throws -> SHACLShapesGraph? {
        try await context.indexQueryContext.withAuxiliaryReadStorage(
            namespace: Self.shaclPrefix
        ) { subspace, transaction in
            try await SHACLShapesStore(subspace: subspace).get(
                iri: iri,
                transaction: transaction
            )
        }
    }

    private func getRetainedShapesGraph(
        iri: String,
        snapshot: any IndexQuerySnapshotAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> SHACLShapesStore.RetainedGraph? {
        try await snapshot.withAuxiliaryReadStorage(
            namespace: Self.shaclPrefix
        ) { subspace, transaction in
            try await SHACLShapesStore(subspace: subspace).getRetained(
                iri: iri,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    private func getRetainedOntology(
        iri: String,
        snapshot: any IndexQuerySnapshotAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> OntologyStore.RetainedOntology? {
        try await snapshot.withAuxiliaryReadStorage(
            path: OntologyContextAPI.storagePath
        ) { root, transaction in
            try await OntologyStore(
                subspace: OntologySubspace(base: root)
            ).reconstructRetainedForValidation(
                iri: iri,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    /// Delete a shapes graph by IRI
    ///
    /// - Parameter iri: The shapes graph IRI to delete
    public func deleteShapesGraph(iri: String) async throws {
        try await context.indexQueryContext.withAuxiliaryWriteStorage(
            namespace: Self.shaclPrefix,
            requiredAccess: .write
        ) { subspace, transaction in
            try SHACLShapesStore(subspace: subspace).delete(
                iri: iri,
                transaction: transaction
            )
        }
    }

    /// Delete all shapes graphs
    public func deleteAllShapesGraphs() async throws {
        try await context.indexQueryContext.withAuxiliaryWriteStorage(
            namespace: Self.shaclPrefix,
            requiredAccess: .write
        ) { subspace, transaction in
            try SHACLShapesStore(subspace: subspace).deleteAll(
                transaction: transaction
            )
        }
    }

    // MARK: - Private

    /// Build a SPARQLQueryExecutor for the given Persistable type's graph index
    private func withExecutor<T: Persistable, Result: Sendable>(
        for type: T.Type,
        snapshot: any IndexQuerySnapshotAccess,
        _ operation: @Sendable @escaping (
            SPARQLQueryExecutor,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        let candidates = try context.indexQueryContext
            .indexDescriptors(for: T.self)
            .compactMap {
            try RDFDatasetIndexSelection(descriptor: $0)
        }
        guard candidates.count == 1 else {
            throw SHACLError.graphIndexNotFound(T.persistableType)
        }
        let selection = candidates[0]
        return try await snapshot.withReadableIndex(
            named: selection.indexName,
            indexType: selection.indexType,
            for: T.self,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: nil
            )
        ) { readableIndex, transaction in
            let sources: [RDFDatasetSource]
            if let readableIndex {
                sources = [
                    try RDFDatasetSource(
                        entityName: T.persistableType,
                        selection: selection,
                        indexSubspace: readableIndex.subspace
                    )
                ]
            } else {
                sources = []
            }
            return try await operation(
                SPARQLQueryExecutor(
                    monotonicClock: context.container.monotonicClock,
                    wallClock: context.container.wallClock,
                    sources: sources
                ),
                transaction
            )
        }
    }
}
