#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseEngine
import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire
import GraphIndex
import OntologyIndex
import StorageKit

/// Resolves the reasoning components selected by one SHACL request.
struct DatabaseSHACLEntailmentResolver: Sendable {
    struct Resolution: Sendable {
        let ontologyContext: OntologyContext?
        let entailmentContext: (any SHACLEntailmentContext)?

        static let none = Resolution(
            ontologyContext: nil,
            entailmentContext: nil
        )
    }

    let ontologyStore: OntologyStore
    let monotonicClock: any StorageMonotonicClock

    func resolve(
        _ entailment: SHACLExecuteOperation.Entailment,
        executor: SPARQLQueryExecutor,
        dataGraph: SHACLDataGraphTarget,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> Resolution {
        switch entailment {
        case .none:
            return .none
        case .rdfs:
            let rdfs = try await RDFSGraphEntailment.resolve(
                executor: executor,
                dataGraph: dataGraph,
                transaction: transaction,
                budget: workBudget
            )
            return Resolution(
                ontologyContext: rdfs.ontologyContext,
                entailmentContext: rdfs
            )
        case .owl(let ontologyIdentifier):
            try workBudget.consume(at: .validation)
            guard let ontology = try await ontologyStore.reconstruct(
                iri: ontologyIdentifier,
                transaction: transaction
            ) else {
                throw SHACLError.ontologyNotFound(ontologyIdentifier)
            }
            try consumeOntology(
                ontology,
                workBudget: workBudget
            )
            return Resolution(
                ontologyContext: OntologyContext(ontology: ontology),
                entailmentContext: OWLGraphEntailment(
                    reasoner: OWLReasoner(
                        ontology: ontology,
                        clock: monotonicClock,
                        configuration: reasonerConfiguration(
                            workBudget.workMeter.budget
                        )
                    )
                )
            )
        }
    }

    private func consumeOntology(
        _ ontology: OWLOntology,
        workBudget: SHACLValidationWorkBudget
    ) throws {
        try workBudget.consume(
            UInt64(ontology.classes.count),
            at: .validation
        )
        try workBudget.consume(
            UInt64(ontology.objectProperties.count),
            at: .validation
        )
        try workBudget.consume(
            UInt64(ontology.dataProperties.count),
            at: .validation
        )
        try workBudget.consume(
            UInt64(ontology.annotationProperties.count),
            at: .validation
        )
        try workBudget.consume(
            UInt64(ontology.individuals.count),
            at: .validation
        )
        try workBudget.consume(
            UInt64(ontology.axioms.count),
            at: .validation
        )
    }

    private func reasonerConfiguration(
        _ budget: ExecutionBudget
    ) -> OWLReasoner.Configuration {
        OWLReasoner.Configuration(
            maxExpansionSteps: Int(
                min(budget.maximumWorkUnits, UInt64(Int.max))
            ),
            enableIncrementalReasoning: true,
            cacheClassification: true,
            timeout: .milliseconds(Int64(clamping: budget.timeoutMilliseconds))
        )
    }
}
#endif
