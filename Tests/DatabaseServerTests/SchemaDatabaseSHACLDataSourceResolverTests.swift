import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import GraphIndex
import OntologyIndex
import StorageKit
import Testing

@Suite("Schema database SHACL data source resolver")
struct SchemaDatabaseSHACLDataSourceResolverTests {
    @Test("entity focus resolves compiled RDF subjects")
    func resolvesEntityFocus() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        let statement = DatabaseSHACLStatement(
            id: "statement-1",
            subject: try .iri(validating: "urn:person:1"),
            predicate: try .iri(validating: "urn:predicate"),
            object: try .iri(validating: "urn:object"),
            graph: try .iri(validating: "urn:data")
        )
        let context = resolutionContext.container.newContext()
        try context.insert(statement)
        try await context.save()

        let identity = try EntityReference(
            entity: DatabaseSHACLStatement.persistableType,
            id: .string(statement.id)
        )
        let resolved = try await StorageTransactionExecutor(
            engine: resolutionContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await resolutionContext.resolver.resolve(
                data: resolutionContext.data,
                focus: .entities([identity]),
                entailment: .none,
                workBudget: SHACLValidationWorkBudget(
                    budget: ExecutionBudget(maximumWorkUnits: 10),
                    monotonicClock: TestProcessMonotonicClock()
                ),
                transaction: transaction
            )
        }

        #expect(resolved.data == resolutionContext.data)
        #expect(
            resolved.focus
                == SHACLExecuteOperation.Focus.entities([identity])
        )
        #expect(resolved.dataGraph == .named(try RDFGraphName(iri: "urn:data")))
        #expect(
            resolved.selectedFocusNodes
                == [try RDFTerm.iri(validating: "urn:person:1")]
        )
        #expect(resolved.snapshotFingerprint.count == 8)
    }

    @Test("an empty entity focus remains an empty selection")
    func preservesEmptyEntityFocus() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        let resolved = try await StorageTransactionExecutor(
            engine: resolutionContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await resolutionContext.resolver.resolve(
                data: resolutionContext.data,
                focus: .entities([]),
                entailment: .none,
                workBudget: SHACLValidationWorkBudget(
                    budget: ExecutionBudget(maximumWorkUnits: 2),
                    monotonicClock: TestProcessMonotonicClock()
                ),
                transaction: transaction
            )
        }

        #expect(resolved.selectedFocusNodes == [])
    }

    @Test("RDFS entailment resolves the selected data graph")
    func resolvesRDFSEntailment() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        try await insertRDFSData(resolutionContext)

        let resolved = try await StorageTransactionExecutor(
            engine: resolutionContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
                try await resolutionContext.resolver.resolve(
                    data: resolutionContext.data,
                    focus: .targets,
                    entailment: .rdfs,
                    workBudget: SHACLValidationWorkBudget(
                        budget: ExecutionBudget(maximumWorkUnits: 200),
                        monotonicClock: TestProcessMonotonicClock()
                    ),
                    transaction: transaction
                )
            }

        #expect(
            resolved.entailmentContext?.contains(
                try RDFTerm.iri(validating: "urn:test:Alice"),
                in: "urn:test:Person"
            ) == true
        )
        #expect(
            resolved.entailmentContext?.subProperties(
                of: "urn:test:knows"
            ).contains("urn:test:manages") == true
        )
    }

    @Test("RDFS closure obeys the validation work budget")
    func boundsRDFSEntailmentWork() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        try await insertRDFSData(resolutionContext)

        await #expect(throws: DatabaseWorkLimitError.self) {
            try await StorageTransactionExecutor(
                engine: resolutionContext.container.engine
            ).withTransaction(
                configuration: .readOnly,
                clock: TestProcessMonotonicClock()
            ) { transaction in
                try await resolutionContext.resolver.resolve(
                    data: resolutionContext.data,
                    focus: .targets,
                    entailment: .rdfs,
                    workBudget: SHACLValidationWorkBudget(
                        budget: ExecutionBudget(maximumWorkUnits: 1),
                        monotonicClock: TestProcessMonotonicClock()
                    ),
                    transaction: transaction
                )
            }
        }
    }

    private func insertRDFSData(
        _ resolutionContext: SHACLDataSourceResolutionContext
    ) async throws {
        let triples = [
            ("subclass", "urn:test:Employee", Self.rdfsSubClassOf, "urn:test:Person"),
            ("subproperty", "urn:test:manages", Self.rdfsSubPropertyOf, "urn:test:knows"),
            ("type", "urn:test:Alice", Self.rdfType, "urn:test:Employee")
        ]
        let context = resolutionContext.container.newContext()
        for (id, subject, predicate, object) in triples {
            try context.insert(
                DatabaseSHACLStatement(
                    id: id,
                    subject: try .iri(validating: subject),
                    predicate: try .iri(validating: predicate),
                    object: try .iri(validating: object),
                    graph: try .iri(validating: "urn:data")
                )
            )
        }
        try await context.save()
    }

    @Test("OWL entailment resolves the stored merged ontology")
    func resolvesOWLEntailment() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        var ontology = OWLOntology(
            iri: "urn:test:shacl-ontology",
            classes: [
                OWLClass(iri: "urn:test:Person"),
                OWLClass(iri: "urn:test:Employee")
            ]
        )
        ontology.axioms = [
            .subClassOf(
                sub: .named("urn:test:Employee"),
                sup: .named("urn:test:Person")
            )
        ]
        let ontologyIdentifier = ontology.iri
        let storedOntology = ontology
        try await resolutionContext.container.engine.withTransaction {
            transaction in
            try await resolutionContext.ontologyStore.loadOntology(
                storedOntology,
                at: Timestamp(secondsSinceUnixEpoch: 1_000),
                transaction: transaction
            )
        }

        let resolved = try await StorageTransactionExecutor(
            engine: resolutionContext.container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
                try await resolutionContext.resolver.resolve(
                    data: resolutionContext.data,
                    focus: .targets,
                    entailment: .owl(ontology: ontologyIdentifier),
                    workBudget: SHACLValidationWorkBudget(
                        budget: ExecutionBudget(maximumWorkUnits: 100),
                        monotonicClock: TestProcessMonotonicClock()
                    ),
                    transaction: transaction
                )
            }

        #expect(
            resolved.entailmentContext?.subsumes(
                superClass: "urn:test:Person",
                subClass: "urn:test:Employee"
            ) == true
        )
    }

    @Test("missing OWL ontology fails explicitly")
    func rejectsMissingOWLOntology() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()

        await #expect(throws: SHACLError.self) {
            try await StorageTransactionExecutor(
                engine: resolutionContext.container.engine
            ).withTransaction(
                configuration: .readOnly,
                clock: TestProcessMonotonicClock()
            ) { transaction in
                try await resolutionContext.resolver.resolve(
                    data: resolutionContext.data,
                    focus: .targets,
                    entailment: .owl(ontology: "urn:test:missing"),
                    workBudget: SHACLValidationWorkBudget(
                        budget: ExecutionBudget(maximumWorkUnits: 10),
                        monotonicClock: TestProcessMonotonicClock()
                    ),
                    transaction: transaction
                )
            }
        }
    }

    private func makeSHACLDataSourceResolutionContext()
        async throws -> SHACLDataSourceResolutionContext {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseSHACLStatement.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseSHACLStatement.self)]
            ),
            security: .disabled
        )
        guard let descriptor = try DatabaseSHACLStatement.indexDescriptors
            .first(where: { $0.kindIdentifier == "rdf_quad" }) else {
            throw SHACLDataSourceResolutionSetupError.missingRDFIndex
        }
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let ontologySubspace = try await container.engine.withTransaction {
            transaction in
            try await container.engine.namespaceResolver.resolveOrCreate(
                path: ["database-framework", "ontology-index"],
                transaction: transaction
            )
        }
        let ontologyStore = OntologyStore(
            subspace: OntologySubspace(base: ontologySubspace)
        )
        return SHACLDataSourceResolutionContext(
            container: container,
            resolver: SchemaDatabaseSHACLDataSourceResolver(
                container: container,
                stateStore: stateStore,
                ontologyStore: ontologyStore
            ),
            ontologyStore: ontologyStore,
            data: SHACLExecuteOperation.DataSource(
                entity: DatabaseSHACLStatement.persistableType,
                index: descriptor.name,
                graph: .named(try RDFTerm.iri(validating: "urn:data"))
            )
        )
    }

    private struct SHACLDataSourceResolutionContext: Sendable {
        let container: DBContainer
        let resolver: SchemaDatabaseSHACLDataSourceResolver
        let ontologyStore: OntologyStore
        let data: SHACLExecuteOperation.DataSource
    }

    private enum SHACLDataSourceResolutionSetupError: Error {
        case missingRDFIndex
    }

    private static let rdfType =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    private static let rdfsSubClassOf =
        "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    private static let rdfsSubPropertyOf =
        "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
}
