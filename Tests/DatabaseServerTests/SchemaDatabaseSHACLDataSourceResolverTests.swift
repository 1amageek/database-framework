import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import DatabaseKit
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
        let resolved = try await resolutionContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await resolutionContext.resolver.resolve(
                data: resolutionContext.data,
                focus: .entities([identity]),
                entailment: .none,
                workBudget: SHACLValidationWorkBudget(
                    budget: ExecutionBudget(maximumWorkUnits: 10)
                ),
                transaction: transaction
            )
        }

        #expect(resolved.data == resolutionContext.data)
        #expect(
            resolved.focus
                == SHACLExecuteOperation.Focus.entities([identity])
        )
        #expect(resolved.graphScope == .named(try RDFGraphName(iri: "urn:data")))
        #expect(
            resolved.selectedFocusNodes
                == [try RDFTerm.iri(validating: "urn:person:1")]
        )
        #expect(resolved.snapshotFingerprint.count == 8)
    }

    @Test("an empty entity focus remains an empty selection")
    func preservesEmptyEntityFocus() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        let resolved = try await resolutionContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await resolutionContext.resolver.resolve(
                data: resolutionContext.data,
                focus: .entities([]),
                entailment: .none,
                workBudget: SHACLValidationWorkBudget(
                    budget: ExecutionBudget(maximumWorkUnits: 2)
                ),
                transaction: transaction
            )
        }

        #expect(resolved.selectedFocusNodes == [])
    }

    @Test("unsupported entailment fails explicitly")
    func rejectsIncompleteEntailment() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        await #expect(throws: DatabaseSHACLDataSourceError.self) {
            try await resolutionContext.container.engine.withTransaction(
                configuration: .readOnly
            ) { transaction in
                try await resolutionContext.resolver.resolve(
                    data: resolutionContext.data,
                    focus: .targets,
                    entailment: .rdfs,
                    workBudget: SHACLValidationWorkBudget(
                        budget: ExecutionBudget(maximumWorkUnits: 2)
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
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseSHACLStatement.self]
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
            try await container.engine.directoryService.createOrOpen(
                path: ["database-framework", "ontology-index"],
                transaction: transaction
            )
        }
        return SHACLDataSourceResolutionContext(
            container: container,
            resolver: SchemaDatabaseSHACLDataSourceResolver(
                container: container,
                stateStore: stateStore,
                ontologyStore: OntologyStore(
                    subspace: OntologySubspace(base: ontologySubspace)
                )
            ),
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
        let data: SHACLExecuteOperation.DataSource
    }

    private enum SHACLDataSourceResolutionSetupError: Error {
        case missingRDFIndex
    }
}
