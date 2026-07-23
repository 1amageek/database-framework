import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import OntologyIndex
import StorageKit
import Testing

@Suite("Schema database SHACL data source resolver")
struct SchemaDatabaseSHACLDataSourceResolverTests {
    @Test("record focus resolves compiled RDF subjects")
    func resolvesRecordFocus() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        var statement = DatabaseSHACLStatement()
        statement.id = "statement-1"
        statement.subject = .iri("urn:person:1")
        statement.predicate = .iri("urn:predicate")
        statement.object = .iri("urn:object")
        statement.graph = .iri("urn:data")
        let context = resolutionContext.container.newContext()
        context.insert(statement)
        try await context.save()

        let identity = RecordIdentity(
            entity: DatabaseSHACLStatement.persistableType,
            id: .string(statement.id)
        )
        let resolved = try await resolutionContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await resolutionContext.resolver.resolve(
                data: resolutionContext.data,
                focus: .records([identity]),
                entailment: .none,
                workBudget: SHACLValidationWorkBudget(
                    budget: DatabaseExecutionBudget(maximumWorkUnits: 10)
                ),
                transaction: transaction
            )
        }

        #expect(resolved.data == resolutionContext.data)
        #expect(resolved.focus == .records([identity]))
        #expect(resolved.graphScope == .named(try RDFGraphName(iri: "urn:data")))
        #expect(resolved.selectedFocusNodes == [.iri("urn:person:1")])
        #expect(resolved.snapshotFingerprint.count == 8)
    }

    @Test("an empty record focus remains an empty selection")
    func preservesEmptyRecordFocus() async throws {
        let resolutionContext = try await makeSHACLDataSourceResolutionContext()
        let resolved = try await resolutionContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await resolutionContext.resolver.resolve(
                data: resolutionContext.data,
                focus: .records([]),
                entailment: .none,
                workBudget: SHACLValidationWorkBudget(
                    budget: DatabaseExecutionBudget(maximumWorkUnits: 2)
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
                        budget: DatabaseExecutionBudget(maximumWorkUnits: 2)
                    ),
                    transaction: transaction
                )
            }
        }
    }

    private func makeSHACLDataSourceResolutionContext()
        async throws -> SHACLDataSourceResolutionContext {
        let container = try await DBContainer(
            for: Schema(
                [DatabaseSHACLStatement.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        guard let descriptor = DatabaseSHACLStatement.indexDescriptors
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
                graph: .named(.iri("urn:data"))
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
