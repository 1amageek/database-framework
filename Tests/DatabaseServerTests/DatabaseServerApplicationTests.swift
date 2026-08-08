import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseServerFoundation
import StorageKit
import TestSupport
import Testing

@Suite("Database server application composition", .serialized)
struct DatabaseServerApplicationTests {
    @Test("Schema-driven definition restores an empty durable catalog")
    func schemaDrivenDefinitionOpensEmptyCatalog() async throws {
        let schemaFactory = AnyDatabaseSchemaRuntimeFactory(
            SchemaDrivenDatabaseRuntimeFactory()
        )
        let definition = DatabaseContainerDefinition(
            schemaRuntimeFactory: schemaFactory,
            security: .disabled,
            databaseName: "schema-driven-application",
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: RealtimeDatabaseWallClock()
        )
        let container = try await definition.open(
            storageEngine: InMemoryEngine()
        )
        defer { await container.shutdown() }

        #expect(definition.isSchemaDriven)
        #expect(definition.declaredSchema == nil)
        #expect(container.schema.entities.isEmpty)
        #expect(container.schema.version == Schema.Version(0, 0, 0))
    }

    @Test("Application erasure preserves definition and runtime factories")
    func applicationErasurePreservesComposition() async throws {
        let schemaFactory = AnyDatabaseSchemaRuntimeFactory(
            SchemaDrivenDatabaseRuntimeFactory()
        )
        let definition = DatabaseContainerDefinition(
            schemaRuntimeFactory: schemaFactory,
            security: .disabled,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: RealtimeDatabaseWallClock()
        )
        let runtimeConfiguration = try makeRuntimeConfiguration(
            schemaFactory: schemaFactory
        )
        let application = try SchemaDrivenDatabaseApplication(
            containerDefinition: definition,
            runtimeConfiguration: runtimeConfiguration
        )
        let erased = AnyDatabaseServerApplication(application)
        let resolvedDefinition = try await erased.makeContainerDefinition()
        let container = try await resolvedDefinition.open(
            storageEngine: InMemoryEngine()
        )
        defer { await container.shutdown() }
        let resolvedRuntime = try await erased.makeRuntimeConfiguration(
            for: container
        )

        #expect(resolvedDefinition.isSchemaDriven)
        #expect(resolvedRuntime.identity.version == "application-test")
        #expect(resolvedRuntime.schemaRuntimeFactory != nil)
    }

    @Test("Schema-driven application rejects incoherent composition")
    func incoherentCompositionIsRejected() throws {
        let emptySchema = try Schema(
            entities: [],
            version: Schema.Version(0, 0, 0)
        )
        let compiledDefinition = DatabaseContainerDefinition(
            schema: emptySchema,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                schema: emptySchema
            ),
            security: .disabled,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: RealtimeDatabaseWallClock()
        )
        let schemaFactory = AnyDatabaseSchemaRuntimeFactory(
            SchemaDrivenDatabaseRuntimeFactory()
        )
        let runtimeConfiguration = try makeRuntimeConfiguration(
            schemaFactory: schemaFactory
        )
        #expect(throws: SchemaDrivenDatabaseApplicationError.compiledContainerDefinition) {
            try SchemaDrivenDatabaseApplication(
                containerDefinition: compiledDefinition,
                runtimeConfiguration: runtimeConfiguration
            )
        }

        let dynamicDefinition = DatabaseContainerDefinition(
            schemaRuntimeFactory: schemaFactory,
            security: .disabled,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: RealtimeDatabaseWallClock()
        )
        let runtimeWithoutSchemaExecution = try makeRuntimeConfiguration(
            schemaFactory: nil
        )
        #expect(throws: SchemaDrivenDatabaseApplicationError.schemaExecutionUnavailable) {
            try SchemaDrivenDatabaseApplication(
                containerDefinition: dynamicDefinition,
                runtimeConfiguration: runtimeWithoutSchemaExecution
            )
        }
    }

    private func makeRuntimeConfiguration(
        schemaFactory: AnyDatabaseSchemaRuntimeFactory?
    ) throws -> DatabaseServerRuntimeConfiguration {
        try DatabaseServerRuntimeConfiguration(
            identity: DatabaseRuntimeIdentity(version: "application-test"),
            serviceFactory: AnyDatabaseServerServiceFactory { _ in
                throw DatabaseServerApplicationTestError.unusedServiceFactory
            },
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            ),
            clock: RealtimeDatabaseWallClock(),
            schemaRuntimeFactory: schemaFactory
        )
    }
}

private enum DatabaseServerApplicationTestError: Error {
    case unusedServiceFactory
}
