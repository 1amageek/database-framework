import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseOperations
import DatabaseFoundation
import StorageKit
import TestSupport
import Testing

@Suite("Database application composition", .serialized)
struct DatabaseOperationApplicationTests {
    @Test("Schema-driven definition restores an empty durable catalog")
    func schemaDrivenDefinitionOpensEmptyCatalog() async throws {
        let schemaFactory = AnyDatabaseSchemaRuntimeFactory(
            SchemaDrivenDatabaseRuntimeFactory()
        )
        let definition = DatabaseContainerDefinition(
            schemaRuntimeFactory: schemaFactory,
            security: .testingDisabled,
            databaseName: "schema-driven-application",
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: RealtimeDatabaseWallClock()
        )
        let container = try await definition.open(
            storageTopology: try DatabaseStorageTopology.testing(
                storageEngine: InMemoryEngine()
            )
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
            security: .testingDisabled,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: RealtimeDatabaseWallClock()
        )
        let runtimeConfiguration = try makeOperationConfiguration(
            schemaFactory: schemaFactory
        )
        let application = TestDatabaseOperationApplication(
            containerDefinition: definition,
            runtimeConfiguration: runtimeConfiguration
        )
        let erased = AnyDatabaseOperationApplication(application)
        let resolvedDefinition = try await erased.makeContainerDefinition()
        let container = try await resolvedDefinition.open(
            storageTopology: try DatabaseStorageTopology.testing(
                storageEngine: InMemoryEngine()
            )
        )
        defer { await container.shutdown() }
        let resolvedRuntime = try await erased.makeOperationConfiguration(
            for: container
        )

        #expect(resolvedDefinition.isSchemaDriven)
        #expect(resolvedRuntime.identity.version == "application-test")
        #expect(resolvedRuntime.schemaRuntimeFactory != nil)
    }

    private func makeOperationConfiguration(
        schemaFactory: AnyDatabaseSchemaRuntimeFactory?
    ) throws -> DatabaseOperationConfiguration {
        try DatabaseOperationConfiguration(
            identity: DatabaseOperationIdentity(version: "application-test"),
            serviceFactory: AnyDatabaseOperationServiceFactory { _ in
                throw DatabaseOperationApplicationTestError.unusedServiceFactory
            },
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            ),
            schemaRuntimeFactory: schemaFactory
        )
    }
}

private struct TestDatabaseOperationApplication: DatabaseOperationApplication {
    let containerDefinition: DatabaseContainerDefinition
    let runtimeConfiguration: DatabaseOperationConfiguration

    func makeContainerDefinition() async throws -> DatabaseContainerDefinition {
        containerDefinition
    }

    func makeOperationConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseOperationConfiguration {
        _ = container
        return runtimeConfiguration
    }
}

private enum DatabaseOperationApplicationTestError: Error {
    case unusedServiceFactory
}
