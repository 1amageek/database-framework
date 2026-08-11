import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseWireRuntime
import DatabaseFoundation
import StorageKit
import TestSupport
import Testing

@Suite("Database application composition", .serialized)
struct DatabaseApplicationTests {
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
        let runtimeConfiguration = try makeRuntimeConfiguration(
            schemaFactory: schemaFactory
        )
        let application = TestDatabaseApplication(
            containerDefinition: definition,
            runtimeConfiguration: runtimeConfiguration
        )
        let erased = AnyDatabaseApplication(application)
        let resolvedDefinition = try await erased.makeContainerDefinition()
        let container = try await resolvedDefinition.open(
            storageTopology: try DatabaseStorageTopology.testing(
                storageEngine: InMemoryEngine()
            )
        )
        defer { await container.shutdown() }
        let resolvedRuntime = try await erased.makeRuntimeConfiguration(
            for: container
        )

        #expect(resolvedDefinition.isSchemaDriven)
        #expect(resolvedRuntime.identity.version == "application-test")
        #expect(resolvedRuntime.schemaRuntimeFactory != nil)
    }

    private func makeRuntimeConfiguration(
        schemaFactory: AnyDatabaseSchemaRuntimeFactory?
    ) throws -> DatabaseOperationRuntimeConfiguration {
        try DatabaseOperationRuntimeConfiguration(
            identity: DatabaseRuntimeIdentity(version: "application-test"),
            serviceFactory: AnyDatabaseOperationServiceFactory { _ in
                throw DatabaseApplicationTestError.unusedServiceFactory
            },
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            ),
            clock: RealtimeDatabaseWallClock(),
            schemaRuntimeFactory: schemaFactory
        )
    }
}

private struct TestDatabaseApplication: DatabaseApplication {
    let containerDefinition: DatabaseContainerDefinition
    let runtimeConfiguration: DatabaseOperationRuntimeConfiguration

    func makeContainerDefinition() async throws -> DatabaseContainerDefinition {
        containerDefinition
    }

    func makeRuntimeConfiguration(
        for container: DBContainer
    ) async throws -> DatabaseOperationRuntimeConfiguration {
        _ = container
        return runtimeConfiguration
    }
}

private enum DatabaseApplicationTestError: Error {
    case unusedServiceFactory
}
