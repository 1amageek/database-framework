import DatabaseKit
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Suite("Database Index Maintenance Runtime Tests", .serialized)
struct DatabaseIndexMaintenanceRuntimeTests {
    @Test("Rebuild advances one transactional slice and resumes after recreation")
    func rebuildResumesAfterRecreation() async throws {
        let engine = InMemoryEngine()
        let firstContainer = try await makeContainer(engine: engine)
        try await insertEntities(into: firstContainer)
        let decodedGeneration = DatabaseTypes.UUID(bytes: Array(0..<16))
        let generation = try #require(decodedGeneration)
        let partitions = try tenantPartition("tenant-a")
        var directoryPath = DirectoryPath<CatalogPartitionedEntity>()
        directoryPath.set(
            CatalogPartitionedEntity.fields.tenantID,
            to: "tenant-a"
        )
        let entitySubspace = try await firstContainer.testBaseDirectory(
            for: CatalogPartitionedEntity.self,
            path: directoryPath
        )
        let longLivedStateReader = IndexLifecycleStore(
            container: firstContainer,
            subspace: entitySubspace
        )
        let initiallyReadable = try await firstContainer.withTestBaseOperation {
            try await StorageTransactionExecutor(
                engine: firstContainer.engine
            ).withTransaction(
                configuration: .readOnly,
                clock: TestProcessMonotonicClock()
            ) { transaction in
                    try await longLivedStateReader.state(
                        of: "catalog_value",
                        transaction: transaction
                    )
            }
        }
        #expect(initiallyReadable == .readable)
        let firstRuntime = DatabaseIndexMaintenanceRuntime(
            container: firstContainer
        )
        let preparedPartitions = try await firstContainer.testBaseContext()
            .withTransaction { transaction in
            try await firstRuntime.prepareResources(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                transaction: transaction.storageAccess
            )
        }
        #expect(preparedPartitions == partitions)

        let firstSlice = try await firstContainer.testBaseContext().withTransaction {
            transaction in
            try await firstRuntime.runRebuildSlice(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                generation: generation,
                mode: .start,
                maximumWorkUnits: 1,
                transaction: transaction.storageAccess
            )
        }
        #expect(firstSlice.completedWorkUnits == 1)
        #expect(!firstSlice.isComplete)
        let stateObservedByLongLivedReader = try await firstContainer
            .withTestBaseOperation {
                try await StorageTransactionExecutor(
                    engine: firstContainer.engine
                ).withTransaction(
                    configuration: .readOnly,
                    clock: TestProcessMonotonicClock()
                ) { transaction in
                        try await longLivedStateReader.state(
                            of: "catalog_value",
                            transaction: transaction
                        )
                }
            }
        #expect(stateObservedByLongLivedReader == .writeOnly)

        let intermediate = try await status(
            runtime: firstRuntime,
            container: firstContainer,
            partitions: partitions
        )
        #expect(intermediate.indexState == .writeOnly)
        #expect(intermediate.rebuildState?.indexedEntityCount == 1)

        let recreatedContainer = try await makeContainer(engine: engine)
        let recreatedRuntime = DatabaseIndexMaintenanceRuntime(
            container: recreatedContainer
        )
        let finalSlice = try await recreatedContainer.testBaseContext().withTransaction {
            transaction in
            try await recreatedRuntime.runRebuildSlice(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                generation: generation,
                mode: .resume,
                maximumWorkUnits: 1,
                transaction: transaction.storageAccess
            )
        }
        #expect(finalSlice.completedWorkUnits == 1)
        #expect(finalSlice.isComplete)
        #expect(finalSlice.indexedEntityCount == 2)

        let completed = try await status(
            runtime: recreatedRuntime,
            container: recreatedContainer,
            partitions: partitions
        )
        #expect(completed.indexState == .readable)
        #expect(completed.rebuildState?.phase == .complete)
        #expect(completed.rebuildState?.indexedEntityCount == 2)
    }

    @Test("A second generation cannot enter an active rebuild")
    func rejectsConcurrentGeneration() async throws {
        let container = try await makeContainer(engine: InMemoryEngine())
        try await insertEntities(into: container)
        let partitions = try tenantPartition("tenant-a")
        let runtime = DatabaseIndexMaintenanceRuntime(container: container)
        let preparedPartitions = try await container.testBaseContext()
            .withTransaction { transaction in
            try await runtime.prepareResources(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                transaction: transaction.storageAccess
            )
        }
        #expect(preparedPartitions == partitions)
        let decodedFirstGeneration = DatabaseTypes.UUID(
            bytes: Array(repeating: 1, count: 16)
        )
        let firstGeneration = try #require(decodedFirstGeneration)
        _ = try await container.testBaseContext().withTransaction { transaction in
            try await runtime.runRebuildSlice(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                generation: firstGeneration,
                mode: .start,
                maximumWorkUnits: 1,
                transaction: transaction.storageAccess
            )
        }
        let decodedSecondGeneration = DatabaseTypes.UUID(
            bytes: Array(repeating: 2, count: 16)
        )
        let secondGeneration = try #require(decodedSecondGeneration)

        await #expect(
            throws: DatabaseIndexRebuildError.buildAlreadyActive(
                index: "catalog_value",
                generation: firstGeneration
            )
        ) {
            try await container.testBaseContext().withTransaction { transaction in
                try await runtime.runRebuildSlice(
                    entity: CatalogPartitionedEntity.persistableType,
                    index: "catalog_value",
                    partitions: partitions,
                    generation: secondGeneration,
                    mode: .start,
                    maximumWorkUnits: 1,
                    transaction: transaction.storageAccess
                )
            }
        }
    }

    @Test("Resume requires an existing matching building entity")
    func resumeRequiresExistingBuildingEntity() async throws {
        let container = try await makeContainer(engine: InMemoryEngine())
        try await insertEntities(into: container)
        let partitions = try tenantPartition("tenant-a")
        let runtime = DatabaseIndexMaintenanceRuntime(container: container)
        _ = try await container.testBaseContext().withTransaction { transaction in
            try await runtime.prepareResources(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                transaction: transaction.storageAccess
            )
        }
        let generation = DatabaseTypes.UUID(high: 3, low: 1)

        await #expect(throws: DatabaseIndexRebuildError.corruptedRebuildState) {
            try await container.testBaseContext().withTransaction { transaction in
                try await runtime.runRebuildSlice(
                    entity: CatalogPartitionedEntity.persistableType,
                    index: "catalog_value",
                    partitions: partitions,
                    generation: generation,
                    mode: .resume,
                    maximumWorkUnits: 1,
                    transaction: transaction.storageAccess
                )
            }
        }
    }

    private func status(
        runtime: DatabaseIndexMaintenanceRuntime,
        container: DBContainer,
        partitions: FieldObject
    ) async throws -> DatabaseIndexMaintenanceStatus {
        try await container.testBaseContext().withTransaction { transaction in
            try await runtime.status(
                entity: CatalogPartitionedEntity.persistableType,
                index: "catalog_value",
                partitions: partitions,
                transaction: transaction.storageAccess
            )
        }
    }

    private func insertEntities(into container: DBContainer) async throws {
        let context = container.testBaseContext()
        var first = CatalogPartitionedEntity()
        first.id = "first"
        first.tenantID = "tenant-a"
        first.value = "alpha"
        var second = CatalogPartitionedEntity()
        second.id = "second"
        second.tenantID = "tenant-a"
        second.value = "beta"
        try context.insert(first)
        try context.insert(second)
        try await context.save()
    }

    private func tenantPartition(_ tenant: String) throws -> FieldObject {
        try FieldObject([
            (key: "tenantID", value: .string(tenant)),
        ])
    }

    private func makeContainer(engine: InMemoryEngine) async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [CatalogPartitionedEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(CatalogPartitionedEntity.self)]
            ),
            security: .testingDisabled
        )
    }
}
