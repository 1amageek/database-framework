#if !os(WASI) && !MultiBase
import DatabaseKit
import DatabaseRuntime
import Foundation
import StorageKit
import TestSupport
import Testing

@_spi(Benchmarking) import DatabaseEngine

@Persistable
private struct DataStoreProbeEntity {
    #Directory<DataStoreProbeEntity>(
        "test",
        "data-store-probe",
        \DataStoreProbeEntity.partitionID,
        "entities",
        layer: .partition
    )

    var id: String = UUID().uuidString
    var partitionID: String
    var value: String
}

@Suite("DataStore persistence probe")
struct DataStoreProbeTests {
    @Test("Dynamic partition store writes and reads the production DataStore path")
    func dynamicPartitionStoreRoundTrips() async throws {
        let engine = InMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try DataStoreProbeEntity.schemaEntity],
                version: .init(1, 0, 0)
            ),
            configuration: try .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-engine-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        DataStoreProbeEntity.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        let partitionID = "partition-\(UUID().uuidString)"
        var path = DatabaseEngine.DirectoryPath<DataStoreProbeEntity>()
        path.set(
            DataStoreProbeEntity.fields.partitionID,
            to: partitionID
        )
        let boundPath = path
        let store = try await DataStoreBenchmarkProbe.openDataStore(
            for: DataStoreProbeEntity.self,
            in: container,
            path: boundPath
        )
        let entity = DataStoreProbeEntity(
            partitionID: partitionID,
            value: "round-trip"
        )

        try await store.executeBatch(inserts: [entity], deletes: [])
        let fetched = try await store.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.fetch(
                DataStoreProbeEntity.self,
                identifiedBy: entity.id,
                in: boundPath,
                consistency: .serializable
            )
        }

        #expect(fetched?.partitionID == partitionID)
        #expect(fetched?.value == entity.value)
    }
}
#endif
