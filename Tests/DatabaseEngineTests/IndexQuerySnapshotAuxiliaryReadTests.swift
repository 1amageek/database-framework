import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Index query snapshot auxiliary reads")
struct IndexQuerySnapshotAuxiliaryReadTests {
    @Test("Multiple auxiliary namespaces observe one storage snapshot")
    func auxiliaryNamespacesShareOneSnapshot() async throws {
        let engine = InMemoryEngine()
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try AuxiliarySnapshotEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "index-query-snapshot-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        AuxiliarySnapshotEntity.self
                    )
                ]
            ),
            security: .testingDisabled,
            initializeIndexes: false
        )
        do {
            let context = container.testBaseContext()
            let path = ["snapshot-contract", "auxiliary"]
            let keys = try await context.indexQueryContext
                .withAuxiliaryWriteStorage(
                    path: path,
                    requiredAccess: .write
                ) { subspace, transaction in
                    let first = subspace.pack(Tuple("first"))
                    let second = subspace.pack(Tuple("second"))
                    try transaction.setValue([0x01], for: first)
                    try transaction.setValue([0x02], for: second)
                    return (first, second)
                }

            let snapshotValue = try await context.indexQueryContext
                .withQuerySnapshot { snapshot in
                    let firstValue = try await snapshot
                        .withAuxiliaryReadStorage(path: path) {
                            _, transaction in
                            try await transaction.getValue(
                                for: keys.0,
                                snapshot: true
                            )
                        }
                    #expect(firstValue == [0x01])

                    try await Task.detached {
                        try await engine.withTransaction { transaction in
                            try transaction.setValue([0x03], for: keys.1)
                        }
                    }.value

                    return try await snapshot.withAuxiliaryReadStorage(
                        path: path
                    ) { _, transaction in
                        try await transaction.getValue(
                            for: keys.1,
                            snapshot: true
                        )
                    }
                }
            #expect(snapshotValue == [0x02])

            let latestValue = try await context.indexQueryContext
                .withAuxiliaryReadStorage(path: path) { _, transaction in
                    try await transaction.getValue(
                        for: keys.1,
                        snapshot: true
                    )
                }
            #expect(latestValue == [0x03])
        } catch {
            await container.shutdown()
            throw error
        }
        await container.shutdown()
    }
}

@Persistable
private struct AuxiliarySnapshotEntity {
    #Directory<AuxiliarySnapshotEntity>("auxiliary_snapshot_entities")

    var id: String
}
