import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine

@Suite("Item storage resource accounting")
struct ItemStorageResourceTests {
    @Test("External assembly charges each copy and releases on exhaustion")
    func externalAssemblyChargesCopyWorkAndReleases() async throws {
        let engine = InMemoryEngine()
        defer { engine.requestShutdown() }
        let configuration = try ItemStorageConfiguration(
            encoding: .identity,
            maximumPlainByteCount: 64,
            maximumStoredByteCount: 64,
            maximumInlineByteCount: 2,
            chunkByteCount: 4
        )
        let items = Subspace("item-resource", "items")
        let blobs = Subspace("item-resource", "blobs")
        let key = items.pack(Tuple("external"))
        let payload = ByteString(repeating: 0x5A, count: 12)

        try await engine.withTransaction { transaction in
            try await ItemStorage(
                transaction: transaction,
                blobsSubspace: blobs,
                configuration: configuration
            ).write(payload, for: key)
        }

        let exhaustedMeter = makeMeter(maximumWorkUnits: 1)
        await #expect {
            _ = try await engine.withTransaction { transaction in
                try await ItemStorage(
                    transaction: transaction,
                    blobsSubspace: blobs,
                    configuration: configuration
                ).readRetained(
                    for: key,
                    workMeter: exhaustedMeter,
                    stage: .storageRow
                )
            }
        } throws: { error in
            error as? DatabaseWorkLimitError == .maximumWorkUnits(
                stage: .storageRow,
                consumed: 1,
                requested: 1,
                maximum: 1
            )
        }
        #expect(exhaustedMeter.retainedIntermediateRows == 0)
        #expect(exhaustedMeter.retainedIntermediateBytes == 0)

        let successMeter = makeMeter(maximumWorkUnits: 4)
        do {
            let loaded = try await engine.withTransaction { transaction in
                try await ItemStorage(
                    transaction: transaction,
                    blobsSubspace: blobs,
                    configuration: configuration
                ).readRetained(
                    for: key,
                    workMeter: successMeter,
                    stage: .storageRow
                )
            }
            #expect(loaded == payload)
            #expect(successMeter.consumedWorkUnits == 4)
            #expect(successMeter.retainedIntermediateBytes == 12)
            withExtendedLifetime(loaded) {}
        }
        #expect(successMeter.retainedIntermediateRows == 0)
        #expect(successMeter.retainedIntermediateBytes == 0)
        await engine.waitUntilShutdown()
    }

    private func makeMeter(maximumWorkUnits: UInt64) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: maximumWorkUnits,
                maximumIntermediateBytes: 64
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
