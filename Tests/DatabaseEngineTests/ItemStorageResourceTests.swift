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

        // The retained point-read boundary detaches the envelope and three
        // chunks. Three chunk assembly copies and one checksum pass bring the
        // total to eight bounded 256-byte work quanta for this fixture.
        let successMeter = makeMeter(maximumWorkUnits: 8)
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
            #expect(successMeter.consumedWorkUnits == 8)
            #expect(successMeter.retainedIntermediateBytes == 12)
            withExtendedLifetime(loaded) {}
        }
        #expect(successMeter.retainedIntermediateRows == 0)
        #expect(successMeter.retainedIntermediateBytes == 0)
        await engine.waitUntilShutdown()
    }

    @Test("Retained external reads bound the envelope and every chunk")
    func retainedExternalReadsUseBoundedPointReads() async throws {
        let engine = ControlledStorageEngine(base: InMemoryEngine())
        defer { await engine.waitUntilShutdown() }
        let configuration = try ItemStorageConfiguration(
            encoding: .identity,
            maximumPlainByteCount: 64,
            maximumStoredByteCount: 64,
            maximumInlineByteCount: 2,
            chunkByteCount: 4
        )
        let items = Subspace("item-resource-bounded", "items")
        let blobs = Subspace("item-resource-bounded", "blobs")
        let key = items.pack(Tuple("external"))
        let payload = ByteString(repeating: 0x3C, count: 12)

        try await engine.withTransaction { transaction in
            try await ItemStorage(
                transaction: transaction,
                blobsSubspace: blobs,
                configuration: configuration
            ).write(payload, for: key)
        }

        let meter = makeMeter(maximumWorkUnits: 100)
        var retained = try await engine.withTransaction { transaction in
            try await ItemStorage(
                transaction: transaction,
                blobsSubspace: blobs,
                configuration: configuration
            ).readRetained(
                for: key,
                workMeter: meter,
                stage: .storageRow
            )
        }

        #expect(retained == payload)
        // The 12-byte payload uses one envelope read and three 4-byte chunks.
        #expect(engine.control.boundedValueReadMaximums.count == 4)
        #expect(
            engine.control.boundedValueReadMaximums.allSatisfy {
                $0 > 0 && $0 <= 64
            }
        )
        #expect(meter.retainedIntermediateBytes == UInt64(payload.count))

        retained = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
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
