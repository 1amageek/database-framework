import Core
import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseValue
import DatabaseWire
import StorageKit
import Testing

@Suite("Database mutation state store", .serialized)
struct DatabaseMutationStateStoreTests {
    @Test("Logical versions advance across committed transactions")
    func advancesLogicalVersionsAcrossTransactions() async throws {
        let storeContext = try await makeMutationStateStoreContext(
            key: "logical-version"
        )

        let firstVersion = try await storeContext.container.engine
            .withTransaction { transaction in
                try await storeContext.stateStore.nextLogicalVersion(
                    transaction: transaction
                )
            }
        let secondVersion = try await storeContext.container.engine
            .withTransaction { transaction in
                try await storeContext.stateStore.nextLogicalVersion(
                    transaction: transaction
                )
            }
        let currentVersion = try await storeContext.container.engine
            .withTransaction(configuration: .readOnly) { transaction in
                try await storeContext.stateStore.currentLogicalVersion(
                    transaction: transaction
                )
            }

        #expect(firstVersion == 1)
        #expect(secondVersion == 2)
        #expect(currentVersion == 2)
    }

    @Test("An empty response round-trips without a chunk")
    func roundTripsEmptyResponse() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "empty")
        let record = makeRecord(responsePayload: [])

        try await store(record, in: storeContext)

        let stored = try await load(from: storeContext)
        let loaded = try #require(stored)
        #expect(loaded == record)
        #expect(try await storedChunks(in: storeContext).isEmpty)
    }

    @Test("A response larger than one FDB value round-trips in bounded chunks")
    func roundTripsResponseLargerThanFDBValue() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "fdb-value-limit")
        let payload = makePayload(count: 100_001)
        let record = makeRecord(responsePayload: payload)

        try await store(record, in: storeContext)

        let chunks = try await storedChunks(in: storeContext)
        try #require(chunks.count == 2)
        #expect(chunks[0].1.count == 90_000)
        #expect(chunks[1].1.count == 10_001)
        #expect(chunks.allSatisfy { $0.1.count <= 90_000 })
        #expect(try await load(from: storeContext) == record)
    }

    @Test("Chunk writes retain borrowed response slices without materializing them")
    func chunkWritesRetainBorrowedSlices() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "borrowed-slices")
        let owner = BorrowCountingDatabaseByteOwner(
            Array(repeating: 0xA5, count: 100_001)
        )
        let record = makeRecord(
            responsePayload: DatabaseBytes(retaining: owner)
        )
        let borrowCountBeforeStore = owner.borrowCount

        try await store(record, in: storeContext)

        #expect(owner.borrowCount == borrowCountBeforeStore)
        _ = try await load(from: storeContext)
        #expect(owner.borrowCount == borrowCountBeforeStore + 2)
    }

    @Test("A maximum-size wire response round-trips")
    func roundTripsMaximumWireResponse() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "maximum-wire-frame")
        let payload = makePayload(
            count: DatabaseWireLimits.default.maximumFrameBytes
        )
        let record = makeRecord(responsePayload: payload)

        try await store(record, in: storeContext)

        let stored = try await load(from: storeContext)
        let loaded = try #require(stored)
        #expect(loaded.responsePayload.count == payload.count)
        #expect(loaded.responsePayload == payload)
        #expect(
            try await storedChunks(in: storeContext).allSatisfy {
                $0.1.count <= 90_000
            }
        )
    }

    @Test("Replacing a response removes every stale chunk")
    func replacementRemovesOrphanedChunks() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "replacement-cleanup")
        try await store(
            makeRecord(responsePayload: makePayload(count: 180_001)),
            in: storeContext
        )
        #expect(try await storedChunks(in: storeContext).count == 3)
        let replacement = makeRecord(responsePayload: [])

        try await store(replacement, in: storeContext)

        #expect(try await storedChunks(in: storeContext).isEmpty)
        #expect(try await load(from: storeContext) == replacement)
    }

    @Test("A metadata-only non-empty response is rejected as corrupted")
    func rejectsMetadataWithoutRequiredChunks() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "metadata-only")
        let record = makeRecord(responsePayload: makePayload(count: 1))
        let metadata = try record.manifest(limits: .default).encode(
            limits: .default
        )
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.setValue(
                Bytes(retaining: metadata),
                for: storeContext.metadataKey
            )
        }

        await expectCorruption(storeContext)
    }

    @Test("A chunk-only idempotency record is rejected as corrupted")
    func rejectsChunkWithoutMetadata() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "chunk-only")
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.setValue(
                Bytes([1]),
                for: storeContext.chunkKey(index: 0)
            )
        }

        await expectCorruption(storeContext)
    }

    @Test("A missing chunk is rejected as corrupted")
    func rejectsMissingChunk() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "missing-chunk")
        try await store(
            makeRecord(responsePayload: makePayload(count: 100_001)),
            in: storeContext
        )
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.clear(key: storeContext.chunkKey(index: 1))
        }

        await expectCorruption(storeContext)
    }

    @Test("An extra chunk is rejected as corrupted")
    func rejectsExtraChunk() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "extra-chunk")
        try await store(
            makeRecord(responsePayload: makePayload(count: 100_001)),
            in: storeContext
        )
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.setValue(
                Bytes([1]),
                for: storeContext.chunkKey(index: 2)
            )
        }

        await expectCorruption(storeContext)
    }

    @Test("A truncated chunk is rejected as corrupted")
    func rejectsTruncatedChunk() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "truncated-chunk")
        try await store(
            makeRecord(responsePayload: makePayload(count: 100_001)),
            in: storeContext
        )
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.setValue(
                Bytes(repeating: 1, count: 89_999),
                for: storeContext.chunkKey(index: 0)
            )
        }

        await expectCorruption(storeContext)
    }

    @Test("A non-canonical chunk index is rejected as corrupted")
    func rejectsNonCanonicalChunkIndex() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "chunk-order")
        try await store(
            makeRecord(responsePayload: makePayload(count: 1)),
            in: storeContext
        )
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.clear(key: storeContext.chunkKey(index: 0))
            try transaction.setValue(
                Bytes([1]),
                for: storeContext.chunks.pack(Tuple("0"))
            )
        }

        await expectCorruption(storeContext)
    }

    @Test("A stored response with a mismatched aggregate digest is rejected")
    func rejectsResponseDigestMismatch() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "response-digest")
        try await store(
            makeRecord(responsePayload: makePayload(count: 100_001)),
            in: storeContext
        )
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.setValue(
                Bytes(repeating: 0xEE, count: 10_001),
                for: storeContext.chunkKey(index: 1)
            )
        }

        await expectCorruption(storeContext)
    }

    @Test("Metadata with non-canonical digest lengths is rejected")
    func rejectsInvalidDigestLengths() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "digest-length")
        let metadata = try encodeUncheckedManifest(
            requestDigest: [1],
            responseDigest: [2],
            totalResponseBytes: 0,
            chunkByteCount: 90_000,
            chunkCount: 0
        )
        try await writeMetadata(metadata, to: storeContext)

        await expectCorruption(storeContext)
    }

    @Test("Metadata with a non-canonical chunk size is rejected")
    func rejectsInvalidChunkSize() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "chunk-size")
        let record = makeRecord(responsePayload: [])
        let metadata = try encodeUncheckedManifest(
            requestDigest: record.requestDigest,
            responseDigest: record.responseDigest,
            totalResponseBytes: 0,
            chunkByteCount: 100_000,
            chunkCount: 0
        )
        try await writeMetadata(metadata, to: storeContext)

        await expectCorruption(storeContext)
    }

    @Test("Metadata with a chunk count inconsistent with its total is rejected")
    func rejectsInvalidChunkCount() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "chunk-count")
        let record = makeRecord(responsePayload: makePayload(count: 1))
        let metadata = try encodeUncheckedManifest(
            requestDigest: record.requestDigest,
            responseDigest: record.responseDigest,
            totalResponseBytes: 1,
            chunkByteCount: 90_000,
            chunkCount: 2
        )
        try await writeMetadata(metadata, to: storeContext)

        await expectCorruption(storeContext)
    }

    @Test("Metadata exceeding the configured collection count is rejected")
    func rejectsChunkCountAboveConfiguredLimit() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "collection-count")
        let payload = makePayload(count: 100_001)
        let record = makeRecord(responsePayload: payload)
        let metadata = try encodeUncheckedManifest(
            requestDigest: record.requestDigest,
            responseDigest: record.responseDigest,
            totalResponseBytes: 100_001,
            chunkByteCount: 90_000,
            chunkCount: 2
        )
        try await writeMetadata(metadata, to: storeContext)
        let limits = try DatabaseWireLimits(
            maximumFrameBytes: DatabaseWireLimits.default.maximumFrameBytes,
            maximumStringBytes: DatabaseWireLimits.default.maximumStringBytes,
            maximumByteStringBytes: DatabaseWireLimits.default.maximumByteStringBytes,
            maximumCollectionCount: 1,
            maximumNestingDepth: DatabaseWireLimits.default.maximumNestingDepth,
            maximumObjectCount: DatabaseWireLimits.default.maximumObjectCount
        )

        await expectCorruption(storeContext, limits: limits)
    }

    @Test("Metadata exceeding the wire frame size is rejected before allocation")
    func rejectsInvalidTotalResponseBytes() async throws {
        let storeContext = try await makeMutationStateStoreContext(key: "response-total")
        let record = makeRecord(responsePayload: [])
        let oversizedTotal = UInt64(
            DatabaseWireLimits.default.maximumFrameBytes
        ) + 1
        let chunkCount = try #require(
            DatabaseIdempotencyManifest.expectedChunkCount(
                totalResponseBytes: oversizedTotal
            )
        )
        let metadata = try encodeUncheckedManifest(
            requestDigest: record.requestDigest,
            responseDigest: record.responseDigest,
            totalResponseBytes: oversizedTotal,
            chunkByteCount: 90_000,
            chunkCount: chunkCount
        )
        try await writeMetadata(metadata, to: storeContext)

        await expectCorruption(storeContext)
    }

    private func makeMutationStateStoreContext(
        key: String
    ) async throws -> MutationStateStoreContext {
        let container = try await DBContainer.open(
            for: Schema(
                [DatabaseEndpointRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let root = try await container.engine.withTransaction {
            transaction in
            try await container.engine.directoryService.createOrOpen(
                path: ["database-framework", "wire-runtime"],
                transaction: transaction
            )
        }
        return MutationStateStoreContext(
            container: container,
            stateStore: stateStore,
            key: key,
            record: root.subspace("idempotency").subspace(key)
        )
    }

    private func makePayload(count: Int) -> DatabaseBytes {
        DatabaseBytes.copying(count: count) { destination in
            for index in 0..<count {
                destination[index] = UInt8(truncatingIfNeeded: index)
            }
        }
    }

    private func makeRecord(
        responsePayload: DatabaseBytes
    ) -> DatabaseIdempotencyRecord {
        let requestPayload: DatabaseBytes = [0xA5]
        return DatabaseIdempotencyRecord(
            operation: .mutationExecute,
            requestDigest: DatabaseRequestDigest.compute(
                operation: .mutationExecute,
                payload: requestPayload
            ),
            responseDigest: DatabaseRequestDigest.compute(
                operation: .mutationExecute,
                payload: responsePayload
            ),
            responsePayload: responsePayload
        )
    }

    private func store(
        _ record: DatabaseIdempotencyRecord,
        in storeContext: MutationStateStoreContext
    ) async throws {
        try await storeContext.container.engine.withTransaction { transaction in
            try storeContext.stateStore.store(
                record,
                for: storeContext.key,
                transaction: transaction,
                limits: .default
            )
        }
    }

    private func load(
        from storeContext: MutationStateStoreContext,
        limits: DatabaseWireLimits = .default
    ) async throws -> DatabaseIdempotencyRecord? {
        try await storeContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await storeContext.stateStore.idempotencyRecord(
                for: storeContext.key,
                transaction: transaction,
                limits: limits
            )
        }
    }

    private func storedChunks(
        in storeContext: MutationStateStoreContext
    ) async throws -> [(Bytes, Bytes)] {
        try await storeContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            let range = storeContext.chunks.range()
            return try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                snapshot: true
            )
        }
    }

    private func writeMetadata(
        _ metadata: DatabaseBytes,
        to storeContext: MutationStateStoreContext
    ) async throws {
        try await storeContext.container.engine.withTransaction { transaction in
            try transaction.setValue(
                Bytes(retaining: metadata),
                for: storeContext.metadataKey
            )
        }
    }

    private func encodeUncheckedManifest(
        requestDigest: DatabaseBytes,
        responseDigest: DatabaseBytes,
        totalResponseBytes: UInt64,
        chunkByteCount: UInt32,
        chunkCount: UInt32
    ) throws -> DatabaseBytes {
        try DatabaseWireWriter.encode(limits: .default) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            writer.writeUInt16(DatabaseIdempotencyManifest.formatVersion)
            DatabaseOperationIdentifier.mutationExecute.encode(into: &writer)
            try writer.writeBytes(requestDigest)
            try writer.writeBytes(responseDigest)
            writer.writeUInt64(totalResponseBytes)
            writer.writeUInt32(chunkByteCount)
            writer.writeUInt32(chunkCount)
        }
    }

    private func expectCorruption(
        _ storeContext: MutationStateStoreContext,
        limits: DatabaseWireLimits = .default
    ) async {
        do {
            _ = try await load(from: storeContext, limits: limits)
            Issue.record("Expected the idempotency record to be corrupted")
        } catch DatabaseMutationError.idempotencyRecordCorrupted {
            return
        } catch {
            Issue.record("Expected idempotencyRecordCorrupted, got \(error)")
        }
    }

    private struct MutationStateStoreContext: Sendable {
        let container: DBContainer
        let stateStore: DatabaseMutationStateStore
        let key: String
        let record: Subspace

        var metadataKey: Bytes {
            record.pack(Tuple("metadata"))
        }

        var chunks: Subspace {
            record.subspace("chunks")
        }

        func chunkKey(index: UInt32) -> Bytes {
            chunks.pack(Tuple(UInt64(index)))
        }
    }
}
