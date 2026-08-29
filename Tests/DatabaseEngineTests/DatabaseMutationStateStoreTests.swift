import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
// TEST_SCALE_SEMANTICS: correctness - verifies the configured mutation-state byte limit.

import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Database mutation state store", .serialized)
struct DatabaseMutationStateStoreTests {
    @Test("logical versions advance across committed transactions")
    func advancesLogicalVersions() async throws {
        let context = try await makeContext(key: "logical-version")
        let first = try await withTransaction(context) { transaction in
            try await context.store.nextLogicalVersion(
                in: context.binding,
                transaction: transaction
            )
        }
        let second = try await withTransaction(context) { transaction in
            try await context.store.nextLogicalVersion(
                in: context.binding,
                transaction: transaction
            )
        }
        let current = try await withTransaction(
            context,
            configuration: .readOnly
        ) { transaction in
            try await context.store.currentLogicalVersion(
                in: context.binding,
                transaction: transaction
            )
        }

        #expect(first == 1)
        #expect(second == 2)
        #expect(current == 2)
    }

    @Test("an empty outcome round-trips without a chunk")
    func roundTripsEmptyOutcome() async throws {
        let context = try await makeContext(key: "empty")
        let record = makeRecord(outcome: [])
        try await store(record, in: context)

        #expect(try await load(from: context) == record)
        #expect(try await storedChunks(in: context).isEmpty)
    }

    @Test("a large outcome round-trips in bounded chunks")
    func roundTripsLargeOutcome() async throws {
        let context = try await makeContext(key: "large")
        let record = makeRecord(outcome: makePayload(count: 100_001))
        try await store(record, in: context)

        let chunks = try await storedChunks(in: context)
        #expect(chunks.count == 2)
        #expect(chunks[0].1.count == 90_000)
        #expect(chunks[1].1.count == 10_001)
        #expect(try await load(from: context) == record)
    }

    @Test("chunk writes retain borrowed outcome slices")
    func retainsBorrowedOutcomeSlices() async throws {
        let context = try await makeContext(key: "borrowed")
        let owner = BorrowCountingByteStringOwner(
            Array(repeating: 0xA5, count: 100_001)
        )
        let record = makeRecord(outcome: ByteString(retaining: owner))
        let before = owner.borrowCount

        try await store(record, in: context)

        #expect(owner.borrowCount == before)
        _ = try await load(from: context)
        #expect(owner.borrowCount == before + 2)
    }

    @Test("the maximum configured outcome round-trips")
    func roundTripsMaximumOutcome() async throws {
        let context = try await makeContext(key: "maximum")
        let payload = makePayload(count: context.limits.maximumOutcomeBytes)
        try await store(makeRecord(outcome: payload), in: context)

        #expect(try await load(from: context)?.outcome == payload)
        #expect(
            try await storedChunks(in: context).allSatisfy {
                $0.1.count <= 90_000
            }
        )
    }

    @Test("replacing an outcome removes stale chunks")
    func replacementRemovesStaleChunks() async throws {
        let context = try await makeContext(key: "replacement")
        try await store(
            makeRecord(outcome: makePayload(count: 180_001)),
            in: context
        )
        #expect(try await storedChunks(in: context).count == 3)

        let replacement = makeRecord(outcome: [])
        try await store(replacement, in: context)

        #expect(try await storedChunks(in: context).isEmpty)
        #expect(try await load(from: context) == replacement)
    }

    @Test("metadata without required chunks is corrupted")
    func rejectsMetadataWithoutChunks() async throws {
        let context = try await makeContext(key: "metadata-only")
        let manifest = DatabaseMutationStateManifest(
            discriminator: [1],
            requestFingerprint: makeFingerprint(1),
            outcomeFingerprint: makeFingerprint(2),
            totalOutcomeBytes: 1,
            chunkCount: 1
        )
        try await writeMetadata(
            try manifest.encode(limits: context.limits),
            to: context
        )

        await expectCorruption(context)
    }

    @Test("chunks without metadata are corrupted")
    func rejectsChunksWithoutMetadata() async throws {
        let context = try await makeContext(key: "chunk-only")
        try await mutate(context) { transaction in
            try transaction.setValue([1], for: context.chunkKey(index: 0))
        }

        await expectCorruption(context)
    }

    @Test("a missing chunk is corrupted")
    func rejectsMissingChunk() async throws {
        let context = try await storedLargeContext(key: "missing")
        try await mutate(context) { transaction in
            try transaction.clear(key: context.chunkKey(index: 1))
        }

        await expectCorruption(context)
    }

    @Test("an extra chunk is corrupted")
    func rejectsExtraChunk() async throws {
        let context = try await storedLargeContext(key: "extra")
        try await mutate(context) { transaction in
            try transaction.setValue([1], for: context.chunkKey(index: 2))
        }

        await expectCorruption(context)
    }

    @Test("a truncated chunk is corrupted")
    func rejectsTruncatedChunk() async throws {
        let context = try await storedLargeContext(key: "truncated")
        try await mutate(context) { transaction in
            try transaction.setValue(
                ByteString(repeating: 1, count: 89_999),
                for: context.chunkKey(index: 0)
            )
        }

        await expectCorruption(context)
    }

    @Test("a non-canonical chunk index is corrupted")
    func rejectsNonCanonicalChunkIndex() async throws {
        let context = try await makeContext(key: "chunk-index")
        try await store(makeRecord(outcome: [1]), in: context)
        try await mutate(context) { transaction in
            try transaction.clear(key: context.chunkKey(index: 0))
            try transaction.setValue(
                [1],
                for: context.chunks.pack(Tuple("0"))
            )
        }

        await expectCorruption(context)
    }

    @Test("invalid fingerprint metadata is corrupted")
    func rejectsInvalidFingerprintMetadata() async throws {
        let context = try await makeContext(key: "fingerprint")
        let metadata = try encodeUncheckedManifest(
            discriminator: [1],
            requestFingerprint: [],
            outcomeFingerprint: [2],
            totalOutcomeBytes: 0,
            chunkByteCount: 90_000,
            chunkCount: 0
        )
        try await writeMetadata(metadata, to: context)

        await expectCorruption(context)
    }

    @Test("version 1 server metadata is rejected")
    func rejectsLegacyServerMetadataVersion() async throws {
        let context = try await makeContext(key: "legacy-version")
        let metadata = try encodeUncheckedManifest(
            version: 1,
            discriminator: [1],
            requestFingerprint: makeFingerprint(1),
            outcomeFingerprint: makeFingerprint(2),
            totalOutcomeBytes: 0,
            chunkByteCount: 90_000,
            chunkCount: 0
        )
        try await writeMetadata(metadata, to: context)

        await expectCorruption(context)
    }

    @Test("a non-canonical chunk size is corrupted")
    func rejectsInvalidChunkSize() async throws {
        let context = try await makeContext(key: "chunk-size")
        let metadata = try encodeUncheckedManifest(
            discriminator: [1],
            requestFingerprint: makeFingerprint(1),
            outcomeFingerprint: makeFingerprint(2),
            totalOutcomeBytes: 0,
            chunkByteCount: 100_000,
            chunkCount: 0
        )
        try await writeMetadata(metadata, to: context)

        await expectCorruption(context)
    }

    @Test("a chunk count inconsistent with its total is corrupted")
    func rejectsInvalidChunkCount() async throws {
        let context = try await makeContext(key: "chunk-count")
        let metadata = try encodeUncheckedManifest(
            discriminator: [1],
            requestFingerprint: makeFingerprint(1),
            outcomeFingerprint: makeFingerprint(2),
            totalOutcomeBytes: 1,
            chunkByteCount: 90_000,
            chunkCount: 2
        )
        try await writeMetadata(metadata, to: context)

        await expectCorruption(context)
    }

    @Test("metadata over the configured outcome limit is corrupted")
    func rejectsOutcomeAboveLimit() async throws {
        let context = try await makeContext(key: "outcome-limit")
        let total = UInt64(context.limits.maximumOutcomeBytes) + 1
        let chunkCount = try #require(
            DatabaseMutationStateManifest.expectedChunkCount(
                totalOutcomeBytes: total
            )
        )
        let metadata = try encodeUncheckedManifest(
            discriminator: [1],
            requestFingerprint: makeFingerprint(1),
            outcomeFingerprint: makeFingerprint(2),
            totalOutcomeBytes: total,
            chunkByteCount: 90_000,
            chunkCount: chunkCount
        )
        try await writeMetadata(metadata, to: context)

        await expectCorruption(context)
    }

    private func makeContext(key: String) async throws -> StoreContext {
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try EntityMutationFixture.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        EntityMutationFixture.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        let store = DatabaseMutationStateStore(container: container)
        let binding = store.controlBinding()
        let limits = try DatabaseMutationStateLimits(
            maximumKeyBytes: 512,
            maximumOutcomeBytes: 4 * 1_024 * 1_024,
            maximumChunkCount: 100_000
        )
        // Operation state is Framework metadata, so `controlBinding` roots it
        // below the control Partition's `system/database-framework` Directory.
        return StoreContext(
            storage: container.controlStorage(),
            store: store,
            binding: binding,
            limits: limits,
            key: key,
            entry: binding.root.subspace("idempotency").subspace(key)
        )
    }

    private func storedLargeContext(key: String) async throws -> StoreContext {
        let context = try await makeContext(key: key)
        try await store(
            makeRecord(outcome: makePayload(count: 100_001)),
            in: context
        )
        return context
    }

    private func makeRecord(
        outcome: ByteString
    ) -> DatabaseMutationReplayRecord {
        DatabaseMutationReplayRecord(
            discriminator: [1],
            requestFingerprint: makeFingerprint(1),
            outcomeFingerprint: makeFingerprint(2),
            outcome: outcome
        )
    }

    private func makeFingerprint(_ byte: UInt8) -> ByteString {
        ByteString(repeating: byte, count: 32)
    }

    private func makePayload(count: Int) -> ByteString {
        ByteString.copying(count: count) { destination in
            for index in 0..<count {
                destination[index] = UInt8(truncatingIfNeeded: index)
            }
        }
    }

    private func store(
        _ record: DatabaseMutationReplayRecord,
        in context: StoreContext
    ) async throws {
        try await mutate(context) { transaction in
            try context.store.store(
                record,
                for: context.key,
                in: context.binding,
                transaction: transaction,
                limits: context.limits
            )
        }
    }

    private func load(
        from context: StoreContext
    ) async throws -> DatabaseMutationReplayRecord? {
        try await withTransaction(context, configuration: .readOnly) {
            transaction in
            try await context.store.record(
                for: context.key,
                in: context.binding,
                transaction: transaction,
                limits: context.limits
            )
        }
    }

    private func storedChunks(
        in context: StoreContext
    ) async throws -> [(ByteString, ByteString)] {
        try await withTransaction(context, configuration: .readOnly) {
            transaction in
            let range = context.chunks.range()
            return try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                snapshot: true
            )
        }
    }

    private func mutate(
        _ context: StoreContext,
        _ body: @Sendable @escaping (any TransactionAccess) async throws -> Void
    ) async throws {
        try await withTransaction(context, body)
    }

    private func withTransaction<Result: Sendable>(
        _ context: StoreContext,
        configuration: TransactionConfiguration = .batch,
        _ body: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await context.storage.transactionExecutor.withTransaction(
            configuration: configuration,
            clock: TestProcessMonotonicClock(),
            body
        )
    }

    private func writeMetadata(
        _ metadata: ByteString,
        to context: StoreContext
    ) async throws {
        try await mutate(context) { transaction in
            try transaction.setValue(metadata, for: context.metadataKey)
        }
    }

    private func encodeUncheckedManifest(
        version: UInt16 = DatabaseMutationStateManifest.formatVersion,
        discriminator: ByteString,
        requestFingerprint: ByteString,
        outcomeFingerprint: ByteString,
        totalOutcomeBytes: UInt64,
        chunkByteCount: UInt32,
        chunkCount: UInt32
    ) throws -> ByteString {
        try StorageFrameEncoder.encode {
            (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
            encoder.writeUInt16(version)
            try encoder.writeBytes(discriminator)
            try encoder.writeBytes(requestFingerprint)
            try encoder.writeBytes(outcomeFingerprint)
            encoder.writeUInt64(totalOutcomeBytes)
            encoder.writeUInt32(chunkByteCount)
            encoder.writeUInt32(chunkCount)
        }
    }

    private func expectCorruption(_ context: StoreContext) async {
        do {
            _ = try await load(from: context)
            Issue.record("Expected persisted mutation state to be corrupted")
        } catch DatabaseMutationStateError.corruptedState {
            return
        } catch {
            Issue.record("Expected corruptedState, got \(error)")
        }
    }

    private struct StoreContext: Sendable {
        let storage: DatabaseExecutionStorage
        let store: DatabaseMutationStateStore
        let binding: DatabaseMutationStateBinding
        let limits: DatabaseMutationStateLimits
        let key: String
        let entry: Subspace

        var metadataKey: ByteString {
            entry.pack(Tuple("metadata"))
        }

        var chunks: Subspace {
            entry.subspace("chunks")
        }

        func chunkKey(index: UInt32) -> ByteString {
            chunks.pack(Tuple(UInt64(index)))
        }
    }
}
