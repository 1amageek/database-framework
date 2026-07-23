#if !os(WASI)
#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime

@Suite("Storage Invariant Tests", .tags(.requiresFDB), .serialized, .heartbeat)
struct StorageInvariantTests {

    private func openDB() async throws -> any StorageEngine {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        return try await FoundationDBScenarioCoordinator.shared.makeEngine()
    }

    @Test("OnlineIndexer.clearFirst clears uniqueness violations stored under metadata subspace")
    func onlineIndexerClearFirstClearsViolationsInMetadataSubspace() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await openDB()

            let testId = String(UUID().uuidString.prefix(8))
            let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "violations", testId).pack())
            let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
            let metadataSubspace = storeSubspace.subspace(SubspaceKey.metadata)

            // Create container for components that need it
            let schema = Schema([Player.self], version: Schema.Version(1, 0, 0))
            let container = try await DBContainer.open(for: schema, configuration: .init(backend: .custom(database)), runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(), security: .disabled)

            let tracker = UniquenessViolationTracker(container: container, metadataSubspace: metadataSubspace)
            let indexName = "unique_clearFirst_idx"

            // Seed a violation in the correct metadata subspace.
            try await database.withTransaction { tx in
                try await tracker.recordViolation(
                    indexName: indexName,
                    persistableType: "TestType",
                    valueKey: Tuple("dup").pack(),
                    existingPrimaryKey: Tuple("pk1"),
                    newPrimaryKey: Tuple("pk2"),
                    transaction: tx
                )
            }

            #expect(try await tracker.hasViolations(indexName: indexName) == true)

            // Build an OnlineIndexer with clearFirst=true. For unique indexes, it must clear violations.
            let index = Index(
                name: indexName,
                kind: ScalarIndexKind<Player>(fields: [\Player.id]),
                rootExpression: FieldKeyExpression(fieldName: "id"),
                isUnique: true
            )

            let maintainer = CountingIndexMaintainer<Player>(indexSubspace: indexSubspace, indexName: index.name)
            let lifecycleStore = IndexLifecycleStore(container: container, subspace: indexSubspace.subspace("_meta"))
            try await lifecycleStore.enable(index.name)

            let indexer = OnlineIndexer(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: 10
            )

            try await indexer.buildIndex(clearFirst: true)

            // If OnlineIndexer targets the wrong metadata subspace, this stays true.
            #expect(try await tracker.hasViolations(indexName: indexName) == false)

            // Cleanup
            try await database.withTransaction { tx in
                let (b, e) = storeSubspace.range()
                try tx.clearRange(beginKey: b, endKey: e)
            }
        }
    }

    @Test("ItemStorage overwrite clears orphan blobs even if existing item is not an ItemEnvelope")
    func itemStorageOverwriteClearsOrphanBlobsForNonEnvelopeExistingItem() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await openDB()

            let testId = String(UUID().uuidString.prefix(8))
            let storeSubspace = Subspace(prefix: Tuple("test", "itemstorage", "overwrite", testId).pack())
            let itemsSubspace = storeSubspace.subspace(SubspaceKey.items).subspace("T")
            let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

            let key = itemsSubspace.pack(Tuple("k"))

            // Seed non-envelope item bytes and orphan blobs.
            try await database.withTransaction { tx in
                try tx.setValue(Bytes("raw".utf8), for: key) // NOT an ItemEnvelope

                let blobBase = blobsSubspace.subspace(Tuple([key]))
                try tx.setValue([0xAA], for: blobBase.pack(Tuple([Int32(0)])))
                try tx.setValue([0xBB], for: blobBase.pack(Tuple([Int32(1)])))
            }

            // Overwrite via ItemStorage.
            try await database.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace, configuration: .v1)
                try await storage.write(Bytes("new".utf8), for: key)
            }

            // Orphan blobs must be gone.
            let blobCount = try await database.withTransaction { tx in
                let blobBase = blobsSubspace.subspace(Tuple([key]))
                let (b, e) = blobBase.range()
                var count = 0
                for try await _ in tx.getRange(begin: b, end: e, snapshot: true) {
                    count += 1
                }
                return count
            }

            #expect(blobCount == 0)

            // Cleanup
            try await database.withTransaction { tx in
                let (b, e) = storeSubspace.range()
                try tx.clearRange(beginKey: b, endKey: e)
            }
        }
    }

    @Test("ItemStorage delete clears orphan blobs even if existing item is not an ItemEnvelope")
    func itemStorageDeleteClearsOrphanBlobsForNonEnvelopeExistingItem() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await openDB()

            let testId = String(UUID().uuidString.prefix(8))
            let storeSubspace = Subspace(prefix: Tuple("test", "itemstorage", "delete", testId).pack())
            let itemsSubspace = storeSubspace.subspace(SubspaceKey.items).subspace("T")
            let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

            let key = itemsSubspace.pack(Tuple("k"))

            // Seed non-envelope item bytes and orphan blobs.
            try await database.withTransaction { tx in
                try tx.setValue(Bytes("raw".utf8), for: key) // NOT an ItemEnvelope

                let blobBase = blobsSubspace.subspace(Tuple([key]))
                try tx.setValue([0xAA], for: blobBase.pack(Tuple([Int32(0)])))
                try tx.setValue([0xBB], for: blobBase.pack(Tuple([Int32(1)])))
            }

            // Delete via ItemStorage.
            try await database.withTransaction { tx in
                let storage = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace, configuration: .v1)
                try await storage.delete(for: key)
            }

            // Orphan blobs must be gone.
            let blobCount = try await database.withTransaction { tx in
                let blobBase = blobsSubspace.subspace(Tuple([key]))
                let (b, e) = blobBase.range()
                var count = 0
                for try await _ in tx.getRange(begin: b, end: e, snapshot: true) {
                    count += 1
                }
                return count
            }

            #expect(blobCount == 0)

            // Cleanup
            try await database.withTransaction { tx in
                let (b, e) = storeSubspace.range()
                try tx.clearRange(beginKey: b, endKey: e)
            }
        }
    }

    @Test("ItemStorage.scan does not prefetch entire range before yielding first element")
    func itemStorageScanIsStreaming() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let database = try await openDB()

            let testId = String(UUID().uuidString.prefix(8))
            let storeSubspace = Subspace(prefix: Tuple("test", "itemstorage", "scan_streaming", testId).pack())
            let itemsSubspace = storeSubspace.subspace(SubspaceKey.items).subspace("T")
            let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

            // Insert multiple items, then scan with a transaction wrapper that:
            // - forces 1 KV per range batch
            // - throws if scan performs > 1 native call (prefetching all results)
            try await database.withTransaction { tx in
                let writer = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace, configuration: .v1)
                for i in 0..<5 {
                    let key = itemsSubspace.pack(Tuple("k\(i)"))
                    try await writer.write(Bytes("v\(i)".utf8), for: key)
                }

                // Allow up to 2 calls because AsyncKVSequence may prefetch the next batch.
                let limiting = LimitingTransaction(wrapping: tx, maxCollectCalls: 2)
                let storage = ItemStorage(transaction: limiting, blobsSubspace: blobsSubspace, configuration: .v1)
                let (b, e) = itemsSubspace.range()

                var it = storage.scan(begin: b, end: e, snapshot: false, limit: 0, reverse: false).makeAsyncIterator()
                let first = try await it.next()
                #expect(first != nil)
                #expect(limiting.callCountValue <= 2)
            }

            // Cleanup
            try await database.withTransaction { tx in
                let (b, e) = storeSubspace.range()
                try tx.clearRange(beginKey: b, endKey: e)
            }
        }
    }

    @Test("ItemEnvelope header contains the complete canonical v1 contract")
    func itemEnvelopeHeaderContainsCanonicalContract() throws {
        #expect(ItemEnvelope.headerSize == 27)

        let payload: Bytes = [0x01, 0x02]
        let checksum = ItemChecksum.crc32c(payload)
        let bytes = try ItemEnvelope.inline(
            payload: payload,
            encoding: .identity,
            plainByteCount: UInt64(payload.count),
            checksum: checksum
        ).serialize()
        #expect(bytes.count == ItemEnvelope.headerSize + 2)
        #expect(Array(bytes.prefix(4)) == ItemEnvelope.magic)
        #expect(bytes[4] == ItemEnvelope.currentVersion)
        #expect(bytes[5] == ItemEnvelope.StorageKind.inline.rawValue)
        #expect(bytes[6] == ItemPayloadEncoding.identity.rawValue)
        #expect(bytes[14] == UInt8(payload.count))
        #expect(bytes[22] == UInt8(payload.count))
        #expect(Array(bytes[23..<27]) == [
            UInt8(truncatingIfNeeded: checksum >> 24),
            UInt8(truncatingIfNeeded: checksum >> 16),
            UInt8(truncatingIfNeeded: checksum >> 8),
            UInt8(truncatingIfNeeded: checksum)
        ])
    }
}
#endif

#endif
