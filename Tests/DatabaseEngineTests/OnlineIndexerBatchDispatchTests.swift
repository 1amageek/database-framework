import Testing
import Foundation
import StorageKit
import DatabaseKit
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
import ScalarIndex

@Suite("OnlineIndexer Batch Dispatch Tests", .heartbeat)
struct OnlineIndexerBatchDispatchTests {
    @Test("Unique build records existing duplicates before readable transition")
    func uniqueBuildRejectsExistingDuplicates() async throws {
        let database = InMemoryEngine()
        let storeSubspace = Subspace(
            prefix: Tuple("test", "onlineindexer", "unique", UUID().uuidString).pack()
        )
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)
        let schema = try Schema(
            entities: [try Player.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [Player.self]
            ),
            security: .disabled
        )
        let index = Index(
            name: "unique_player_name_idx",
            kind: IndexKindMetadata(
                identifier: IndexDefinition.scalar.identifier,
                subspaceStructure: .flat,
                fields: [Player.fields.name.ascending.metadata],
                metadata: [:]
            ),
            rootExpression: FieldKeyExpression(fieldName: "name"),
            isUnique: true
        )
        let maintainer = ScalarIndexMaintainer<Player>(
            index: index,
            subspace: indexSubspace.subspace(index.subspaceKey),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )
        try await lifecycleStore.enable(index.name)

        var first = Player(name: "duplicate", score: 1, level: 1)
        first.id = "first"
        var second = Player(name: "duplicate", score: 2, level: 1)
        second.id = "second"
        try await database.withTransaction { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: blobsSubspace,
                configuration: .v1
            )
            for player in [first, second] {
                let key = itemSubspace
                    .subspace(Player.persistableType)
                    .pack(Tuple(player.id))
                try await storage.write(
                    try DataAccess.serialize(player),
                    for: key
                )
            }
        }

        let indexer = try OnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            uniquenessMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore,
            batchSize: 10
        )
        await #expect(
            throws: OnlineIndexBuildError.uniquenessViolationsDetected(
                indexName: index.name,
                violationCount: 1,
                totalConflictingEntities: 2
            )
        ) {
            try await indexer.buildIndex(clearFirst: true)
        }
        #expect(try await lifecycleStore.state(of: index.name) == .writeOnly)
        let tracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: storeSubspace.subspace(SubspaceKey.metadata)
        )
        let summary = try await tracker.violationSummary(indexName: index.name)
        #expect(summary.violationCount == 1)
        #expect(summary.totalConflictingEntities == 2)
    }

    @Test("Invalid build configuration fails before index state changes")
    func invalidBuildConfigurationFailsBeforeStateChanges() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(
            prefix: Tuple("test", "onlineindexer", "configuration", testId).pack()
        )
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let schema = try Schema(
            entities: [try Player.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [Player.self]
            ),
            security: .disabled
        )
        let index = PlayerIdentifierIndexDefinition.make(
            name: "invalid_configuration_idx"
        )
        let maintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: index.name
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        #expect(throws: OnlineIndexBuildError.invalidBatchSize(0)) {
            _ = try OnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: 0
            )
        }
        #expect(
            throws: OnlineIndexBuildError.invalidThrottleDelayMilliseconds(-1)
        ) {
            _ = try OnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                throttleDelayMs: -1
            )
        }
        let unsupportedUniqueIndex = Index(
            name: "unsupported_unique_idx",
            kind: index.kind,
            rootExpression: index.rootExpression,
            isUnique: true
        )
        #expect(
            throws: OnlineIndexBuildError.unsupportedUniquenessConstraint(
                indexName: unsupportedUniqueIndex.name
            )
        ) {
            _ = try OnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                index: unsupportedUniqueIndex,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore
            )
        }

        let indexer = try OnlineIndexer<Player>(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore
        )
        await #expect(
            throws: OnlineIndexBuildError.invalidMaximumConcurrency(0)
        ) {
            try await indexer.buildIndexInParallel(maxConcurrency: 0)
        }
        await #expect(
            throws: OnlineIndexBuildError.invalidChunkSizeBytes(0)
        ) {
            try await indexer.buildIndexInParallel(chunkSizeBytes: 0)
        }

        #expect(throws: OnlineIndexBuildError.emptyTargetSet) {
            _ = try MultiTargetOnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                targets: [],
                lifecycleStore: lifecycleStore
            )
        }
        #expect(
            throws: OnlineIndexBuildError.duplicateTargetIndexName(index.name)
        ) {
            let target = IndexBuildTarget(index: index, maintainer: maintainer)
            _ = try MultiTargetOnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                targets: [target, target],
                lifecycleStore: lifecycleStore
            )
        }
        #expect(throws: OnlineIndexBuildError.invalidBatchSize(0)) {
            _ = try MutualOnlineIndexer<Player>(
                container: container,
                storeSubspace: storeSubspace,
                itemType: Player.persistableType,
                forwardIndex: index,
                reverseIndex: PlayerIdentifierIndexDefinition.make(
                    name: "invalid_configuration_reverse_idx"
                ),
                forwardMaintainer: maintainer,
                reverseMaintainer: maintainer,
                lifecycleStore: lifecycleStore,
                batchSize: 0
            )
        }
        #expect(try await lifecycleStore.state(of: index.name) == .disabled)
    }

    @Test("Build index dispatches complete batches to scanItems")
    func buildIndexDispatchesBatchesToScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "batch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

        let schema = try Schema(entities: [try Player.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [Player.self]),
            security: .disabled
        )

        let batchSize = 5
        let players = PlayerDatasetGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let index = PlayerIdentifierIndexDefinition.make(name: "batch_hook_idx")
        let maintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: index.name
        )

        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        try await lifecycleStore.enable(index.name)

        let indexer = try OnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            index: index,
            indexMaintainer: maintainer,
            indexLifecycleStore: lifecycleStore,
            batchSize: batchSize
        )

        try await indexer.buildIndex(clearFirst: true)

        #expect(maintainer.getBatchSizes() == [5, 5, 2])
        #expect(maintainer.getScanItemCallCount() == 0)
        #expect(maintainer.getUniqueProcessedCount() == players.count)
    }

    @Test("Multi-target build dispatches complete batches to each scanItems")
    func multiTargetBuildDispatchesBatchesToEachScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "multiBatch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

        let schema = try Schema(entities: [try Player.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [Player.self]),
            security: .disabled
        )

        let batchSize = 5
        let players = PlayerDatasetGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let firstIndex = PlayerIdentifierIndexDefinition.make(name: "multi_batch_hook_1")
        let secondIndex = PlayerIdentifierIndexDefinition.make(name: "multi_batch_hook_2")
        let firstMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: firstIndex.name
        )
        let secondMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: secondIndex.name
        )

        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        let indexer = try MultiTargetOnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            targets: [
                IndexBuildTarget(index: firstIndex, maintainer: firstMaintainer),
                IndexBuildTarget(index: secondIndex, maintainer: secondMaintainer)
            ],
            lifecycleStore: lifecycleStore,
            batchSize: batchSize
        )

        try await indexer.buildIndexes(clearFirst: true)

        #expect(firstMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(secondMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(firstMaintainer.getScanItemCallCount() == 0)
        #expect(secondMaintainer.getScanItemCallCount() == 0)
        #expect(firstMaintainer.getUniqueProcessedCount() == players.count)
        #expect(secondMaintainer.getUniqueProcessedCount() == players.count)
    }

    @Test("Mutual build dispatches complete batches to both scanItems")
    func mutualBuildDispatchesBatchesToBothScanItems() async throws {
        let database = InMemoryEngine()
        let testId = String(UUID().uuidString.prefix(8))
        let storeSubspace = Subspace(prefix: Tuple("test", "onlineindexer", "mutualBatch", testId).pack())
        let itemSubspace = storeSubspace.subspace(SubspaceKey.items)
        let indexSubspace = storeSubspace.subspace(SubspaceKey.indexes)
        let blobsSubspace = storeSubspace.subspace(SubspaceKey.blobs)

        let schema = try Schema(entities: [try Player.schemaEntity], version: Schema.Version(1, 0, 0))
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [Player.self]),
            security: .disabled
        )

        let batchSize = 5
        let players = PlayerDatasetGenerator.generateForBatchTesting(
            batchSize: batchSize,
            batches: 2,
            remainder: 2
        )

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            for player in players {
                let key = itemSubspace.subspace(Player.persistableType).pack(Tuple(player.id))
                let value = try DataAccess.serialize(player)
                try await storage.write(value, for: key)
            }
        }

        let forwardIndex = PlayerIdentifierIndexDefinition.make(name: "mutual_batch_forward")
        let reverseIndex = PlayerIdentifierIndexDefinition.make(name: "mutual_batch_reverse")
        let forwardMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: forwardIndex.name
        )
        let reverseMaintainer = BatchTrackingIndexMaintainer<Player>(
            indexSubspace: indexSubspace,
            indexName: reverseIndex.name
        )

        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: indexSubspace.subspace("_meta")
        )

        let indexer = try MutualOnlineIndexer(
            container: container,
            storeSubspace: storeSubspace,
            itemType: Player.persistableType,
            forwardIndex: forwardIndex,
            reverseIndex: reverseIndex,
            forwardMaintainer: forwardMaintainer,
            reverseMaintainer: reverseMaintainer,
            lifecycleStore: lifecycleStore,
            batchSize: batchSize
        )

        try await indexer.buildIndexes(clearFirst: true)

        #expect(forwardMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(reverseMaintainer.getBatchSizes() == [5, 5, 2])
        #expect(forwardMaintainer.getScanItemCallCount() == 0)
        #expect(reverseMaintainer.getScanItemCallCount() == 0)
        #expect(forwardMaintainer.getUniqueProcessedCount() == players.count)
        #expect(reverseMaintainer.getUniqueProcessedCount() == players.count)
    }
}
