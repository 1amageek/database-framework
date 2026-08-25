#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseRuntime
import FDBStorage
import Foundation
import StorageKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite(
    "OnlineIndexer durable atomicity",
    .tags(.requiresFDB),
    .foundationDBScenario,
    .serialized,
    .heartbeat
)
struct OnlineIndexerAtomicityTests {
    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    struct AtomicIndexingContext: Sendable {
        let container: DBContainer
        let testSubspace: Subspace
        let itemSubspace: Subspace
        let blobsSubspace: Subspace

        init(
            index: ResolvedIndex,
            storageEngine: any StorageEngine
        ) async throws {
            let testIdentifier = UUID().uuidString.prefix(8)
            self.testSubspace = Subspace(
                prefix: Tuple(
                    "test",
                    "atomicity",
                    String(testIdentifier)
                ).pack()
            )
            self.itemSubspace = testSubspace.subspace("R")
            self.blobsSubspace = testSubspace.subspace("B")
            self.container = try await Self.openContainer(
                index: index,
                storageEngine: storageEngine
            )
        }

        static func openContainer(
            index: ResolvedIndex,
            storageEngine: any StorageEngine
        ) async throws -> DBContainer {
            let schema = try Schema(
                entities: [
                    try Schema.Entity(
                        from: Player.self,
                        including: [index.descriptor]
                    )
                ],
                version: Schema.Version(1, 0, 0)
            )
            return try await DBContainer.open(
                for: schema,
                configuration: .testing(storageEngine: storageEngine),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [
                        try DatabaseFrameworkRuntime.entity(
                            Player.self,
                            including: [index.descriptor]
                        )
                    ]
                ),
                security: .testingDisabled
            )
        }

        func insertPlayers(_ players: [Player]) async throws {
            for batchStart in stride(
                from: 0,
                to: players.count,
                by: 50
            ) {
                let batchEnd = min(batchStart + 50, players.count)
                try await container.engine.withTransaction { transaction in
                    let storage = ItemStorage(
                        transaction: transaction,
                        blobsSubspace: blobsSubspace,
                        configuration: .v1
                    )
                    for player in players[batchStart..<batchEnd] {
                        let key = itemSubspace
                            .subspace(Player.persistableType)
                            .pack(Tuple(player.id))
                        try await storage.write(
                            try DataAccess.serialize(player),
                            for: key
                        )
                    }
                }
            }
        }

        func removeTestData(using container: DBContainer? = nil) async throws {
            let selectedContainer = container ?? self.container
            try await selectedContainer.engine.withTransaction { transaction in
                let range = testSubspace.range()
                try transaction.clearRange(
                    beginKey: range.begin,
                    endKey: range.end
                )
            }
        }
    }

    @Test("Failed batch persists neither index work nor progress")
    func failedBatchPersistsNeitherWorkNorProgress() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "failed_batch_atomicity_idx"
            )
            let control = StorageTransactionControl()
            let baseEngine = try await FoundationDBScenarioCoordinator.shared
                .makeEngine()
            let controlledEngine = ControlledStorageEngine(
                base: baseEngine,
                control: control
            )
            let context = try await AtomicIndexingContext(
                index: index,
                storageEngine: controlledEngine
            )
            let players = PlayerDatasetGenerator.generatePlayers(count: 5)
            try await context.insertPlayers(players)

            let lifecycleStore = IndexLifecycleStore(
                container: context.container,
                subspace: context.testSubspace
            )
            let physicalIndexSubspace = try lifecycleStore.indexSubspace(
                for: index.name
            )
            let progressKey = physicalIndexSubspace
                .subspace("_progress")
                .pack(Tuple(index.name))
            let maintainer = CountingIndexMaintainer<Player>(
                indexSubspace: physicalIndexSubspace
            )
            try await lifecycleStore.enable(index.name)

            let injectedFailure = StorageError(
                code: .backendFailure,
                operation: .commit,
                backend: .foundationDB,
                message: "Injected online-index batch commit failure"
            )
            control.failNextMutatingCommit(with: injectedFailure)
            let indexer = try OnlineIndexer(
                container: context.container,
                storeSubspace: context.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: maintainer,
                indexLifecycleStore: lifecycleStore,
                batchSize: 3
            )

            do {
                try await indexer.buildIndex()
                Issue.record("Expected the controlled batch commit to fail")
            } catch let failure as StorageError {
                #expect(failure == injectedFailure)
            } catch {
                Issue.record("Expected StorageError, got \(error)")
            }

            let stagedKeys = control.lastInterceptedMutationKeys
            #expect(stagedKeys.contains(progressKey))
            #expect(stagedKeys.contains { key in
                key != progressKey
                    && Self.contains(key, in: physicalIndexSubspace.range())
            })
            #expect(
                try await Self.rows(
                    in: physicalIndexSubspace,
                    using: context.container
                ).isEmpty
            )
            #expect(try await lifecycleStore.state(of: index.name) == .writeOnly)

            try await context.removeTestData()
        }
    }

    @Test("A reopened container resumes after a failed batch")
    func reopenedContainerResumesAfterFailedBatch() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "durable_resume_idx"
            )
            let control = StorageTransactionControl()
            let firstBaseEngine = try await FoundationDBScenarioCoordinator
                .shared.makeEngine()
            let firstEngine = ControlledStorageEngine(
                base: firstBaseEngine,
                control: control
            )
            let context = try await AtomicIndexingContext(
                index: index,
                storageEngine: firstEngine
            )
            let players = PlayerDatasetGenerator.generatePlayers(count: 7)
            try await context.insertPlayers(players)

            let firstLifecycleStore = IndexLifecycleStore(
                container: context.container,
                subspace: context.testSubspace
            )
            let firstPhysicalIndexSubspace = try firstLifecycleStore
                .indexSubspace(for: index.name)
            try await firstLifecycleStore.enable(index.name)
            let injectedFailure = StorageError(
                code: .backendFailure,
                operation: .commit,
                backend: .foundationDB,
                message: "Injected online-index resume failure"
            )
            control.failNextMutatingCommit(with: injectedFailure)
            let failingIndexer = try OnlineIndexer(
                container: context.container,
                storeSubspace: context.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: CountingIndexMaintainer<Player>(
                    indexSubspace: firstPhysicalIndexSubspace
                ),
                indexLifecycleStore: firstLifecycleStore,
                batchSize: 3
            )
            do {
                try await failingIndexer.buildIndex()
                Issue.record("Expected the controlled batch commit to fail")
            } catch let failure as StorageError {
                #expect(failure == injectedFailure)
            } catch {
                Issue.record("Expected StorageError, got \(error)")
            }
            await context.container.shutdown()

            let reopenedEngine = try await FoundationDBScenarioCoordinator
                .shared.makeEngine()
            let reopenedContainer = try await AtomicIndexingContext.openContainer(
                index: index,
                storageEngine: reopenedEngine
            )
            let reopenedLifecycleStore = IndexLifecycleStore(
                container: reopenedContainer,
                subspace: context.testSubspace
            )
            let reopenedPhysicalIndexSubspace = try reopenedLifecycleStore
                .indexSubspace(for: index.name)
            #expect(
                reopenedPhysicalIndexSubspace.range().begin
                    == firstPhysicalIndexSubspace.range().begin
            )
            #expect(
                try await reopenedLifecycleStore.state(of: index.name)
                    == .writeOnly
            )

            let resumedIndexer = try OnlineIndexer(
                container: reopenedContainer,
                storeSubspace: context.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: CountingIndexMaintainer<Player>(
                    indexSubspace: reopenedPhysicalIndexSubspace
                ),
                indexLifecycleStore: reopenedLifecycleStore,
                batchSize: 3
            )
            try await resumedIndexer.buildIndex()

            let rows = try await Self.rows(
                in: reopenedPhysicalIndexSubspace,
                using: reopenedContainer
            )
            let expectedKeys = Set(players.map {
                reopenedPhysicalIndexSubspace.pack(Tuple($0.id))
            })
            #expect(Set(rows.map(\.0)) == expectedKeys)
            #expect(rows.allSatisfy { $0.1 == ByteString([0x01]) })
            #expect(
                try await reopenedLifecycleStore.state(of: index.name)
                    == .readable
            )

            try await context.removeTestData(using: reopenedContainer)
            await reopenedContainer.shutdown()
        }
    }

    @Test("Successful completion clears progress and publishes readability")
    func successfulCompletionClearsProgressAndPublishesReadability() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let index = try PlayerIdentifierIndexDefinition.make(
                name: "completed_progress_idx"
            )
            let engine = try await FoundationDBScenarioCoordinator.shared
                .makeEngine()
            let context = try await AtomicIndexingContext(
                index: index,
                storageEngine: engine
            )
            let players = PlayerDatasetGenerator.generatePlayers(count: 7)
            try await context.insertPlayers(players)

            let lifecycleStore = IndexLifecycleStore(
                container: context.container,
                subspace: context.testSubspace
            )
            let physicalIndexSubspace = try lifecycleStore.indexSubspace(
                for: index.name
            )
            let progressKey = physicalIndexSubspace
                .subspace("_progress")
                .pack(Tuple(index.name))
            try await lifecycleStore.enable(index.name)
            let indexer = try OnlineIndexer(
                container: context.container,
                storeSubspace: context.testSubspace,
                itemType: Player.persistableType,
                index: index,
                indexMaintainer: CountingIndexMaintainer<Player>(
                    indexSubspace: physicalIndexSubspace
                ),
                indexLifecycleStore: lifecycleStore,
                batchSize: 3
            )

            try await indexer.buildIndex()

            let progress = try await context.container.engine.withTransaction {
                transaction in
                try await transaction.getValue(
                    for: progressKey,
                    snapshot: false
                )
            }
            #expect(progress == nil)
            #expect(try await lifecycleStore.state(of: index.name) == .readable)
            #expect(
                try await Self.rows(
                    in: physicalIndexSubspace,
                    using: context.container
                ).count == players.count
            )

            try await context.removeTestData()
        }
    }

    private static func rows(
        in subspace: Subspace,
        using container: DBContainer
    ) async throws -> [(ByteString, ByteString)] {
        let range = subspace.range()
        return try await container.engine.withTransaction { transaction in
            try await transaction.collectRange(
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                snapshot: true
            )
        }
    }

    private static func contains(
        _ key: ByteString,
        in range: (begin: ByteString, end: ByteString)
    ) -> Bool {
        (key == range.begin || range.begin.lexicographicallyPrecedes(key))
            && key.lexicographicallyPrecedes(range.end)
    }
}
#endif
#endif
