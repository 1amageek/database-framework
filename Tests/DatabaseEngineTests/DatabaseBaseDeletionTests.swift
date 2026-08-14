#if MultipleBases
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@Persistable
private struct BaseDeletionEntity {
    #Directory<BaseDeletionEntity>("base-deletion", "entities")

    var id: String = ""
    var value: String = ""
}

@Suite("Base deletion lifecycle", .serialized)
struct DatabaseBaseDeletionTests {
    @Test("Deletion marker permits only the owning job to finish after Grants are cleared")
    func owningJobFinishesAfterGrantRemoval() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try context.insert(BaseDeletionEntity(id: "one", value: "retained"))
        try await context.save()

        let baseID = try TestBaseEnvironment.id()
        let retired = try await retire(baseID, container: container)
        let owner = ByteString((0..<16).map(UInt8.init))
        let deleting = try await container.executionPrepareBaseDeletion(
            baseID,
            expectedRevision: retired.revision,
            owner: owner
        )
        #expect(deleting.lifecycle == .deleting)

        _ = try await container.executionClearBaseForDeletion(
            baseID,
            owner: owner,
            authorization: TestBaseEnvironment.authorization
        )
        #expect(
            try await container.executionPermitsBaseDeletionFinalization(
                baseID,
                owner: owner
            )
        )
        #expect(
            try await container.executionPermitsBaseDeletionFinalization(
                baseID,
                owner: ByteString(repeating: 0xff, count: 16)
            ) == false
        )

        let tombstone = try await container.executionFinishBaseDeletion(
            baseID,
            owner: owner
        )
        let replayedTombstone = try await container.executionFinishBaseDeletion(
            baseID,
            owner: owner
        )
        #expect(replayedTombstone == tombstone)
        try await container.withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await container.executionFinalizeSuccessfulBaseDeletion(
                baseID,
                owner: owner,
                controlTransaction: transaction.storageAccess
            )
        }
        let remainingIntent = try await container
            .withControlMetadataTransaction(configuration: .readOnly) {
                transaction in
                try await DatabaseBaseDeletionStore(
                    root: container.storageTopology.controlDomain.root,
                    collection: "intents"
                ).load(baseID, transaction: transaction.storageAccess)
            }
        #expect(remainingIntent == nil)
        #expect(tombstone.lifecycle == .tombstone)
        #expect(tombstone.revision == retired.revision + 2)
        await #expect(throws: DatabaseBaseCatalogError.self) {
            try await container.provisionBase(
                baseID,
                placementID: container.storageTopology.defaultPlacementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal("test-runner"),
                        resource: .base(baseID),
                        access: .all
                    ),
                ],
                expectedRevision: 0
            )
        }
    }

    @Test("Unauthorized clear leaves data intact and cancellation restores retired state")
    func unauthorizedClearRollsBackDeletionIntent() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try context.insert(BaseDeletionEntity(id: "one", value: "retained"))
        try await context.save()

        let baseID = try TestBaseEnvironment.id()
        let retired = try await retire(baseID, container: container)
        let owner = ByteString(repeating: 0x22, count: 16)
        _ = try await container.executionPrepareBaseDeletion(
            baseID,
            expectedRevision: retired.revision,
            owner: owner
        )

        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await container.executionClearBaseForDeletion(
                baseID,
                owner: owner,
                authorization: .authenticated(
                    Principal(identifier: "intruder")
                )
            )
        }
        #expect(
            try await container.executionPermitsBaseDeletionFinalization(
                baseID,
                owner: owner
            ) == false
        )

        let restored = try await container
            .executionPrepareUnsuccessfulBaseDeletionRecovery(
                baseID,
                owner: owner
            )
        try await container.withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await container.executionFinalizeUnsuccessfulBaseDeletion(
                baseID,
                owner: owner,
                controlTransaction: transaction.storageAccess
            )
        }
        #expect(restored.lifecycle == .retired)
        let active = try await container.activateBase(
            baseID,
            expectedRevision: restored.revision,
            authorization: TestBaseEnvironment.authorization
        )
        #expect(active.lifecycle == .active)
        let values = try await container.testBaseContext()
            .fetch(BaseDeletionEntity.self)
            .execute()
        #expect(values.map(\.id) == ["one"])
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try BaseDeletionEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        BaseDeletionEntity.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func retire(
        _ id: Base.ID,
        container: DBContainer
    ) async throws -> DatabaseBaseRecord {
        let active = try await record(id, container: container)
        return try await container.retireBase(
            id,
            expectedRevision: active.revision
        )
    }

    private func record(
        _ id: Base.ID,
        container: DBContainer
    ) async throws -> DatabaseBaseRecord {
        try await container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try #require(
                try await container.baseCatalog.load(
                    id,
                    transaction: transaction.storageAccess
                )
            )
        }
    }
}
#endif
