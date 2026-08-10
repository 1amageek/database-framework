@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@Persistable
private struct BasePlacementMoveEntity {
    #Directory<BasePlacementMoveEntity>("base-placement", "entities")

    var id: String = ""
    var title: String = ""
    var priority: Int64 = 0
}

@Suite("Base placement moves", .serialized)
struct DatabaseBasePlacementMoveTests {
    @Test("retired Base moves across domains with verified data and Grants")
    func crossDomainMove() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }

        let context = fixture.container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(fixture.baseID).newContext()
        for index in 0..<20 {
            var entity = BasePlacementMoveEntity()
            entity.id = "person-\(index)"
            entity.title = "Person \(index)"
            entity.priority = Int64(index)
            try context.insert(entity)
        }
        try await context.save()

        let active = try await record(
            fixture.baseID,
            container: fixture.container
        )
        let retired = try await fixture.container.retireBase(
            fixture.baseID,
            expectedRevision: active.revision
        )
        #expect(retired.lifecycle == .retired)

        let owner = ByteString((0..<16).map(UInt8.init))
        let descriptor = try await fixture.container.prepareBasePlacementMove(
            fixture.baseID,
            destinationPlacementID: fixture.destinationPlacementID,
            expectedRevision: retired.revision,
            owner: owner
        )
        let copied = try await transfer(
            descriptor,
            destination: nil,
            container: fixture.container
        )
        let source = try await transfer(
            descriptor,
            destination: false,
            container: fixture.container
        )
        let destination = try await transfer(
            descriptor,
            destination: true,
            container: fixture.container
        )
        #expect(copied.keyCount == source.keyCount)
        #expect(source.keyCount == destination.keyCount)
        #expect(source.byteCount == destination.byteCount)
        #expect(source.digest == destination.digest)

        let moved = try await fixture.container.cutOverBasePlacementMove(
            descriptor
        )
        #expect(moved.lifecycle == .retired)
        #expect(moved.placementID == fixture.destinationPlacementID)

        await #expect(throws: DatabaseBaseCatalogError.self) {
            try await fixture.container.finishBasePlacementMove(
                descriptor,
                owner: ByteString(repeating: 0xff, count: 16)
            )
        }
        let sourceBeforeOwningCleanup = try await transfer(
            descriptor,
            destination: false,
            container: fixture.container
        )
        #expect(sourceBeforeOwningCleanup.keyCount > 0)

        let cleaned = try await fixture.container.finishBasePlacementMove(
            descriptor,
            owner: owner
        )
        let replayedCleanup = try await fixture.container.finishBasePlacementMove(
            descriptor,
            owner: owner
        )
        #expect(replayedCleanup == cleaned)
        try await fixture.container.withControlMetadataTransaction(
            configuration: .batch
        ) { transaction in
            try await fixture.container.finalizeSuccessfulBasePlacementMove(
                descriptor,
                owner: owner,
                controlTransaction: transaction.storageAccess
            )
        }
        await #expect(throws: DatabaseBaseCatalogError.self) {
            try await fixture.container.finishBasePlacementMove(
                descriptor,
                owner: owner
            )
        }
        let activeAtDestination = try await fixture.container.activateBase(
            fixture.baseID,
            expectedRevision: cleaned.revision,
            authorization: TestBaseEnvironment.authorization
        )
        #expect(activeAtDestination.placementID == fixture.destinationPlacementID)

        let destinationContext = fixture.container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(fixture.baseID).newContext()
        let values = try await destinationContext
            .fetch(BasePlacementMoveEntity.self)
            .execute()
        #expect(values.count == 20)
        #expect(Set(values.map(\.id)).count == 20)

        let sourceAfterCleanup = try await transfer(
            descriptor,
            destination: false,
            container: fixture.container
        )
        #expect(sourceAfterCleanup.keyCount == 0)
        #expect(sourceAfterCleanup.byteCount == 0)
    }

    private struct Fixture {
        let container: DBContainer
        let baseID: Base.ID
        let destinationPlacementID: Base.Placement.ID
    }

    private func makeFixture() async throws -> Fixture {
        let sourceDomainID = try DatabaseStorageDomain.ID("source")
        let destinationDomainID = try DatabaseStorageDomain.ID("destination")
        let sourcePlacementID = try Base.Placement.ID("source")
        let destinationPlacementID = try Base.Placement.ID("destination")
        let baseID = try TestBaseEnvironment.id()
        let principal = Principal(
            identifier: "test-runner",
            roles: ["test-runner"]
        )
        let topology = try DatabaseStorageTopology(
            controlDomainID: sourceDomainID,
            domains: [
                try DatabaseStorageDomain(
                    id: sourceDomainID,
                    namespacePath: ["database", "source"],
                    storageEngine: InMemoryEngine()
                ),
                try DatabaseStorageDomain(
                    id: destinationDomainID,
                    namespacePath: ["database", "destination"],
                    storageEngine: InMemoryEngine()
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: sourcePlacementID,
                    domainID: sourceDomainID,
                    path: ["bases"]
                ),
                try DatabaseStoragePlacement(
                    id: destinationPlacementID,
                    domainID: destinationDomainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: sourcePlacementID
        )
        let configuration = DBConfiguration(
            name: "base-placement-move",
            storageTopology: topology,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock()
        )
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try BasePlacementMoveEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: configuration,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        BasePlacementMoveEntity.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
        _ = try await container.provisionBase(
            baseID,
            placementID: sourcePlacementID,
            initialGrants: [
                Security.Grant(
                    subject: .principal(principal.identifier),
                    resource: .base(baseID),
                    access: .all
                ),
            ],
            expectedRevision: 0
        )
        return Fixture(
            container: container,
            baseID: baseID,
            destinationPlacementID: destinationPlacementID
        )
    }

    private func transfer(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        destination: Bool?,
        container: DBContainer
    ) async throws -> DatabaseBasePlacementTransferProgress {
        var continuation: ByteString?
        var digest: ByteString?
        var keyCount: UInt64 = 0
        var byteCount: UInt64 = 0
        while true {
            let progress: DatabaseBasePlacementTransferProgress
            if let destination {
                progress = try await container.verifyBasePlacementBatch(
                    descriptor,
                    destination: destination,
                    continuation: continuation,
                    digest: digest,
                    keyCount: keyCount,
                    byteCount: byteCount
                )
            } else {
                progress = try await container.copyBasePlacementBatch(
                    descriptor,
                    continuation: continuation,
                    digest: digest,
                    keyCount: keyCount,
                    byteCount: byteCount
                )
            }
            continuation = progress.continuation
            digest = progress.digest
            keyCount = progress.keyCount
            byteCount = progress.byteCount
            if progress.isComplete { return progress }
        }
    }

    private func record(
        _ id: Base.ID,
        container: DBContainer
    ) async throws -> DatabaseBaseRecord {
        try await container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            let loaded = try await container.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            )
            return try #require(loaded)
        }
    }
}
