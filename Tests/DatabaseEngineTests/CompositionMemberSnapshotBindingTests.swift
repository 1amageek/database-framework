#if MultiBase
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Composition member snapshot binding")
struct CompositionMemberSnapshotBindingTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let baseID: Base.ID
        let readerAuthorization: AuthorizationContext
    }

    @Test("Member context reuses the snapshot transaction and remains read-only")
    func memberContextReusesSnapshotTransaction() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])

        try await source.withReadSnapshot { snapshot in
            let member = try #require(snapshot.lease.members.first)
            try await source.withMemberContext(
                member,
                in: snapshot
            ) { context, snapshotTransaction in
                let binding = try #require(
                    ActiveDatabaseTransactionContext.binding
                )
                #expect(
                    Self.sameTransaction(
                        binding.transaction,
                        snapshotTransaction
                    )
                )

                let reusedSnapshot = try await context.indexQueryContext
                    .withTransaction { transaction in
                        Self.sameTransaction(
                            transaction,
                            snapshotTransaction
                        )
                    }
                #expect(reusedSnapshot)

                await #expect(throws: DatabaseGrantAuthorizationError.self) {
                    try await context.withExecutionTransaction(
                        requiredAccess: .write
                    ) { _ in () }
                }
            }
        }
    }

    private func makeFixture() async throws -> Fixture {
        let domainID = try DatabaseStorageDomain.ID(
            "composition-snapshot-binding"
        )
        let placementID = try Base.Placement.ID(
            "composition-snapshot-binding"
        )
        let baseID = try Base.ID("composition-snapshot-binding")
        let owner = Principal(identifier: "snapshot-owner")
        let reader = Principal(identifier: "snapshot-reader")
        let engine = InMemoryEngine()
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: ["tests", "composition-snapshot-binding"],
                    storageEngine: engine
                )
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
                )
            ],
            defaultPlacementID: placementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-snapshot-binding",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Anchor.self)
                ]
            ),
            security: .testingDisabled
        )
        do {
            _ = try await container.provisionBase(
                baseID,
                placementID: placementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(baseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(baseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            baseID: baseID,
            readerAuthorization: .authenticated(reader)
        )
    }

    private static func sameTransaction(
        _ lhs: any TransactionAccess,
        _ rhs: any TransactionAccess
    ) -> Bool {
        ObjectIdentifier(lhs as AnyObject) == ObjectIdentifier(rhs as AnyObject)
    }
}
#endif
