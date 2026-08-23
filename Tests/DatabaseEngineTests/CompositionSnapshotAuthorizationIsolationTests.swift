#if MultiBase
import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

@Suite("Composition snapshot authorization isolation")
struct CompositionSnapshotAuthorizationIsolationTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let firstBaseID: Base.ID
        let secondBaseID: Base.ID
        let ownerAuthorization: AuthorizationContext
        let readerAuthorization: AuthorizationContext
        let outsiderAuthorization: AuthorizationContext
    }

    @Test("Snapshot remains bound to the source that authorized it")
    func snapshotRejectsForeignSource() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let readerSource = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.firstBaseID])
        let outsiderSource = try fixture.container.session(
            authorization: fixture.outsiderAuthorization
        ).composition(bases: [fixture.firstBaseID])

        await #expect(throws: DatabaseCompositionAccessError.self) {
            try await outsiderSource.withReadSnapshot { _ in () }
        }
        try await readerSource.withReadSnapshot { snapshot in
            let member = try #require(snapshot.members.first)
            await #expect(throws: DatabaseCompositionAccessError.self) {
                try await outsiderSource.withMemberContext(
                    member,
                    in: snapshot
                ) { _, _ in () }
            }
        }
    }

    @Test("Snapshot transaction rejects mutation and foreign member leases")
    func snapshotRejectsMutationAndForeignMember() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let firstSource = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.firstBaseID])
        let secondSource = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.secondBaseID])
        let foreignMember = try await secondSource.withReadSnapshot {
            try #require($0.members.first)
        }

        try await firstSource.withReadSnapshot { snapshot in
            let member = try #require(snapshot.members.first)
            let readTransaction = try snapshot.transaction(for: member)
            #expect(!(readTransaction is any TransactionAccess))
            let admittedTransaction = try snapshot.admittedTransaction(
                for: member
            )
            #expect(throws: DatabaseReadTransactionError.self) {
                try admittedTransaction.setValue(
                    ByteString(utf8: "value"),
                    for: ByteString(utf8: "composition-read-snapshot")
                )
            }
            #expect(throws: DatabaseCompositionAccessError.self) {
                _ = try snapshot.transaction(for: foreignMember)
            }
        }
    }

    @Test("Admitted transaction remains bound to its Base data root")
    func admittedTransactionRejectsForeignBaseContext() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let session = fixture.container.session(
            authorization: fixture.readerAuthorization
        )
        let firstContext = session.base(fixture.firstBaseID).newContext()
        let secondContext = session.base(fixture.secondBaseID).newContext()
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(Anchor.persistableType))
        )
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: fixture.container.monotonicClock
        )

        await #expect(
            throws: DatabaseTransactionExecutionScopeError.dataRootMismatch
        ) {
            try await firstContext.executeCanonicalRead { transaction in
                try await secondContext.executeCanonicalQuery(
                    query,
                    execution: execution,
                    transaction: transaction
                )
            }
        }
    }

    @Test("Read execution transaction attenuates direct and nested writes")
    func readExecutionTransactionRejectsWrites() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let readerContext = fixture.container.session(
            authorization: fixture.readerAuthorization
        ).base(fixture.firstBaseID).newContext()

        try await readerContext.withExecutionReadTransaction(
            requiredAccess: .read
        ) { transaction in
            #expect(!(transaction.storageAccess is any TransactionAccess))
        }

        let ownerContext = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(fixture.firstBaseID).newContext()
        _ = try await ownerContext.withExecutionTransaction(
            requiredAccess: .all
        ) { _ in
            try await ownerContext.withExecutionReadTransaction(
                requiredAccess: .read
            ) { transaction in
                #expect(!(transaction.storageAccess is any TransactionAccess))
            }
        }

        await #expect(throws: DatabaseReadTransactionError.self) {
            try await readerContext.withExecutionDataOperation {
                try await fixture.container.withDatabaseTransaction(
                    requiredAccess: .read
                ) { transaction in
                    try transaction.setValue(
                        ByteString(utf8: "data-root-read-transaction"),
                        for: ByteString(utf8: "read-capability-test")
                    )
                }
            }
        }

        await #expect(throws: DatabaseReadTransactionError.self) {
            try await fixture.container.withControlTransaction(
                requiredAccess: .read,
                authorization: fixture.readerAuthorization
            ) { transaction in
                try transaction.executionStorageAccess.setValue(
                    ByteString(utf8: "control-read-transaction"),
                    for: ByteString(utf8: "read-capability-test")
                )
            }
        }

        await #expect(throws: DatabaseReadTransactionError.self) {
            try await fixture.container.executionWithBaseAdministrationTransaction(
                baseID: fixture.firstBaseID,
                requiredAccess: .read,
                authorization: fixture.readerAuthorization
            ) { transaction in
                try transaction.executionStorageAccess.setValue(
                    ByteString(utf8: "base-admin-read-transaction"),
                    for: ByteString(utf8: "read-capability-test")
                )
            }
        }
    }

    @Test("Canonical query preserves active write transaction identity")
    func canonicalQueryAcceptsActiveWriteTransaction() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let context = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(fixture.firstBaseID).newContext()
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(Anchor.persistableType))
        )
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: fixture.container.monotonicClock
        )

        let response = try await context.withExecutionTransaction(
            requiredAccess: .all
        ) { transaction in
            try await context.executeCanonicalQuery(
                query,
                execution: execution,
                transaction: transaction.storageAccess
            )
        }
        #expect(response.rows.isEmpty)
    }

    @Test("Write execution transaction rejects foreign Base and control roots")
    func writeExecutionTransactionRejectsForeignRoots() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let firstLease = try fixture.container.acquireBaseLease(
            fixture.firstBaseID
        )
        let secondLease = try fixture.container.acquireBaseLease(
            fixture.secondBaseID
        )
        let firstKey = firstLease.root.subspace("data").pack(Tuple("inside"))
        let secondKey = secondLease.root.subspace("data").pack(Tuple("outside"))
        let controlKey = fixture.container.controlStorage().root
            .pack(Tuple("outside"))
        let value = ByteString(utf8: "value")
        let context = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(fixture.firstBaseID).newContext()

        try await context.withExecutionTransaction(
            requiredAccess: [.read, .write]
        ) { transaction in
            try transaction.executionStorageAccess.setValue(
                value,
                for: firstKey
            )
            #expect(
                throws: DatabaseReadTransactionError.keyOutsideDataRoot
            ) {
                try transaction.executionStorageAccess.setValue(
                    value,
                    for: secondKey
                )
            }
            #expect(
                throws: DatabaseReadTransactionError.keyOutsideDataRoot
            ) {
                try transaction.executionStorageAccess.setValue(
                    value,
                    for: controlKey
                )
            }
        }

        let readBack = try await context.withExecutionTransaction(
            requiredAccess: .read
        ) { transaction in
            try await transaction.executionStorageAccess.getValue(
                for: firstKey,
                snapshot: true
            )
        }
        #expect(readBack == value)
    }

    private func makeFixture() async throws -> Fixture {
        let domainID = try DatabaseStorageDomain.ID(
            "composition-authorization-isolation"
        )
        let placementID = try Base.Placement.ID(
            "composition-authorization-isolation"
        )
        let firstBaseID = try Base.ID("composition-authorization-first")
        let secondBaseID = try Base.ID("composition-authorization-second")
        let owner = Principal(identifier: "composition-authorization-owner")
        let reader = Principal(identifier: "composition-authorization-reader")
        let outsider = Principal(
            identifier: "composition-authorization-outsider"
        )
        let databaseAdmin = Principal(
            identifier: "composition-authorization-database-admin",
            roles: ["admin"]
        )
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: ["tests", "composition-authorization"],
                    storageEngine: InMemoryEngine()
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
                name: "composition-authorization-isolation",
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
            for baseID in [firstBaseID, secondBaseID] {
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
            }
            try await container.grantDatabaseAccessForTesting(
                Security.Grant(
                    subject: .principal(reader.identifier),
                    resource: .database,
                    access: .read
                ),
                authorization: .authenticated(databaseAdmin)
            )
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            firstBaseID: firstBaseID,
            secondBaseID: secondBaseID,
            ownerAuthorization: .authenticated(owner),
            readerAuthorization: .authenticated(reader),
            outsiderAuthorization: .authenticated(outsider)
        )
    }
}
#endif
