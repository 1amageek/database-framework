#if MultiBase
import DatabaseKit
import DatabaseRuntime
import StorageKit
import Synchronization
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
                try await outsiderSource.withMemberReadSession(
                    member,
                    in: snapshot,
                    workMeter: DatabaseWorkMeter(
                        budget: ExecutionBudget(),
                        monotonicClock: fixture.container.monotonicClock
                    )
                ) { _ in () }
            }
        }
    }

    @Test("Escaped snapshot revokes member lifecycle authority")
    func escapedSnapshotRevokesMemberLifecycleAuthority() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let firstSource = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.firstBaseID])
        let snapshot = try await firstSource.withReadSnapshot { snapshot in
            let foreignMember = DatabaseCompositionMember(
                baseID: fixture.secondBaseID
            )
            #expect(throws: DatabaseCompositionAccessError.self) {
                _ = try snapshot.vault.memberAccess(for: foreignMember)
            }
            return snapshot
        }
        let member = try #require(snapshot.members.first)
        #expect(throws: DatabaseCompositionAccessError.self) {
            _ = try snapshot.vault.memberAccess(for: member)
        }
    }

    @Test("Snapshot drains an admitted member read before transaction exit")
    func snapshotDrainsInFlightMemberRead() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(storageEngine: storage)
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.firstBaseID])
        let readKey = ByteString(utf8: "in-flight-member-read")
        let barrier = storage.control.suspendNextValueRead(for: readKey)
        let probe = CompositionSnapshotDrainProbe()

        let snapshotTask = Task {
            defer { probe.markSnapshotFinished() }
            try await source.withReadSnapshot { snapshot in
                let member = try #require(snapshot.members.first)
                probe.record(snapshot: snapshot, member: member)
                let task = Task {
                    try await source.withMemberReadSession(
                        member,
                        in: snapshot,
                        workMeter: DatabaseWorkMeter(
                            budget: ExecutionBudget(),
                            monotonicClock: fixture.container.monotonicClock
                        )
                    ) { session in
                        try await session.transaction.getValue(
                            for: readKey,
                            snapshot: true
                        )
                    }
                }
                probe.record(memberRead: task)
                let monitor = try await barrier.waitUntilEntered(
                    beforeCompletionOf: task
                )
                probe.record(memberReadCompletionMonitor: monitor)
            }
        }

        let snapshotCompletionMonitor = try await barrier.waitUntilEntered(
            beforeCompletionOf: snapshotTask
        )
        do {
            let (snapshot, member) = try #require(
                probe.snapshotAndMember()
            )
            while true {
                do {
                    let access = try snapshot.vault.memberAccess(
                        for: member
                    )
                    access.operationLease.end()
                    await Task.yield()
                } catch is DatabaseCompositionAccessError {
                    break
                }
            }
            #expect(!probe.snapshotFinished)

            barrier.release()
            try await snapshotTask.value
            await snapshotCompletionMonitor.value
            let read = try #require(probe.memberRead)
            #expect(try await read.value == nil)
            let readCompletionMonitor = try #require(
                probe.memberReadCompletionMonitor
            )
            await readCompletionMonitor.value
            #expect(probe.snapshotFinished)
        } catch {
            barrier.release()
            snapshotTask.cancel()
            _ = await snapshotTask.result
            if let read = probe.memberRead {
                read.cancel()
                _ = await read.result
            }
            if let monitor = probe.memberReadCompletionMonitor {
                await monitor.value
            }
            await snapshotCompletionMonitor.value
            throw error
        }
    }

    @Test("Admitted transaction cannot be rebound to another Base context")
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

        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await firstContext.withReadSnapshot(
                workMeter: execution.workMeter
            ) { _ in
                try await DatabaseReadSession.withSession(
                    context: secondContext,
                    workMeter: execution.workMeter
                ) { session in
                    try await session.executeCanonical(
                        query,
                        execution: execution
                    )
                }
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

        await #expect(throws: DatabaseReadTransactionError.self) {
            try await readerContext.withExecutionTransaction(
                requiredAccess: .read
            ) { transaction in
                try await transaction.save(
                    Anchor(id: "direct-read-transaction"),
                    precondition: .none
                )
            }
        }

        let ownerContext = fixture.container.session(
            authorization: fixture.ownerAuthorization
        ).base(fixture.firstBaseID).newContext()
        _ = try await ownerContext.withExecutionTransaction(
            requiredAccess: .all
        ) { _ in
            await #expect(throws: DatabaseReadTransactionError.self) {
                try await ownerContext.withExecutionTransaction(
                    requiredAccess: .read
                ) { transaction in
                    try await transaction.save(
                        Anchor(id: "nested-read-transaction"),
                        precondition: .none
                    )
                }
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
        ) { _ in
            try await DatabaseReadSession.withSession(
                context: context,
                workMeter: execution.workMeter
            ) { session in
                try await session.executeCanonical(
                    query,
                    execution: execution
                )
            }
        }
        #expect(response.rows.isEmpty)
    }

    private func makeFixture(
        storageEngine: any StorageEngine = InMemoryEngine()
    ) async throws -> Fixture {
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
                    rootPath: ["tests", "composition-authorization"],
                    storageEngine: storageEngine
                )
            ],
            placements: [
                DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID
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

private final class CompositionSnapshotDrainProbe: Sendable {
    private struct State: Sendable {
        var snapshot: DatabaseCompositionReadSnapshot?
        var member: DatabaseCompositionMember?
        var memberRead: Task<ByteString?, any Error>?
        var memberReadCompletionMonitor: Task<Void, Never>?
        var snapshotFinished = false
    }

    private let state = Mutex(State())

    var snapshotFinished: Bool {
        state.withLock { $0.snapshotFinished }
    }

    var memberRead: Task<ByteString?, any Error>? {
        state.withLock { $0.memberRead }
    }

    var memberReadCompletionMonitor: Task<Void, Never>? {
        state.withLock { $0.memberReadCompletionMonitor }
    }

    func record(
        snapshot: DatabaseCompositionReadSnapshot,
        member: DatabaseCompositionMember
    ) {
        state.withLock { state in
            state.snapshot = snapshot
            state.member = member
        }
    }

    func record(memberRead: Task<ByteString?, any Error>) {
        state.withLock { $0.memberRead = memberRead }
    }

    func record(memberReadCompletionMonitor: Task<Void, Never>) {
        state.withLock {
            $0.memberReadCompletionMonitor = memberReadCompletionMonitor
        }
    }

    func markSnapshotFinished() {
        state.withLock { $0.snapshotFinished = true }
    }

    func snapshotAndMember() -> (
        DatabaseCompositionReadSnapshot,
        DatabaseCompositionMember
    )? {
        state.withLock { state in
            guard let snapshot = state.snapshot,
                  let member = state.member else {
                return nil
            }
            return (snapshot, member)
        }
    }
}
#endif
