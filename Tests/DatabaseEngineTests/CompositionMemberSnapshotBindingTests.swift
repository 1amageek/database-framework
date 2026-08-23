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
        let baseRevision: UInt64
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
            let member = try #require(snapshot.members.first)
            let admittedTransaction = try snapshot.admittedTransaction(
                for: member
            )
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
                        admittedTransaction
                    )
                )
                #expect(!(snapshotTransaction is any TransactionAccess))

                let reusedSnapshot = try await context
                    .withReadStorageAccess { transaction in
                        #expect(!(transaction is any TransactionAccess))
                        let nestedBinding = try #require(
                            ActiveDatabaseTransactionContext.binding
                        )
                        #expect(
                            nestedBinding.identity.dataRoot
                                == binding.identity.dataRoot
                        )
                        return true
                    }
                #expect(reusedSnapshot)

                await #expect(throws: DatabaseReadTransactionError.self) {
                    try await context.withExecutionTransaction(
                        requiredAccess: .write
                    ) { _ in () }
                }
            }
        }
    }

    @Test(
        "Escaped snapshot cannot retain transactions or Base leases",
        .timeLimit(.minutes(1))
    )
    func escapedSnapshotIsClosedAtCallbackBoundary() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])

        let escaped = try await source.withReadSnapshot { $0 }
        let member = try #require(escaped.members.first)
        #expect(throws: DatabaseCompositionAccessError.self) {
            _ = try escaped.transaction(for: member)
        }

        let escapedTransaction = try await source.withReadSnapshot {
            snapshot in
            try snapshot.transaction(
                for: #require(snapshot.members.first)
            )
        }
        await #expect(
            throws: DatabaseReadTransactionError.snapshotClosed
        ) {
            try await escapedTransaction.getValue(
                for: ByteString(utf8: "escaped-snapshot-read"),
                snapshot: true
            )
        }

        let retired = try await fixture.container.retireBase(
            fixture.baseID,
            expectedRevision: fixture.baseRevision
        )
        #expect(retired.lifecycle == .retired)
    }

    @Test(
        "Escaped cursor cannot deadlock snapshot close",
        .timeLimit(.minutes(1))
    )
    func escapedCursorIsRevokedWithoutBlockingClose() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])

        var cursor = try await source.withReadSnapshot { snapshot in
            let member = try #require(snapshot.members.first)
            let admitted = try snapshot.admittedMember(for: member)
            let (begin, end) = admitted.lease.root.range()
            return admitted.transaction.rangeCursor(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
        }

        await #expect(
            throws: DatabaseReadTransactionError.snapshotClosed
        ) {
            _ = try await cursor.next()
        }
        try await cursor.finish()
    }

    @Test(
        "Inherited member context cannot retain a Base lease",
        .timeLimit(.minutes(1))
    )
    func inheritedTaskLocalDoesNotDelayBaseRetirement() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])
        let release = CompositionInheritedTaskGate()

        let inheritedTask = try await source.withReadSnapshot { snapshot in
            let member = try #require(snapshot.members.first)
            return try await source.withMemberContext(
                member,
                in: snapshot
            ) { _, _ in
                Task { await release.waitUntilOpen() }
            }
        }

        let retired = try await fixture.container.retireBase(
            fixture.baseID,
            expectedRevision: fixture.baseRevision
        )
        #expect(retired.lifecycle == .retired)
        await release.open()
        await inheritedTask.value
    }

    @Test(
        "Snapshot close drains an admitted member operation",
        .timeLimit(.minutes(1))
    )
    func snapshotCloseDrainsAdmittedMemberOperation() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])
        let entered = CompositionInheritedTaskGate()
        let release = CompositionInheritedTaskGate()
        let observation = CompositionSnapshotCloseObservation()

        try await source.withReadSnapshot { snapshot in
            let member = try #require(snapshot.members.first)
            let memberTask = Task {
                try await source.withMemberContext(
                    member,
                    in: snapshot
                ) { _, _ in
                    await entered.open()
                    await release.waitUntilOpen()
                }
            }
            await entered.waitUntilOpen()

            let closeTask = Task {
                try await snapshot.close()
                await observation.markClosed()
            }
            for _ in 0..<20 { await Task.yield() }
            #expect(!(await observation.isClosed()))

            await release.open()
            try await memberTask.value
            try await closeTask.value
            #expect(await observation.isClosed())
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
            let record = try await container.provisionBase(
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
            return Fixture(
                container: container,
                baseID: baseID,
                baseRevision: record.revision,
                readerAuthorization: .authenticated(reader)
            )
        } catch {
            await container.shutdown()
            throw error
        }
    }

    private static func sameTransaction(
        _ lhs: any TransactionReadAccess,
        _ rhs: any TransactionReadAccess
    ) -> Bool {
        ObjectIdentifier(lhs as AnyObject) == ObjectIdentifier(rhs as AnyObject)
    }
}

private actor CompositionInheritedTaskGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func waitUntilOpen() async {
        guard !isOpen else { return }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func open() {
        isOpen = true
        let pending = waiters
        waiters.removeAll(keepingCapacity: false)
        for waiter in pending { waiter.resume() }
    }
}

private actor CompositionSnapshotCloseObservation {
    private var closed = false

    func markClosed() { closed = true }
    func isClosed() -> Bool { closed }
}
#endif
