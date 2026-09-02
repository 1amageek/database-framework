#if MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

/// Proves that a Composition member admission lease outlives every domain
/// transaction admitted under it.
///
/// A Base lifecycle transition stops admission and then waits for the active
/// leases of that Base to reach zero. The federated read holds its domain
/// transactions directly, so if its member lease ended before those
/// transactions were terminal, that wait could complete while a domain commit
/// was still in flight, letting retirement, deletion, or placement movement
/// advance past a running transaction.
@Suite("Composition lease lifetime")
struct CompositionLeaseLifetimeTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    @Test("A Base drain does not complete while a domain commit is in flight")
    func baseDrainWaitsForDomainCommit() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let fixture = try await makeFixture(storageEngine: storage)
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])
        let probe = CompositionLeaseDrainProbe()
        let armed = StorageOperationBarrier()

        let snapshotTask = Task {
            try await source.withReadSnapshot { _ in
                // The domain transactions commit after this closure returns,
                // so arming the barrier here selects exactly the commit that
                // the member lease must outlive. Arming it earlier would let
                // an unrelated metadata commit consume the barrier.
                probe.record(
                    commit: storage.control.suspendNextReadOnlyCommit()
                )
                armed.signalEntry()
            }
        }
        let armedMonitor = try await armed.waitUntilEntered(
            beforeCompletionOf: snapshotTask
        )
        do {
            let commit = try #require(probe.commit)
            let commitMonitor = try await commit.waitUntilEntered(
                beforeCompletionOf: snapshotTask
            )
            let drainTask = Task {
                defer { probe.markDrainFinished() }
                try await fixture.container.stopBaseAdmissionAndDrain(
                    fixture.baseID
                )
            }
            do {
                // The drain has nothing left to wait for the moment the
                // member lease ends, so it would finish within a few
                // scheduling hops if the lease had already been released.
                for _ in 0..<64 { await Task.yield() }
                #expect(!probe.drainFinished)

                commit.release()
                try await snapshotTask.value
                try await drainTask.value
                #expect(probe.drainFinished)
                await commitMonitor.value
            } catch {
                commit.release()
                drainTask.cancel()
                _ = await drainTask.result
                snapshotTask.cancel()
                _ = await snapshotTask.result
                await commitMonitor.value
                throw error
            }
        } catch {
            probe.commit?.release()
            snapshotTask.cancel()
            _ = await snapshotTask.result
            await armedMonitor.value
            throw error
        }
        await armedMonitor.value
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let baseID: Base.ID
        let readerAuthorization: AuthorizationContext
    }

    private func makeFixture(
        storageEngine: any StorageEngine
    ) async throws -> Fixture {
        let domainID = try DatabaseStorageDomain.ID("composition-lease-lifetime")
        let placementID = try Base.Placement.ID("composition-lease-lifetime")
        let baseID = try Base.ID("composition-lease-lifetime-base")
        let owner = Principal(identifier: "composition-lease-lifetime-owner")
        let reader = Principal(identifier: "composition-lease-lifetime-reader")
        let databaseAdmin = Principal(
            identifier: "composition-lease-lifetime-database-admin",
            roles: ["admin"]
        )
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    rootPath: ["tests", "composition-lease-lifetime"],
                    storageEngine: storageEngine
                )
            ],
            placements: [
                DatabaseStoragePlacement(id: placementID, domainID: domainID)
            ],
            defaultPlacementID: placementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-lease-lifetime",
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
            baseID: baseID,
            readerAuthorization: .authenticated(reader)
        )
    }
}

private final class CompositionLeaseDrainProbe: Sendable {
    private struct State: Sendable {
        var commit: StorageOperationBarrier?
        var drainFinished = false
    }

    private let state = Mutex(State())

    var commit: StorageOperationBarrier? {
        state.withLock { $0.commit }
    }

    var drainFinished: Bool {
        state.withLock { $0.drainFinished }
    }

    func record(commit: StorageOperationBarrier) {
        state.withLock { $0.commit = commit }
    }

    func markDrainFinished() {
        state.withLock { $0.drainFinished = true }
    }
}

#endif
