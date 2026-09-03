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
/// transaction admitted under it, and that the snapshot ends that lease
/// explicitly rather than wherever its last reference happens to go.
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

    @Test(
        "A Base drain does not complete while a domain commit is in flight",
        .timeLimit(.minutes(1))
    )
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
            try await source.withReadSnapshot { snapshot in
                // Keeping this member lease referenced for the rest of the
                // test is what makes the drain observation below reject an
                // ARC-released lease. `DatabaseBaseLease` owns its counted
                // token, so while this reference is live no release point can
                // end the lease: the drain reaches zero active leases only if
                // the snapshot ends the lease explicitly.
                guard let member = snapshot.members.first else {
                    throw CompositionLeaseLifetimeFailure.noMember
                }
                let access = try snapshot.vault.memberAccess(for: member)
                probe.retain(memberLease: access.lease)
                // The vault drain waits on its operation count, not on the
                // lease, so this borrow ends here and only the lease is kept.
                access.operationLease.end()
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
                // The drain parks only after it observes the Base's active
                // lease count above zero, and it can leave that parked state
                // only when the count reaches zero. Waiting for that terminal
                // state, rather than for a number of scheduling hops, is what
                // makes the observation below a fact about the lease rather
                // than about the scheduler. A drain that never parks means it
                // found no active lease, so the loop also ends on that
                // outcome instead of spinning to the suite time limit.
                var parked: DatabaseBaseDrainState
                let parkDeadline = ContinuousClock.now.advanced(by: .seconds(20))
                repeat {
                    try Task.checkCancellation()
                    try #require(
                        ContinuousClock.now < parkDeadline,
                        "The Base drain neither parked nor finished"
                    )
                    try await Task.sleep(for: .milliseconds(1))
                    parked = try #require(
                        fixture.container.baseDrainState(fixture.baseID)
                    )
                } while parked.parkedDrainCount == 0 && !probe.drainFinished
                // The commit this snapshot task is suspended in has not
                // returned, so the lease the drain is waiting on is the member
                // lease that admitted it.
                #expect(parked.activeLeaseCount > 0)
                #expect(!probe.drainFinished)

                commit.release()
                try await snapshotTask.value
                // The member lease is still referenced here, so nothing
                // outside the snapshot can have ended it. An implementation
                // that released the lease at an ARC release point instead
                // keeps the count above zero for as long as this reference
                // lives, so it fails on this deadline with a recorded reason
                // rather than at the suite time limit.
                var drained: DatabaseBaseDrainState
                let drainDeadline = ContinuousClock.now.advanced(by: .seconds(20))
                repeat {
                    try Task.checkCancellation()
                    try #require(
                        ContinuousClock.now < drainDeadline,
                        "The Base drain never observed an explicit member lease release"
                    )
                    try await Task.sleep(for: .milliseconds(1))
                    drained = try #require(
                        fixture.container.baseDrainState(fixture.baseID)
                    )
                } while drained.activeLeaseCount > 0
                try await drainTask.value
                #expect(probe.drainFinished)
                await commitMonitor.value
            } catch {
                commit.release()
                snapshotTask.cancel()
                _ = await snapshotTask.result
                // Dropping the held reference lets a lease that ends only at
                // an ARC release point reach its token, so a parked drain
                // still terminates when this test failed because the snapshot
                // never released the lease explicitly.
                probe.releaseMemberLease()
                drainTask.cancel()
                _ = await drainTask.result
                await commitMonitor.value
                throw error
            }
        } catch {
            probe.commit?.release()
            snapshotTask.cancel()
            _ = await snapshotTask.result
            probe.releaseMemberLease()
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

private enum CompositionLeaseLifetimeFailure: Error {
    case noMember
}

private final class CompositionLeaseDrainProbe: Sendable {
    private struct State: Sendable {
        var commit: StorageOperationBarrier?
        var memberLease: DatabaseBaseLease?
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

    func retain(memberLease: DatabaseBaseLease) {
        state.withLock { $0.memberLease = memberLease }
    }

    /// Drops the held member lease outside the lock. A lease that ends at an
    /// ARC release point finishes its token when its last reference goes away,
    /// which resumes a parked drain, and that must not run inside this mutex.
    func releaseMemberLease() {
        let released = state.withLock { state -> DatabaseBaseLease? in
            let lease = state.memberLease
            state.memberLease = nil
            return lease
        }
        withExtendedLifetime(released) {}
    }

    func markDrainFinished() {
        state.withLock { $0.drainFinished = true }
    }
}

#endif
