#if !os(WASI)
#if FOUNDATION_DB
import DatabaseTypes
import FDBStorage
import StorageKit
import TestSupport
import Testing

@Suite(
    "FoundationDB Scenario Lifecycle",
    .tags(.requiresFDB),
    .serialized,
    .heartbeat
)
struct FoundationDBScenarioLifecycleTests {
    @Test("Engine creation outside a scenario fails explicitly")
    func engineCreationOutsideScenarioFailsExplicitly() async {
        await #expect(
            throws: FoundationDBScenarioAccessError.engineRequestedOutsideScenario
        ) {
            _ = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        }
    }

    /// Proves that the serialized gate releases only after every owner the
    /// scenario created is terminal.
    ///
    /// A transaction the scenario opened and never closed still holds a live
    /// backend handle, so without a scenario-owned boundary its storage work
    /// would reach the service while the next scenario clears the whole key
    /// range. The leaked owner is therefore refused rather than served, and
    /// cancelling it still succeeds so it can reach its terminal state.
    @Test("A successful scenario awaits shutdown for every created owner")
    func successfulScenarioShutsDownEveryEngine() async throws {
        let owners = FoundationDBScenarioOwnerCapture()

        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let first = try await FoundationDBScenarioCoordinator.shared.makeEngine()
            let second = try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
                try await FoundationDBScenarioCoordinator.shared.makeEngine()
            }
            await owners.store(
                engines: [first, second],
                leakedTransaction: try first.createTransaction()
            )
            try await assertFoundationDBIsReachable(first)
            try await assertFoundationDBIsReachable(second)
        }

        for engine in await owners.engines {
            await expectShutdownRejection(engine)
        }

        let leaked = try #require(await owners.leakedTransaction)
        await expectStorageRejection {
            _ = try await leaked.getValue(
                for: ByteString(utf8: "scenario-lifecycle-leaked-read"),
                snapshot: true
            )
        }
        await expectStorageRejection {
            try await leaked.commit()
        }
        // Requesting a versionstamp is local bookkeeping, but resolving it
        // waits on the commit the backend would run, so the wait is refused
        // like any other service work.
        let pendingVersionstamp = leaked.requestVersionstamp()
        await expectStorageRejection {
            _ = try await pendingVersionstamp.value
        }
        try await leaked.cancel()
    }

    @Test("A failed scenario awaits shutdown for every created engine")
    func failedScenarioShutsDownEveryEngine() async throws {
        let owners = FoundationDBScenarioOwnerCapture()

        await #expect(throws: FoundationDBScenarioExpectedFailure.self) {
            try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
                let engine = try await FoundationDBScenarioCoordinator.shared.makeEngine()
                await owners.store(engines: [engine], leakedTransaction: nil)
                try await assertFoundationDBIsReachable(engine)
                throw FoundationDBScenarioExpectedFailure()
            }
        }

        for engine in await owners.engines {
            await expectShutdownRejection(engine)
        }
    }
}

/// Verifies what the coordinator reports at a scenario boundary.
///
/// The decision is exercised directly rather than through a running scenario.
/// Producing a refusal inside the window between the ledger closing and its
/// report being taken is not orchestratable: both contend on one mutex, so a
/// scenario-level test of the same claim would decide nothing.
@Suite("FoundationDB scenario boundary decision")
struct FoundationDBScenarioBoundaryDecisionTests {
    @Test("A refusal is reported together with the scenario's own failure")
    func boundaryFailureCarriesBothOutcomes() async throws {
        let ledger = ScenarioAdmissionLedger(backend: .foundationDB)
        ledger.close()
        #expect(throws: StorageError.self) {
            try ledger.requireOpen(.read)
        }
        let report = await ledger.waitUntilQuiescent()

        let scenarioFailure = FoundationDBScenarioExpectedFailure()
        let boundaryFailure = try #require(
            FoundationDBScenarioCoordinator.scenarioBoundaryFailure(
                report,
                underlying: scenarioFailure
            ) as? FoundationDBScenarioAccessError
        )
        guard case .scenarioLeftStorageWorkOutstanding(
            let rejectedOperationCount,
            let underlyingFailure,
            _
        ) = boundaryFailure else {
            Issue.record("Expected an outstanding-work failure")
            return
        }
        #expect(rejectedOperationCount == 1)
        #expect(underlyingFailure == String(describing: scenarioFailure))
    }

    @Test("A clean boundary leaves the scenario outcome alone")
    func cleanBoundaryReportsNothing() async {
        let ledger = ScenarioAdmissionLedger(backend: .foundationDB)
        ledger.close()
        let report = await ledger.waitUntilQuiescent()

        let cleanScenario = FoundationDBScenarioCoordinator
            .scenarioBoundaryFailure(report, underlying: nil)
        #expect(cleanScenario == nil)

        let failedScenario = FoundationDBScenarioCoordinator
            .scenarioBoundaryFailure(
                report,
                underlying: FoundationDBScenarioExpectedFailure()
            )
        #expect(failedScenario == nil)

        let noEngineCreated = FoundationDBScenarioCoordinator
            .scenarioBoundaryFailure(nil, underlying: nil)
        #expect(noEngineCreated == nil)
    }
}

private actor FoundationDBScenarioOwnerCapture {
    private(set) var engines: [FoundationDBScenarioEngine] = []
    private(set) var leakedTransaction:
        FoundationDBScenarioEngine.TransactionType?

    func store(
        engines: [FoundationDBScenarioEngine],
        leakedTransaction: FoundationDBScenarioEngine.TransactionType?
    ) {
        self.engines = engines
        self.leakedTransaction = leakedTransaction
    }
}

private struct FoundationDBScenarioExpectedFailure: Error {}

private func assertFoundationDBIsReachable(
    _ engine: FoundationDBScenarioEngine
) async throws {
    _ = try await engine.withTransaction { transaction in
        try await transaction.getValue(
            for: ByteString(utf8: "scenario-lifecycle-health"),
            snapshot: true
        )
    }
}

private func expectShutdownRejection(
    _ engine: FoundationDBScenarioEngine
) async {
    await expectStorageRejection {
        _ = try await engine.withTransaction { transaction in
            try await transaction.getValue(
                for: ByteString(utf8: "scenario-lifecycle-after-shutdown"),
                snapshot: true
            )
        }
    }
}

private func expectStorageRejection(
    _ operation: @escaping @Sendable () async throws -> Void
) async {
    await #expect {
        try await operation()
    } throws: { error in
        guard let storageError = error as? StorageError else {
            return false
        }
        return storageError.code == .invalidOperation
    }
}
#endif
#endif
