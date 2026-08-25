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

    @Test("A successful scenario awaits shutdown for every created engine")
    func successfulScenarioShutsDownEveryEngine() async throws {
        let engines = FoundationDBScenarioEngineCapture()

        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let first = try await FoundationDBScenarioCoordinator.shared.makeEngine()
            let second = try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
                try await FoundationDBScenarioCoordinator.shared.makeEngine()
            }
            await engines.store([first, second])
            try await assertFoundationDBIsReachable(first)
            try await assertFoundationDBIsReachable(second)
        }

        for engine in await engines.values {
            await expectShutdownRejection(engine)
        }
    }

    @Test("A failed scenario awaits shutdown for every created engine")
    func failedScenarioShutsDownEveryEngine() async throws {
        let engines = FoundationDBScenarioEngineCapture()

        await #expect(throws: FoundationDBScenarioExpectedFailure.self) {
            try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
                let engine = try await FoundationDBScenarioCoordinator.shared.makeEngine()
                await engines.store([engine])
                try await assertFoundationDBIsReachable(engine)
                throw FoundationDBScenarioExpectedFailure()
            }
        }

        for engine in await engines.values {
            await expectShutdownRejection(engine)
        }
    }
}

private actor FoundationDBScenarioEngineCapture {
    private(set) var values: [FDBStorageEngine] = []

    func store(_ engines: [FDBStorageEngine]) {
        values = engines
    }
}

private struct FoundationDBScenarioExpectedFailure: Error {}

private func assertFoundationDBIsReachable(
    _ engine: FDBStorageEngine
) async throws {
    _ = try await engine.withTransaction { transaction in
        try await transaction.getValue(
            for: ByteString(utf8: "scenario-lifecycle-health"),
            snapshot: true
        )
    }
}

private func expectShutdownRejection(
    _ engine: FDBStorageEngine
) async {
    await #expect {
        _ = try await engine.withTransaction { transaction in
            try await transaction.getValue(
                for: ByteString(utf8: "scenario-lifecycle-after-shutdown"),
                snapshot: true
            )
        }
    } throws: { error in
        guard let storageError = error as? StorageError else {
            return false
        }
        return storageError.code == .invalidOperation
    }
}
#endif
#endif
