#if !os(WASI)
#if FOUNDATION_DB
import Testing
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine

@Suite("Transaction Basic Tests", .foundationDBScenario, .serialized, .heartbeat)
struct TransactionBasicTests {

    @Test func simpleReadWrite() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Simple write
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            try tx.setValue([1, 2, 3], for: [0, 0, 1])
        }

        // Simple read
        let value = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            try await tx.getValue(for: [0, 0, 1])
        }

        #expect(value == [1, 2, 3])
    }

    @Test func simpleGetRange() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Write multiple keys
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            try tx.setValue([1], for: [0, 0, 2, 1])
            try tx.setValue([2], for: [0, 0, 2, 2])
            try tx.setValue([3], for: [0, 0, 2, 3])
        }

        // Read with collectRange
        let results = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 0, 2]),
                to: .firstGreaterOrEqual([0, 0, 3])
            )
        }

        #expect(results.count == 3)
    }
}
#endif

#endif
