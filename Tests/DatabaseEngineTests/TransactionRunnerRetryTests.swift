import Testing
import StorageKit
import Synchronization
@testable import DatabaseEngine

@Suite("TransactionRunner Retry Tests", .serialized, .heartbeat)
struct TransactionRunnerRetryTests {
    @Test("Retries retryable storage errors and then succeeds")
    func retriesRetryableStorageErrorsAndSucceeds() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(database: engine)
        let attempts = AttemptCounter()

        let result = try await runner.run(
            configuration: TransactionConfiguration(retryLimit: 3, maxRetryDelay: 1),
            operationDescription: "test retry success"
        ) { _ in
            let attempt = attempts.increment()
            if attempt < 3 {
                throw StorageError.transactionConflict
            }
            return "ok"
        }

        #expect(result == "ok")
        #expect(attempts.value == 3)
    }

    @Test("Retries retryable createTransaction errors and then succeeds")
    func retriesRetryableCreateTransactionErrorsAndSucceeds() async throws {
        let engine = FlakyCreateTransactionEngine(failuresBeforeSuccess: 2)
        let runner = TransactionRunner(database: engine)
        let bodyAttempts = AttemptCounter()

        let result = try await runner.run(
            configuration: TransactionConfiguration(retryLimit: 3, maxRetryDelay: 1),
            operationDescription: "test create transaction retry"
        ) { transaction in
            _ = bodyAttempts.increment()
            transaction.setValue([0x01], for: [0xA1])
            return "ok"
        }

        #expect(result == "ok")
        #expect(engine.createAttempts == 3)
        #expect(bodyAttempts.value == 1)
    }

    @Test("Throws retry exhausted error after retry limit")
    func throwsRetryExhaustedErrorAfterRetryLimit() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(database: engine)
        let attempts = AttemptCounter()

        do {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(retryLimit: 2, maxRetryDelay: 1),
                operationDescription: "test retry exhaustion"
            ) { _ in
                _ = attempts.increment()
                throw StorageError.transactionConflict
            }
            Issue.record("Expected retry exhaustion")
        } catch let error as TransactionRetryExhaustedError {
            #expect(error.attempts == 2)
            #expect(error.operationDescription == "test retry exhaustion")
            #expect(error.lastStorageError != nil)
            #expect(error.lastErrorDescription.isEmpty == false)
        }

        #expect(attempts.value == 2)
    }

    @Test("Cancellation during backoff is not retried")
    func cancellationDuringBackoffIsNotRetried() async throws {
        let engine = InMemoryEngine()
        let runner = TransactionRunner(database: engine)
        let attempts = AttemptCounter()

        let task = Task {
            let _: Void = try await runner.run(
                configuration: TransactionConfiguration(retryLimit: 5, maxRetryDelay: 50),
                operationDescription: "test cancellation"
            ) { _ in
                _ = attempts.increment()
                throw StorageError.transactionConflict
            }
        }

        while attempts.value == 0 {
            await Task.yield()
        }
        task.cancel()

        do {
            try await task.value
            Issue.record("Expected cancellation")
        } catch is CancellationError {
            #expect(attempts.value == 1)
        }
    }

    @Test("Instrumented transactions use the shared runner retry policy")
    func instrumentedTransactionsUseSharedRunnerRetryPolicy() async throws {
        let engine = InMemoryEngine()
        let attempts = AttemptCounter()
        let timer = StoreTimer(emitMetrics: false)

        let (result, metrics) = try await engine.withInstrumentedTransaction(timer: timer) { tx in
            let attempt = attempts.increment()
            if attempt == 1 {
                throw StorageError.transactionConflict
            }
            tx.setValue([0x01], for: [0xA0])
            return "committed"
        }

        #expect(result == "committed")
        #expect(attempts.value == 2)
        #expect(metrics.retryCount == 1)
        #expect(metrics.committed == true)
        #expect(metrics.writeCount == 1)
        #expect(timer.getCount(.retries) == 1)
    }
}

private final class AttemptCounter: Sendable {
    private let state = Mutex<Int>(0)

    var value: Int {
        state.withLock { $0 }
    }

    func increment() -> Int {
        state.withLock {
            $0 += 1
            return $0
        }
    }
}

private final class FlakyCreateTransactionEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        let failuresBeforeSuccess: Int
    }

    typealias TransactionType = InMemoryTransaction

    private let engine = InMemoryEngine()
    private let remainingFailures: Mutex<Int>
    private let attempts = Mutex<Int>(0)

    var createAttempts: Int {
        attempts.withLock { $0 }
    }

    init(failuresBeforeSuccess: Int) {
        self.remainingFailures = Mutex(failuresBeforeSuccess)
    }

    init(configuration: Configuration) {
        self.remainingFailures = Mutex(configuration.failuresBeforeSuccess)
    }

    func createTransaction() throws -> InMemoryTransaction {
        attempts.withLock { $0 += 1 }
        let shouldFail = remainingFailures.withLock { remaining in
            guard remaining > 0 else { return false }
            remaining -= 1
            return true
        }
        if shouldFail {
            throw StorageError.transactionConflict
        }
        return try engine.createTransaction()
    }

    func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let transaction = try createTransaction()
        do {
            let result = try await operation(transaction)
            try await transaction.commit()
            return result
        } catch {
            transaction.cancel()
            throw error
        }
    }

    var directoryService: any DirectoryService {
        engine.directoryService
    }
}
