import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for TransactionContext and the new transaction API
///
/// **Coverage**:
/// - TransactionConfiguration presets
/// - TransactionContext CRUD operations
/// - FDBContext.withTransaction API
/// - Snapshot vs transactional read semantics
/// - Index updates within transactions
@Suite("TransactionContext Tests", .serialized)
struct TransactionContextTests {

    // MARK: - Helper Types

    /// Test model for transaction tests
    @Persistable
    struct TransactionTestUser {
        #Directory<TransactionTestUser>("test", "txcontext", "users")
        var id: String = ULID().ulidString
        var name: String
        var balance: Int
    }

    /// Test model for products
    @Persistable
    struct TransactionTestProduct {
        #Directory<TransactionTestProduct>("test", "txcontext", "products")
        var id: String = ULID().ulidString
        var name: String
        var price: Double
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(
            [TransactionTestUser.self, TransactionTestProduct.self],
            version: Schema.Version(1, 0, 0)
        )

        return FDBContainer(
            database: database,
            schema: schema
        )
    }

    private func cleanup(container: FDBContainer) async throws {
        let context = container.newContext()
        try await context.deleteAll(TransactionTestUser.self)
        try await context.deleteAll(TransactionTestProduct.self)
        try await context.save()
    }

    // MARK: - TransactionConfiguration Tests

    @Test("TransactionConfiguration.default has expected values")
    func defaultConfigurationValues() {
        let config = TransactionConfiguration.default
        #expect(config.timeout == nil)
        #expect(config.retryLimit == 5)
        #expect(config.maxRetryDelay == 1000)
        #expect(config.priority == .default)
        #expect(config.readPriority == .normal)
        #expect(config.disableReadCache == false)
        #expect(config.weakReadSemantics == nil)  // Strict consistency by default
    }

    @Test("TransactionConfiguration.batch has expected values")
    func batchConfigurationValues() {
        let config = TransactionConfiguration.batch
        #expect(config.timeout == 30_000)
        #expect(config.retryLimit == 20)
        #expect(config.maxRetryDelay == 2000)
        #expect(config.priority == .batch)
        #expect(config.readPriority == .low)
        #expect(config.disableReadCache == true)
        #expect(config.weakReadSemantics == .relaxed)  // Batch uses relaxed semantics
    }

    @Test("TransactionConfiguration.system has expected values")
    func systemConfigurationValues() {
        let config = TransactionConfiguration.system
        #expect(config.timeout == 2_000)
        #expect(config.retryLimit == 5)
        #expect(config.maxRetryDelay == 1000)  // Uses default
        #expect(config.priority == .system)
        #expect(config.readPriority == .high)
    }

    @Test("TransactionConfiguration.interactive has expected values")
    func interactiveConfigurationValues() {
        let config = TransactionConfiguration.interactive
        #expect(config.timeout == 1_000)
        #expect(config.retryLimit == 3)
        #expect(config.maxRetryDelay == 1000)  // Uses default
        #expect(config.priority == .default)
        #expect(config.readPriority == .normal)
    }

    @Test("Custom TransactionConfiguration")
    func customConfiguration() {
        let config = TransactionConfiguration(
            timeout: 5000,
            retryLimit: 8,
            maxRetryDelay: 500,
            priority: .batch,
            readPriority: .high,
            disableReadCache: true
        )
        #expect(config.timeout == 5000)
        #expect(config.retryLimit == 8)
        #expect(config.maxRetryDelay == 500)
        #expect(config.priority == .batch)
        #expect(config.readPriority == .high)
        #expect(config.disableReadCache == true)
    }

    // MARK: - TransactionContext Basic Operations

    @Test("TransactionContext set and get")
    func transactionContextSetAndGet() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "Alice", balance: 100)

        // Write and read within transaction
        try await context.withTransaction { tx in
            try await tx.set(user)
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched != nil)
            #expect(fetched?.name == "Alice")
            #expect(fetched?.balance == 100)
        }

        // Verify persisted after transaction commits
        try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched != nil)
            #expect(fetched?.name == "Alice")
        }
    }

    @Test("TransactionContext delete")
    func transactionContextDelete() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "Bob", balance: 200)

        // Insert
        try await context.withTransaction { tx in
            try await tx.set(user)
        }

        // Verify exists
        try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched != nil)
        }

        // Delete
        try await context.withTransaction { tx in
            try await tx.delete(TransactionTestUser.self, id: user.id)
        }

        // Verify deleted
        try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched == nil)
        }
    }

    @Test("TransactionContext getMany")
    func transactionContextGetMany() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user1 = TransactionTestUser(name: "User1", balance: 100)
        let user2 = TransactionTestUser(name: "User2", balance: 200)
        let user3 = TransactionTestUser(name: "User3", balance: 300)

        // Insert
        try await context.withTransaction { tx in
            try await tx.set(user1)
            try await tx.set(user2)
            try await tx.set(user3)
        }

        // GetMany
        try await context.withTransaction { tx in
            let users = try await tx.getMany(
                TransactionTestUser.self,
                ids: [user1.id, user2.id, user3.id]
            )
            #expect(users.count == 3)
        }

        // GetMany with missing ID
        try await context.withTransaction { tx in
            let users = try await tx.getMany(
                TransactionTestUser.self,
                ids: [user1.id, "nonexistent", user3.id]
            )
            #expect(users.count == 2)
        }
    }

    // MARK: - Snapshot Read Tests

    @Test("Snapshot read parameter works")
    func snapshotReadParameter() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "SnapshotTest", balance: 500)

        // Insert
        try await context.withTransaction { tx in
            try await tx.set(user)
        }

        // Snapshot read should work
        try await context.withTransaction { tx in
            // Transactional read (default)
            let transactionalRead = try await tx.get(
                TransactionTestUser.self,
                id: user.id,
                snapshot: false
            )
            #expect(transactionalRead != nil)

            // Snapshot read
            let snapshotRead = try await tx.get(
                TransactionTestUser.self,
                id: user.id,
                snapshot: true
            )
            #expect(snapshotRead != nil)

            // Both should return same data
            #expect(transactionalRead?.balance == snapshotRead?.balance)
        }
    }

    // MARK: - Read-Modify-Write Pattern

    @Test("Read-modify-write pattern")
    func readModifyWritePattern() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "ModifyTest", balance: 1000)

        // Insert initial value
        try await context.withTransaction { tx in
            try await tx.set(user)
        }

        // Read-modify-write
        try await context.withTransaction { tx in
            guard var fetched = try await tx.get(TransactionTestUser.self, id: user.id) else {
                throw TestError.userNotFound
            }
            fetched.balance -= 100
            try await tx.set(fetched)
        }

        // Verify modification
        try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched?.balance == 900)
        }
    }

    // MARK: - Transaction Configuration Application

    @Test("Batch configuration can be applied")
    func batchConfigurationApplication() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "BatchTest", balance: 100)

        // Use batch configuration
        try await context.withTransaction(configuration: .batch) { tx in
            try await tx.set(user)
        }

        // Verify
        try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched != nil)
        }
    }

    @Test("Interactive configuration can be applied")
    func interactiveConfigurationApplication() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "InteractiveTest", balance: 50)

        // Use interactive configuration
        try await context.withTransaction(configuration: .interactive) { tx in
            try await tx.set(user)
        }

        // Verify
        try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            #expect(fetched != nil)
        }
    }

    // MARK: - Return Value Tests

    @Test("withTransaction returns result")
    func withTransactionReturnsResult() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)
        let context = container.newContext()

        let user = TransactionTestUser(name: "ReturnTest", balance: 777)

        try await context.withTransaction { tx in
            try await tx.set(user)
        }

        // Transaction returns a value
        let balance: Int = try await context.withTransaction { tx in
            let fetched = try await tx.get(TransactionTestUser.self, id: user.id)
            return fetched?.balance ?? 0
        }

        #expect(balance == 777)
    }

    // MARK: - Backoff Algorithm Tests

    @Test("Exponential backoff calculation")
    func exponentialBackoffCalculation() {
        // Test exponential growth: 300ms * 2^attempt
        // Backoff includes jitter (0-50%), so we test bounds

        // Attempt 0: 300ms base
        let delay0 = TransactionRunner.calculateBackoff(attempt: 0, maxDelayMs: 10000)
        #expect(delay0 >= 300)
        #expect(delay0 <= 450)  // 300 + up to 50% jitter

        // Attempt 1: 600ms base
        let delay1 = TransactionRunner.calculateBackoff(attempt: 1, maxDelayMs: 10000)
        #expect(delay1 >= 600)
        #expect(delay1 <= 900)  // 600 + up to 50% jitter

        // Attempt 2: 1200ms base (but we test with high maxDelay)
        let delay2 = TransactionRunner.calculateBackoff(attempt: 2, maxDelayMs: 10000)
        #expect(delay2 >= 1200)
        #expect(delay2 <= 1800)  // 1200 + up to 50% jitter
    }

    @Test("Backoff respects max delay")
    func backoffRespectsMaxDelay() {
        // With maxDelay of 500ms, high attempts should be capped
        let delay = TransactionRunner.calculateBackoff(attempt: 10, maxDelayMs: 500)
        #expect(delay >= 500)
        #expect(delay <= 750)  // 500 + up to 50% jitter
    }

    @Test("Backoff handles edge cases")
    func backoffHandlesEdgeCases() {
        // Very high attempt number should not overflow
        let delayHigh = TransactionRunner.calculateBackoff(attempt: 100, maxDelayMs: 1000)
        #expect(delayHigh >= 1000)
        #expect(delayHigh <= 1500)  // Capped at maxDelay + jitter

        // Zero max delay
        let delayZero = TransactionRunner.calculateBackoff(attempt: 0, maxDelayMs: 0)
        #expect(delayZero == 0)  // Both base and jitter are 0
    }

    // MARK: - Error Types

    enum TestError: Error {
        case userNotFound
    }
}
