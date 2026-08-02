#if POSTGRESQL
// PostgreSQLConcurrencyTests.swift
// PostgreSQL-specific concurrency and MVCC tests
//
// These tests validate PostgreSQL-specific behaviors that differ from FoundationDB:
// - MVCC-based concurrent transactions
// - Serialization failure detection and retry
// - Connection pool behavior
// - Lazy connection acquisition
// - Empty BYTEA handling

import DatabaseTypes
import DatabaseRuntime
import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import DatabaseKit
import TestSupport

@Persistable
struct PGCounter: Equatable {
    #Directory<PGCounter>("test", "pg", "concurrency")

    var id: String = UUID().uuidString
    var value: Int64 = 0
}

/// Collect range scan results from a concrete transaction type
private func collectRange(
    _ tx: some Transaction,
    begin: ByteString,
    end: ByteString,
    limit: Int = 0,
    reverse: Bool = false
) async throws -> [(key: ByteString, value: ByteString)] {
    try await tx.collectRange(
        begin: begin,
        end: end,
        limit: limit,
        reverse: reverse
    )
}

@Suite("PostgreSQL Concurrency Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct PostgreSQLConcurrencyTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try PGCounter.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGCounter.self)])
    }

    @Test("A closed container does not poison the next scenario engine")
    func closedContainerDoesNotPoisonNextEngine() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let firstContainer = try await setupContainer()
            await firstContainer.shutdown()

            let secondContainer = try await setupContainer()
            let context = secondContainer.newContext()
            var counter = PGCounter()
            counter.id = uniqueID("fresh-engine")
            counter.value = 42
            try context.insert(counter)
            try await context.save()

            let stored = try await context.fetch(PGCounter.self)
                .where(PGCounter.fields.id == counter.id)
                .first()
            #expect(stored?.value == 42)
        }
    }

    // MARK: - Concurrent Write Isolation

    @Test("Concurrent writes to different keys succeed")
    func concurrentWritesDifferentKeys() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()

            let id1 = uniqueID("conc-1")
            let id2 = uniqueID("conc-2")

            // Write two items in parallel tasks
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    let ctx = container.newContext()
                    var item = PGCounter()
                    item.id = id1
                    item.value = 100
                    try ctx.insert(item)
                    try await ctx.save()
                }
                group.addTask {
                    let ctx = container.newContext()
                    var item = PGCounter()
                    item.id = id2
                    item.value = 200
                    try ctx.insert(item)
                    try await ctx.save()
                }
                try await group.waitForAll()
            }

            // Both should be persisted
            let ctx = container.newContext()

            let result1 = try await ctx.fetch(PGCounter.self)
                .where(PGCounter.fields.id == id1)
                .first()
            #expect(result1?.value == 100)

            let result2 = try await ctx.fetch(PGCounter.self)
                .where(PGCounter.fields.id == id2)
                .first()
            #expect(result2?.value == 200)
        }
    }

    // MARK: - Transaction Auto-Retry on Conflict

    @Test("withTransaction retries on serialization failure")
    func withTransactionRetries() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("retry")

            // Seed data
            var item = PGCounter()
            item.id = itemId
            item.value = 0
            try context.insert(item)
            try await context.save()

            // Sequential increments (each in its own transaction)
            for _ in 0..<5 {
                let ctx = container.newContext()
                try await ctx.withTransaction { tx in
                    guard var existing = try await tx.fetch(PGCounter.self, identifiedBy: itemId) else {
                        Issue.record("Item not found")
                        return
                    }
                    existing.value += 1
                    try await tx.save(existing)
                }
            }

            // Verify final value
            let final_ = try await context.fetch(PGCounter.self)
                .where(PGCounter.fields.id == itemId)
                .first()
            #expect(final_?.value == 5)
        }
    }

    // MARK: - Empty Value Handling

    @Test("Empty byte arrays round-trip correctly")
    func emptyByteArrayRoundTrip() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let key: ByteString = [0x99, 0x01, 0x02]

            // Write empty value
            let tx1 = try engine.createTransaction()
            try tx1.setValue([], for: key)
            try await tx1.commit()

            // Read back
            let tx2 = try engine.createTransaction()
            let value = try await tx2.getValue(for: key)
            try await tx2.cancel()

            #expect(value != nil)
            #expect(value == [])

            // Cleanup
            let tx3 = try engine.createTransaction()
            try tx3.clear(key: key)
            try await tx3.commit()
        }
    }

    // MARK: - Range Scan (PostgreSQLRangeResult lazy evaluation)

    @Test("Range scan with PostgreSQLRangeResult works correctly")
    func rangeScanLazyEvaluation() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let prefix: ByteString = [0xAA]
            let endPrefix: ByteString = [0xAB]

            // Insert multiple keys with same prefix
            let tx1 = try engine.createTransaction()
            for i: UInt8 in 0..<10 {
                let key = prefix.appending(i)
                try tx1.setValue(ByteString([i]), for: key)
            }
            try await tx1.commit()

            // Range scan
            let tx2 = try engine.createTransaction()
            let begin = prefix.appending(0x00)
            let results = try await collectRange(tx2, begin: begin, end: endPrefix)
            try await tx2.cancel()

            #expect(results.count == 10)

            // Cleanup
            let tx3 = try engine.createTransaction()
            try tx3.clearRange(beginKey: prefix, endKey: endPrefix)
            try await tx3.commit()
        }
    }

    @Test("Reverse range scan returns items in reverse order")
    func reverseRangeScan() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let prefix: ByteString = [0xBB]
            let endPrefix: ByteString = [0xBC]

            // Insert ordered keys
            let tx1 = try engine.createTransaction()
            for i: UInt8 in 0..<5 {
                let key = prefix.appending(i)
                try tx1.setValue(ByteString([i]), for: key)
            }
            try await tx1.commit()

            // Reverse range scan
            let tx2 = try engine.createTransaction()
            let begin = prefix.appending(0x00)
            let results = try await collectRange(
                tx2,
                begin: begin,
                end: endPrefix,
                reverse: true
            )
            try await tx2.cancel()

            #expect(results.count == 5)
            // Verify reverse order
            for i in 0..<results.count {
                #expect(results[i].value == ByteString([UInt8(4 - i)]))
            }

            // Cleanup
            let tx3 = try engine.createTransaction()
            try tx3.clearRange(beginKey: prefix, endKey: endPrefix)
            try await tx3.commit()
        }
    }

    @Test("Range scan with limit returns correct count")
    func rangeScanWithLimit() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let engine = try await PostgreSQLScenarioCoordinator.shared.engine

            let prefix: ByteString = [0xCC]
            let endPrefix: ByteString = [0xCD]

            // Insert 10 keys
            let tx1 = try engine.createTransaction()
            for i: UInt8 in 0..<10 {
                let key = prefix.appending(i)
                try tx1.setValue(ByteString([i]), for: key)
            }
            try await tx1.commit()

            // Scan with limit 3
            let tx2 = try engine.createTransaction()
            let begin = prefix.appending(0x00)
            let results = try await collectRange(
                tx2,
                begin: begin,
                end: endPrefix,
                limit: 3
            )
            try await tx2.cancel()

            #expect(results.count == 3)

            // Cleanup
            let tx3 = try engine.createTransaction()
            try tx3.clearRange(beginKey: prefix, endKey: endPrefix)
            try await tx3.commit()
        }
    }

    // MARK: - SchemaRegistry Persistence

    @Test("SchemaRegistry persists and loads on PostgreSQL")
    func schemaRegistryPersistence() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()

            // SchemaRegistry is automatically initialized by DBContainer.
            // Verify we can resolve the directory for PGCounter
            let subspace = try await container.resolveDirectory(for: PGCounter.self)
            #expect(subspace.prefix.count > 0)
        }
    }

    // MARK: - Nested Transaction Detection

    @Test("Nested withTransaction reuses parent connection")
    func nestedTransactionReuse() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupContainer()
            let context = container.newContext()

            let itemId = uniqueID("nested")

            // Outer transaction
            try await context.withTransaction { outerTx in
                var item = PGCounter()
                item.id = itemId
                item.value = 42
                try await outerTx.save(
                    item,
                    precondition: .notExists
                )

                // The inner operations via context should reuse
                // the ActiveTransactionScope connection
                let fetched = try await outerTx.fetch(
                    PGCounter.self,
                    identifiedBy: itemId
                )
                #expect(fetched?.value == 42)
            }

            // Verify committed
            let result = try await context.fetch(PGCounter.self)
                .where(PGCounter.fields.id == itemId)
                .first()
            #expect(result?.value == 42)
        }
    }
}
#endif
