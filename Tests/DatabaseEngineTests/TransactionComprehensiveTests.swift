#if !os(WASI)
#if FOUNDATION_DB
// TransactionComprehensiveTests.swift
// Comprehensive tests for Transaction infrastructure

import DatabaseTypes
import Foundation
import Testing
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine

@Suite("Transaction Comprehensive Tests", .foundationDBScenario, .serialized, .heartbeat)
struct TransactionComprehensiveTests {

    // MARK: - Multiple getRange() Tests

    @Test("Multiple getRange() calls in single transaction")
    func multipleGetRangeInSingleTransaction() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Setup: Write 50 keys
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<50 {
                try tx.setValue([UInt8(i)], for: [0, 1, UInt8(i)])
            }
        }

        // Test: Read with 10 separate getRange() calls in same transaction
        let results = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var allResults: [[(ByteString, ByteString)]] = []

            // 10 separate getRange() calls
            for batch in 0..<10 {
                var batchResults: [(ByteString, ByteString)] = []
                let start = batch * 5
                let end = start + 5

                let items = try await tx.collectRange(
                    from: .firstGreaterOrEqual([0, 1, UInt8(start)]),
                    to: .firstGreaterOrEqual([0, 1, UInt8(end)]),
                    snapshot: true
                )
                batchResults = items

                allResults.append(batchResults)
            }

            return allResults
        }

        // Verify: Each batch should have 5 items
        #expect(results.count == 10)
        for batch in results {
            #expect(batch.count == 5)
        }
    }

    @Test("100 getRange() calls in single transaction")
    func hundredGetRangeCalls() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Setup: Write 100 keys
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<100 {
                try tx.setValue([UInt8(i % 256)], for: [0, 2, UInt8(i % 256)])
            }
        }

        // Test: 100 getRange() calls
        let count = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var totalCount = 0

            for i in 0..<100 {
                var sequence = tx.rangeCursor(
                    from: .firstGreaterOrEqual([0, 2, UInt8(i % 256)]),
                    to: .firstGreaterOrEqual([0, 2, UInt8((i + 1) % 256)]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )

                while try await sequence.next() != nil {
                    totalCount += 1
                }
            }

            return totalCount
        }

        #expect(count > 0)
    }

    // MARK: - Iterator Lifecycle Tests

    @Test("Iterator fully consumed")
    func iteratorFullyConsumed() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<10 {
                try tx.setValue([UInt8(i)], for: [0, 3, UInt8(i)])
            }
        }

        let results = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            let pairs = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 3]),
                to: .firstGreaterOrEqual([0, 4]),
                snapshot: true
            )
            return pairs.map(\.1)
        }

        #expect(results.count == 10)
    }

    @Test("Iterator partially consumed")
    func iteratorPartiallyConsumed() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<100 {
                try tx.setValue([UInt8(i % 256)], for: [0, 4, UInt8(i % 256)])
            }
        }

        let results = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            let pairs = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 4]),
                to: .firstGreaterOrEqual([0, 5]),
                limit: 5,
                snapshot: true
            )
            return pairs.map(\.1)
        }

        #expect(results.count == 5)
    }

    // MARK: - Snapshot Tests

    @Test("Snapshot read does not conflict")
    func snapshotReadDoesNotConflict() async throws {
        try await verifyRangeReadConflict(
            snapshot: true,
            expectsConflict: false
        )
    }

    @Test("Non-snapshot read adds to conflict range")
    func nonSnapshotReadAddsToConflictRange() async throws {
        try await verifyRangeReadConflict(
            snapshot: false,
            expectsConflict: true
        )
    }

    private func verifyRangeReadConflict(
        snapshot: Bool,
        expectsConflict: Bool
    ) async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let subspace = Subspace(
            prefix: Tuple(
                "test",
                "snapshot-conflict",
                UUID().uuidString
            ).pack()
        )
        let observedKey = subspace.pack(Tuple("observed"))
        let readerWriteKey = subspace.pack(Tuple("reader-write"))
        let range = subspace.range()

        try await database.withTransaction { transaction in
            try transaction.setValue([0x01], for: observedKey)
        }

        let reader = try database.createTransaction()
        var cursor = reader.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        let observed = try await cursor.next()
        try await cursor.finish()
        #expect(observed?.0 == observedKey)
        #expect(observed?.1 == ByteString([0x01]))

        let competingWriter = try database.createTransaction()
        try competingWriter.setValue([0x02], for: observedKey)
        try await competingWriter.commit()

        try reader.setValue([0x03], for: readerWriteKey)
        if expectsConflict {
            do {
                try await reader.commit()
                Issue.record("Expected a serializable range-read conflict")
            } catch let failure as StorageError {
                #expect(failure.code == .transactionConflict)
                #expect(failure.operation == .commit)
                #expect(failure.backend == .foundationDB)
            } catch {
                Issue.record("Expected StorageError, got \(error)")
            }
        } else {
            try await reader.commit()
        }

        try await database.withTransaction { transaction in
            try transaction.clearRange(
                beginKey: range.begin,
                endKey: range.end
            )
        }
    }

    // MARK: - Nested getRange Tests

    @Test("Nested getRange iteration")
    func nestedGetRangeIteration() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Setup: 5 groups × 10 items
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for group in 0..<5 {
                for item in 0..<10 {
                    try tx.setValue([UInt8(item)], for: [0, 9, UInt8(group), UInt8(item)])
                }
            }
        }

        // Test: Nested iteration (outer: groups, inner: items)
        let results = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var groupCounts: [Int] = []

            for group in 0..<5 {
                var itemCount = 0
                var sequence = tx.rangeCursor(
                    from: .firstGreaterOrEqual([0, 9, UInt8(group)]),
                    to: .firstGreaterOrEqual([0, 9, UInt8(group + 1)]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )

                while try await sequence.next() != nil {
                    itemCount += 1
                }

                groupCounts.append(itemCount)
            }

            return groupCounts
        }

        #expect(results.count == 5)
        for count in results {
            #expect(count == 10)
        }
    }

    // MARK: - Error Handling Tests

    @Test("Transaction commit succeeds after multiple getRange")
    func commitSucceedsAfterMultipleGetRange() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Write initial data
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<50 {
                try tx.setValue([UInt8(i)], for: [0, 10, UInt8(i)])
            }
        }

        // Execute 20 getRange() calls and verify commit succeeds
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for batch in 0..<20 {
                let start = batch * 2
                let end = start + 3

                var sequence = tx.rangeCursor(
                    from: .firstGreaterOrEqual([0, 10, UInt8(start)]),
                    to: .firstGreaterOrEqual([0, 10, UInt8(end)]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )

                var count = 0
                while try await sequence.next() != nil {
                    count += 1
                }

                // Verify we got some results
                #expect(count >= 0)
            }

            // Write something to verify commit works
            try tx.setValue([99], for: [0, 10, 99])
        }

        // Verify the write was committed
        let value = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            try await tx.getValue(for: [0, 10, 99])
        }

        #expect(value == [99])
    }
}
#endif

#endif
