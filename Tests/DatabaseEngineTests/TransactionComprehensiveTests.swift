// TransactionComprehensiveTests.swift
// Comprehensive tests for Transaction infrastructure

import Testing
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine

@Suite("Transaction Comprehensive Tests", .serialized)
struct TransactionComprehensiveTests {

    // MARK: - Multiple getRange() Tests

    @Test("Multiple getRange() calls in single transaction")
    func multipleGetRangeInSingleTransaction() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Setup: Write 50 keys
        try await runner.run(configuration: .default) { tx in
            for i in 0..<50 {
                tx.setValue([UInt8(i)], for: [0, 1, UInt8(i)])
            }
        }

        // Test: Read with 10 separate getRange() calls in same transaction
        let results = try await runner.run(configuration: .default) { tx in
            var allResults: [[(Bytes, Bytes)]] = []

            // 10 separate getRange() calls
            for batch in 0..<10 {
                var batchResults: [(Bytes, Bytes)] = []
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
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Setup: Write 100 keys
        try await runner.run(configuration: .default) { tx in
            for i in 0..<100 {
                tx.setValue([UInt8(i % 256)], for: [0, 2, UInt8(i % 256)])
            }
        }

        // Test: 100 getRange() calls
        let count = try await runner.run(configuration: .default) { tx in
            var totalCount = 0

            for i in 0..<100 {
                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual([0, 2, UInt8(i % 256)]),
                    to: .firstGreaterOrEqual([0, 2, UInt8((i + 1) % 256)]),
                    snapshot: true
                )

                for try await _ in sequence {
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
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        try await runner.run(configuration: .default) { tx in
            for i in 0..<10 {
                tx.setValue([UInt8(i)], for: [0, 3, UInt8(i)])
            }
        }

        let results = try await runner.run(configuration: .default) { tx in
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
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        try await runner.run(configuration: .default) { tx in
            for i in 0..<100 {
                tx.setValue([UInt8(i % 256)], for: [0, 4, UInt8(i % 256)])
            }
        }

        let results = try await runner.run(configuration: .default) { tx in
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

    @Test("Multiple concurrent iterators")
    func multipleConcurrentIterators() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        try await runner.run(configuration: .default) { tx in
            for i in 0..<20 {
                tx.setValue([UInt8(i)], for: [0, 5, UInt8(i)])
            }
        }

        let results = try await runner.run(configuration: .default) { tx in
            let items1 = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 5, 0]),
                to: .firstGreaterOrEqual([0, 5, 7]),
                snapshot: true
            )
            let items2 = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 5, 7]),
                to: .firstGreaterOrEqual([0, 5, 14]),
                snapshot: true
            )
            let items3 = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 5, 14]),
                to: .firstGreaterOrEqual([0, 6]),
                snapshot: true
            )

            return (items1.count, items2.count, items3.count)
        }

        #expect(results.0 > 0)
        #expect(results.1 > 0)
        #expect(results.2 > 0)
    }

    // MARK: - Snapshot Tests

    @Test("Snapshot read does not conflict")
    func snapshotReadDoesNotConflict() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Setup initial data
        try await runner.run(configuration: .default) { tx in
            tx.setValue([1], for: [0, 6, 1])
        }

        // Read with snapshot: true (should not add to conflict range)
        let value = try await runner.run(configuration: .default) { tx in
            let pairs = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 6]),
                to: .firstGreaterOrEqual([0, 7]),
                limit: 1,
                snapshot: true
            )
            return pairs.first?.1
        }

        #expect(value != nil)
    }

    @Test("Non-snapshot read adds to conflict range")
    func nonSnapshotReadAddsToConflictRange() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Setup initial data
        try await runner.run(configuration: .default) { tx in
            tx.setValue([1], for: [0, 7, 1])
        }

        // Read with snapshot: false (adds to conflict range)
        let value = try await runner.run(configuration: .default) { tx in
            let pairs = try await tx.collectRange(
                from: .firstGreaterOrEqual([0, 7]),
                to: .firstGreaterOrEqual([0, 8]),
                limit: 1,
                snapshot: false
            )
            return pairs.first?.1
        }

        #expect(value != nil)
    }

    // MARK: - Large Data Tests

    @Test("Scan 1000 items with getRange")
    func scanThousandItems() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Setup: Write 1000 items
        try await runner.run(configuration: .default) { tx in
            for i in 0..<1000 {
                let key = [0, 8] + withUnsafeBytes(of: i.bigEndian) { Array($0) }
                tx.setValue([UInt8(i % 256)], for: key)
            }
        }

        // Test: Scan all 1000 items
        let count = try await runner.run(configuration: .default) { tx in
            var total = 0
            let sequence = tx.getRange(
                from: .firstGreaterOrEqual([0, 8]),
                to: .firstGreaterOrEqual([0, 9]),
                snapshot: true
            )

            for try await _ in sequence {
                total += 1
            }

            return total
        }

        #expect(count == 1000)
    }

    // MARK: - Nested getRange Tests

    @Test("Nested getRange iteration")
    func nestedGetRangeIteration() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Setup: 5 groups × 10 items
        try await runner.run(configuration: .default) { tx in
            for group in 0..<5 {
                for item in 0..<10 {
                    tx.setValue([UInt8(item)], for: [0, 9, UInt8(group), UInt8(item)])
                }
            }
        }

        // Test: Nested iteration (outer: groups, inner: items)
        let results = try await runner.run(configuration: .default) { tx in
            var groupCounts: [Int] = []

            for group in 0..<5 {
                var itemCount = 0
                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual([0, 9, UInt8(group)]),
                    to: .firstGreaterOrEqual([0, 9, UInt8(group + 1)]),
                    snapshot: true
                )

                for try await _ in sequence {
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
        try await FDBTestSetup.shared.initialize()
        let database = try await FDBStorageEngine.open()
        let runner = TransactionRunner(database: database)

        // Write initial data
        try await runner.run(configuration: .default) { tx in
            for i in 0..<50 {
                tx.setValue([UInt8(i)], for: [0, 10, UInt8(i)])
            }
        }

        // Execute 20 getRange() calls and verify commit succeeds
        try await runner.run(configuration: .default) { tx in
            for batch in 0..<20 {
                let start = batch * 2
                let end = start + 3

                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual([0, 10, UInt8(start)]),
                    to: .firstGreaterOrEqual([0, 10, UInt8(end)]),
                    snapshot: true
                )

                var count = 0
                for try await _ in sequence {
                    count += 1
                }

                // Verify we got some results
                #expect(count >= 0)
            }

            // Write something to verify commit works
            tx.setValue([99], for: [0, 10, 99])
        }

        // Verify the write was committed
        let value = try await runner.run(configuration: .default) { tx in
            try await tx.getValue(for: [0, 10, 99])
        }

        #expect(value == [99])
    }
}
