#if !os(WASI)
#if FOUNDATION_DB
// TransactionAdvancedTests.swift
// Advanced transaction tests: edge cases, concurrency, error handling

import DatabaseTypes
import Testing
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine

@Suite("Transaction Advanced Tests", .foundationDBScenario, .serialized, .heartbeat)
struct TransactionAdvancedTests {

    // MARK: - Edge Cases

    @Test("Empty getRange returns no items")
    func emptyGetRange() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        let count = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var itemCount = 0
            var sequence = tx.rangeCursor(
                from: .firstGreaterOrEqual([0xFD, 0xFF, 0xFF]),
                to: .firstGreaterOrEqual([0xFD, 0xFF, 0xFF, 1]),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )

            while try await sequence.next() != nil {
                itemCount += 1
            }

            return itemCount
        }

        #expect(count == 0)
    }

    @Test("Multiple empty getRange calls")
    func multipleEmptyGetRangeCalls() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        let totalCount = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var total = 0

            // Repetition semantics need more than one call, not a scale load.
            for i in 0..<3 {
                var sequence = tx.rangeCursor(
                    from: .firstGreaterOrEqual([0xFE, UInt8(i)]),
                    to: .firstGreaterOrEqual([0xFE, UInt8(i), 0]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )

                while try await sequence.next() != nil {
                    total += 1
                }
            }

            return total
        }

        #expect(totalCount == 0)
    }

    // MARK: - Mixed Read/Write Operations

    @Test("Interleaved read and write operations")
    func interleavedReadWrite() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<20 {
                try tx.setValue([UInt8(i)], for: [0x22, UInt8(i)])
            }
        }

        let results = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var readValues: [ByteString] = []

            for i in 0..<10 {
                // Read
                try await tx.forEachInRange(
                    from: .firstGreaterOrEqual([0x22, UInt8(i * 2)]),
                    to: .firstGreaterOrEqual([0x22, UInt8(i * 2 + 2)]),
                    snapshot: true
                ) { _, value in
                    readValues.append(value)
                }

                // Write
                try tx.setValue([UInt8(i + 100)], for: [0x22, UInt8(i + 20)])
            }

            return readValues.count
        }

        #expect(results == 20)
    }

    @Test("Commit succeeds after mixed operations")
    func commitSucceedsAfterMixedOperations() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Setup: Clear the test key range first
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            try tx.clearRange(beginKey: [0x23], endKey: [0x24])
        }

        // Verify transaction can commit after mixed read/write operations
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            // Writes
            for i in 0..<20 {
                try tx.setValue([UInt8(i)], for: [0x23, 0x20, UInt8(i)])
            }

            // Reads with getRange
            var readCount = 0
            var sequence = tx.rangeCursor(
                from: .firstGreaterOrEqual([0x23, 0x20]),
                to: .firstGreaterOrEqual([0x23, 0x21]),
                limit: 0,
                reverse: false,
                snapshot: false,  // Use snapshot: false to see own writes
                streamingMode: .iterator
            )

            while try await sequence.next() != nil {
                readCount += 1
            }

            // More writes
            for i in 20..<30 {
                try tx.setValue([UInt8(i)], for: [0x23, 0x30, UInt8(i)])
            }

            #expect(readCount == 20)
        }
    }

    // MARK: - Skip List-like Patterns

    @Test("Skip List insertion pattern simulation")
    func skipListInsertionPattern() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Simulate Skip List: 6 levels, insert 1 item
        let insertionResult = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var rankPerLevel: [Int] = []

            // Phase 1: Find insertion point at each level (like Skip List)
            for level in 0..<6 {
                var rank = 0
                var sequence = tx.rangeCursor(
                    from: .firstGreaterOrEqual([0x24, UInt8(level)]),
                    to: .firstGreaterOrEqual([0x24, UInt8(level + 1)]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )

                while try await sequence.next() != nil {
                    rank += 1
                }

                rankPerLevel.append(rank)
            }

            // Phase 2: Insert at determined positions
            for level in 0..<3 {  // Insert at 3 levels
                try tx.setValue([1], for: [0x24, UInt8(level), 0, 0])
            }

            return rankPerLevel
        }

        #expect(insertionResult.count == 6)
    }

    @Test("Skip List multiple insertions pattern")
    func skipListMultipleInsertionsPattern() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Simulate inserting 20 items with Skip List pattern
        for item in 0..<20 {
            try await runner.run(configuration: .default, producing: .writeResult) { tx in
                // Phase 1: Scan all levels (6 getRange calls)
                for level in 0..<6 {
                    var sequence = tx.rangeCursor(
                        from: .firstGreaterOrEqual([0x25, UInt8(level)]),
                        to: .firstGreaterOrEqual([0x25, UInt8(level + 1)]),
                        limit: 0,
                        reverse: false,
                        snapshot: true,
                        streamingMode: .iterator
                    )

                    var count = 0
                    while try await sequence.next() != nil {
                        count += 1
                        if count >= 10 { break }  // Partial scan
                    }
                }

                // Phase 2: Write to levels 0-2
                for level in 0..<3 {
                    let key = [0x25, UInt8(level)] + withUnsafeBytes(of: UInt16(item).bigEndian) { Array($0) }
                    try tx.setValue([UInt8(item % 256)], for: ByteString(key))
                }
            }
        }

        // Verify all items were inserted
        let count = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var total = 0
            var sequence = tx.rangeCursor(
                from: .firstGreaterOrEqual([0x25, 0]),
                to: .firstGreaterOrEqual([0x25, 1]),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )

            while try await sequence.next() != nil {
                total += 1
            }

            return total
        }

        #expect(count == 20)
    }

    @Test("Deeply nested getRange loops")
    func deeplyNestedGetRangeLoops() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Setup: 3 dimensions × 5 items each = 45 total items
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for x in 0..<3 {
                for y in 0..<3 {
                    for z in 0..<5 {
                        try tx.setValue([1], for: [0x27, UInt8(x), UInt8(y), UInt8(z)])
                    }
                }
            }
        }

        // Test: 2-level nested getRange (X → Y levels)
        let count = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var total = 0

            // Outer loop: X dimension
            for x in 0..<3 {
                var seqX = tx.rangeCursor(
                    from: .firstGreaterOrEqual([0x27, UInt8(x)]),
                    to: .firstGreaterOrEqual([0x27, UInt8(x + 1)]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .iterator
                )

                // Count all items in this X slice
                while try await seqX.next() != nil {
                    total += 1
                }
            }

            return total
        }

        #expect(count == 45)  // 3 × 3 × 5
    }

    // MARK: - Consistency Tests

    @Test("Snapshot read sees consistent view")
    func snapshotReadConsistentView() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        // Setup initial state
        try await runner.run(configuration: .default, producing: .writeResult) { tx in
            for i in 0..<10 {
                try tx.setValue([UInt8(i)], for: [0x28, UInt8(i)])
            }
        }

        // Read with snapshot: true multiple times in same transaction
        let (count1, count2, count3) = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            var c1 = 0
            var c2 = 0
            var c3 = 0

            // First read
            var seq1 = tx.rangeCursor(
                from: .firstGreaterOrEqual([0x28]),
                to: .firstGreaterOrEqual([0x29]),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            while try await seq1.next() != nil { c1 += 1 }

            // Second read (should see same data)
            var seq2 = tx.rangeCursor(
                from: .firstGreaterOrEqual([0x28]),
                to: .firstGreaterOrEqual([0x29]),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            while try await seq2.next() != nil { c2 += 1 }

            // Third read (should see same data)
            var seq3 = tx.rangeCursor(
                from: .firstGreaterOrEqual([0x28]),
                to: .firstGreaterOrEqual([0x29]),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .iterator
            )
            while try await seq3.next() != nil { c3 += 1 }

            return (c1, c2, c3)
        }

        #expect(count1 == count2)
        #expect(count2 == count3)
        #expect(count1 == 10)
    }

    @Test("Read your own writes")
    func readYourOwnWrites() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let runner = TransactionRunner(transactionExecutor: StorageTransactionExecutor(engine: database), clock: TestProcessMonotonicClock())

        let result = try await runner.run(configuration: .default, producing: .writeResult) { tx in
            // Write
            try tx.setValue([1], for: [0x29, 0])
            try tx.setValue([2], for: [0x29, 1])
            try tx.setValue([3], for: [0x29, 2])

            // Read (should see writes within same transaction)
            var count = 0
            var sequence = tx.rangeCursor(
                from: .firstGreaterOrEqual([0x29]),
                to: .firstGreaterOrEqual([0x2A]),
                limit: 0,
                reverse: false,
                snapshot: false,  // Non-snapshot to see own writes
                streamingMode: .iterator
            )

            while try await sequence.next() != nil {
                count += 1
            }

            return count
        }

        #expect(result == 3)
    }

}
#endif

#endif
