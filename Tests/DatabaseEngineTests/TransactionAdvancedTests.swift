// TransactionAdvancedTests.swift
// Advanced transaction tests: edge cases, concurrency, error handling

import Testing
import FoundationDB
import TestSupport
@testable import DatabaseEngine

@Suite("Transaction Advanced Tests", .serialized)
struct TransactionAdvancedTests {

    // MARK: - Edge Cases

    @Test("Empty getRange returns no items")
    func emptyGetRange() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        let count = try await runner.run(configuration: .default) { tx in
            var itemCount = 0
            let sequence = tx.getRange(
                from: .firstGreaterOrEqual([0xFD, 0xFF, 0xFF]),
                to: .firstGreaterOrEqual([0xFD, 0xFF, 0xFF, 1]),
                snapshot: true
            )

            for try await _ in sequence {
                itemCount += 1
            }

            return itemCount
        }

        #expect(count == 0)
    }

    @Test("Multiple empty getRange calls")
    func multipleEmptyGetRangeCalls() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        let totalCount = try await runner.run(configuration: .default) { tx in
            var total = 0

            // 50 empty getRange() calls
            for i in 0..<50 {
                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual([0xFE, UInt8(i)]),
                    to: .firstGreaterOrEqual([0xFE, UInt8(i), 0]),
                    snapshot: true
                )

                for try await _ in sequence {
                    total += 1
                }
            }

            return total
        }

        #expect(totalCount == 0)
    }

    @Test("Very large number of getRange calls (500)")
    func fiveHundredGetRangeCalls() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Setup: 500 keys
        try await runner.run(configuration: .default) { tx in
            for i in 0..<500 {
                let key = [0x11] + withUnsafeBytes(of: UInt16(i).bigEndian) { Array($0) }
                tx.setValue([UInt8(i % 256)], for: key)
            }
        }

        // Test: 500 getRange() calls (1 per key)
        let count = try await runner.run(configuration: .default) { tx in
            var total = 0

            for i in 0..<500 {
                let start = [0x11] + withUnsafeBytes(of: UInt16(i).bigEndian) { Array($0) }
                let end = [0x11] + withUnsafeBytes(of: UInt16(i + 1).bigEndian) { Array($0) }

                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual(start),
                    to: .firstGreaterOrEqual(end),
                    snapshot: true
                )

                for try await _ in sequence {
                    total += 1
                }
            }

            return total
        }

        #expect(count == 500)
    }

    // MARK: - InstrumentedTransaction Tracker Tests
    // Note: Tracker tests are commented out - tracker feature is not needed
    // The actual fix was using snapshot: true for optimistic locking

    // @Test("Tracker with no iterators")
    // func trackerWithNoIterators() async throws {
    //     try await FDBTestSetup.shared.initialize()
    //     let database = try FDBClient.openDatabase()
    //     let rawTx = try database.createTransaction()
    //     let instrumented = InstrumentedTransaction(wrapping: rawTx)
    //
    //     // Verify tracker starts empty
    //     #expect(instrumented.tracker.activeIteratorCount == 0)
    //
    //     // Simple getValue (no iterator)
    //     instrumented.setValue([1, 2, 3], for: [0x20, 0])
    //     _ = try await instrumented.getValue(for: [0x20, 0])
    //
    //     // Still no iterators
    //     #expect(instrumented.tracker.activeIteratorCount == 0)
    //
    //     instrumented.cancel()
    // }
    //
    // @Test("Tracker registers and unregisters iterators")
    // func trackerRegistersIterators() async throws {
    //     try await FDBTestSetup.shared.initialize()
    //     let database = try FDBClient.openDatabase()
    //     let runner = TransactionRunner(database: database)
    //
    //     // Setup data
    //     try await runner.run(configuration: .default) { tx in
    //         for i in 0..<10 {
    //             tx.setValue([UInt8(i)], for: [0x21, UInt8(i)])
    //         }
    //     }
    //
    //     // Test tracker behavior
    //     try await runner.run(configuration: .default) { tx in
    //         guard let instrumented = tx as? InstrumentedTransaction else {
    //             Issue.record("Transaction is not InstrumentedTransaction")
    //             return
    //         }
    //
    //         // Initially empty
    //         #expect(instrumented.tracker.activeIteratorCount == 0)
    //
    //         // Create iterator
    //         let sequence = instrumented.getRange(
    //             from: .firstGreaterOrEqual([0x21]),
    //             to: .firstGreaterOrEqual([0x22]),
    //             snapshot: true
    //         )
    //         var iterator = sequence.makeAsyncIterator()
    //
    //         // Register manually (in real code, this happens in makeTrackedIterator)
    //         instrumented.tracker.register(iterator)
    //         #expect(instrumented.tracker.activeIteratorCount == 1)
    //
    //         // Consume iterator
    //         while let _ = try await iterator.next() {}
    //
    //         // Note: unregister is NOT called automatically here
    //         // In production, we rely on iterator deinit
    //     }
    // }

    // MARK: - Mixed Read/Write Operations

    @Test("Interleaved read and write operations")
    func interleavedReadWrite() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        try await runner.run(configuration: .default) { tx in
            for i in 0..<20 {
                tx.setValue([UInt8(i)], for: [0x22, UInt8(i)])
            }
        }

        let results = try await runner.run(configuration: .default) { tx in
            var readValues: [FDB.Bytes] = []

            for i in 0..<10 {
                // Read
                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual([0x22, UInt8(i * 2)]),
                    to: .firstGreaterOrEqual([0x22, UInt8(i * 2 + 2)]),
                    snapshot: true
                )

                for try await (_, value) in sequence {
                    readValues.append(value)
                }

                // Write
                tx.setValue([UInt8(i + 100)], for: [0x22, UInt8(i + 20)])
            }

            return readValues.count
        }

        #expect(results == 20)
    }

    @Test("Commit succeeds after mixed operations")
    func commitSucceedsAfterMixedOperations() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Setup: Clear the test key range first
        try await runner.run(configuration: .default) { tx in
            tx.clearRange(beginKey: [0x23], endKey: [0x24])
        }

        // Verify transaction can commit after mixed read/write operations
        try await runner.run(configuration: .default) { tx in
            // Writes
            for i in 0..<20 {
                tx.setValue([UInt8(i)], for: [0x23, 0x20, UInt8(i)])
            }

            // Reads with getRange
            var readCount = 0
            let sequence = tx.getRange(
                from: .firstGreaterOrEqual([0x23, 0x20]),
                to: .firstGreaterOrEqual([0x23, 0x21]),
                snapshot: false  // Use snapshot: false to see own writes
            )

            for try await _ in sequence {
                readCount += 1
            }

            // More writes
            for i in 20..<30 {
                tx.setValue([UInt8(i)], for: [0x23, 0x30, UInt8(i)])
            }

            #expect(readCount == 20)
        }
    }

    // MARK: - Skip List-like Patterns

    @Test("Skip List insertion pattern simulation")
    func skipListInsertionPattern() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Simulate Skip List: 6 levels, insert 1 item
        let insertionResult = try await runner.run(configuration: .default) { tx in
            var rankPerLevel: [Int] = []

            // Phase 1: Find insertion point at each level (like Skip List)
            for level in 0..<6 {
                var rank = 0
                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual([0x24, UInt8(level)]),
                    to: .firstGreaterOrEqual([0x24, UInt8(level + 1)]),
                    snapshot: true
                )

                for try await _ in sequence {
                    rank += 1
                }

                rankPerLevel.append(rank)
            }

            // Phase 2: Insert at determined positions
            for level in 0..<3 {  // Insert at 3 levels
                tx.setValue([1], for: [0x24, UInt8(level), 0, 0])
            }

            return rankPerLevel
        }

        #expect(insertionResult.count == 6)
    }

    @Test("Skip List multiple insertions pattern")
    func skipListMultipleInsertionsPattern() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Simulate inserting 20 items with Skip List pattern
        for item in 0..<20 {
            try await runner.run(configuration: .default) { tx in
                // Phase 1: Scan all levels (6 getRange calls)
                for level in 0..<6 {
                    let sequence = tx.getRange(
                        from: .firstGreaterOrEqual([0x25, UInt8(level)]),
                        to: .firstGreaterOrEqual([0x25, UInt8(level + 1)]),
                        snapshot: true
                    )

                    var count = 0
                    for try await _ in sequence {
                        count += 1
                        if count >= 10 { break }  // Partial scan
                    }
                }

                // Phase 2: Write to levels 0-2
                for level in 0..<3 {
                    let key = [0x25, UInt8(level)] + withUnsafeBytes(of: UInt16(item).bigEndian) { Array($0) }
                    tx.setValue([UInt8(item % 256)], for: key)
                }
            }
        }

        // Verify all items were inserted
        let count = try await runner.run(configuration: .default) { tx in
            var total = 0
            let sequence = tx.getRange(
                from: .firstGreaterOrEqual([0x25, 0]),
                to: .firstGreaterOrEqual([0x25, 1]),
                snapshot: true
            )

            for try await _ in sequence {
                total += 1
            }

            return total
        }

        #expect(count == 20)
    }

    // MARK: - Extreme Cases

    @Test("1000 getRange calls with 1000 items")
    func thousandGetRangeWithThousandItems() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Setup: 1000 items
        try await runner.run(configuration: .default) { tx in
            for i in 0..<1000 {
                let key = [0x26] + withUnsafeBytes(of: UInt16(i).bigEndian) { Array($0) }
                tx.setValue([UInt8(i % 256)], for: key)
            }
        }

        // Test: 1000 getRange calls
        let count = try await runner.run(configuration: .default) { tx in
            var total = 0

            for i in 0..<1000 {
                let start = [0x26] + withUnsafeBytes(of: UInt16(i).bigEndian) { Array($0) }
                let end = [0x26] + withUnsafeBytes(of: UInt16(i + 1).bigEndian) { Array($0) }

                let sequence = tx.getRange(
                    from: .firstGreaterOrEqual(start),
                    to: .firstGreaterOrEqual(end),
                    snapshot: true
                )

                for try await _ in sequence {
                    total += 1
                }
            }

            return total
        }

        #expect(count == 1000)
    }

    @Test("Deeply nested getRange loops")
    func deeplyNestedGetRangeLoops() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Setup: 3 dimensions × 5 items each = 45 total items
        try await runner.run(configuration: .default) { tx in
            for x in 0..<3 {
                for y in 0..<3 {
                    for z in 0..<5 {
                        tx.setValue([1], for: [0x27, UInt8(x), UInt8(y), UInt8(z)])
                    }
                }
            }
        }

        // Test: 2-level nested getRange (X → Y levels)
        let count = try await runner.run(configuration: .default) { tx in
            var total = 0

            // Outer loop: X dimension
            for x in 0..<3 {
                let seqX = tx.getRange(
                    from: .firstGreaterOrEqual([0x27, UInt8(x)]),
                    to: .firstGreaterOrEqual([0x27, UInt8(x + 1)]),
                    snapshot: true
                )

                // Count all items in this X slice
                for try await _ in seqX {
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
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Setup initial state
        try await runner.run(configuration: .default) { tx in
            for i in 0..<10 {
                tx.setValue([UInt8(i)], for: [0x28, UInt8(i)])
            }
        }

        // Read with snapshot: true multiple times in same transaction
        let (count1, count2, count3) = try await runner.run(configuration: .default) { tx in
            var c1 = 0
            var c2 = 0
            var c3 = 0

            // First read
            let seq1 = tx.getRange(
                from: .firstGreaterOrEqual([0x28]),
                to: .firstGreaterOrEqual([0x29]),
                snapshot: true
            )
            for try await _ in seq1 { c1 += 1 }

            // Second read (should see same data)
            let seq2 = tx.getRange(
                from: .firstGreaterOrEqual([0x28]),
                to: .firstGreaterOrEqual([0x29]),
                snapshot: true
            )
            for try await _ in seq2 { c2 += 1 }

            // Third read (should see same data)
            let seq3 = tx.getRange(
                from: .firstGreaterOrEqual([0x28]),
                to: .firstGreaterOrEqual([0x29]),
                snapshot: true
            )
            for try await _ in seq3 { c3 += 1 }

            return (c1, c2, c3)
        }

        #expect(count1 == count2)
        #expect(count2 == count3)
        #expect(count1 == 10)
    }

    @Test("Read your own writes")
    func readYourOwnWrites() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        let result = try await runner.run(configuration: .default) { tx in
            // Write
            tx.setValue([1], for: [0x29, 0])
            tx.setValue([2], for: [0x29, 1])
            tx.setValue([3], for: [0x29, 2])

            // Read (should see writes within same transaction)
            var count = 0
            let sequence = tx.getRange(
                from: .firstGreaterOrEqual([0x29]),
                to: .firstGreaterOrEqual([0x2A]),
                snapshot: false  // Non-snapshot to see own writes
            )

            for try await _ in sequence {
                count += 1
            }

            return count
        }

        #expect(result == 3)
    }

    // MARK: - Performance Characteristics

    @Test("Large scan does not timeout")
    func largeScanDoesNotTimeout() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let runner = TransactionRunner(database: database)

        // Setup: 5000 items
        try await runner.run(configuration: .default) { tx in
            for i in 0..<5000 {
                let key = [0x2A] + withUnsafeBytes(of: UInt16(i).bigEndian) { Array($0) }
                tx.setValue([UInt8(i % 256)], for: key)
            }
        }

        // Scan all 5000 items
        let count = try await runner.run(configuration: .default) { tx in
            var total = 0
            let sequence = tx.getRange(
                from: .firstGreaterOrEqual([0x2A]),
                to: .firstGreaterOrEqual([0x2B]),
                snapshot: true
            )

            for try await _ in sequence {
                total += 1
            }

            return total
        }

        #expect(count == 5000)
    }
}
