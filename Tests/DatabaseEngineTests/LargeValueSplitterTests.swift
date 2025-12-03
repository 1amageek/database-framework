// LargeValueSplitterTests.swift
// DatabaseEngine Tests - LargeValueSplitter tests for handling values > 100KB

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine

@Suite("LargeValueSplitter Tests", .serialized)
struct LargeValueSplitterTests {

    // MARK: - Setup

    private func setupDatabase() async throws -> any DatabaseProtocol {
        try await FDBTestEnvironment.shared.ensureInitialized()
        return try FDBClient.openDatabase()
    }

    private func testSubspace() -> Subspace {
        Subspace(prefix: Tuple("test", "splitter", UUID().uuidString).pack())
    }

    // MARK: - Small Value Tests (No Splitting)

    @Test("Small value is stored directly")
    func smallValueStoredDirectly() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let testData: [UInt8] = Array(repeating: 0x41, count: 1000)
        let baseKey = subspace.pack(Tuple(["small"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        // Verify: load returns same data
        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }

        #expect(loaded == testData)

        // Verify: not split
        let isSplit = try await database.withTransaction { transaction in
            try await splitter.isSplit(for: baseKey, transaction: transaction)
        }
        #expect(isSplit == false)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Value at threshold is not split")
    func valueAtThresholdNotSplit() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        // Exactly 90KB (threshold)
        let testData: [UInt8] = Array(repeating: 0x42, count: 90_000)
        let baseKey = subspace.pack(Tuple(["threshold"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        let isSplit = try await database.withTransaction { transaction in
            try await splitter.isSplit(for: baseKey, transaction: transaction)
        }
        #expect(isSplit == false)

        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Large Value Tests (Splitting)

    @Test("Large value is split and reassembled")
    func largeValueSplitAndReassembled() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        // 200KB - should be split into 3 parts
        let testData: [UInt8] = (0..<200_000).map { UInt8(truncatingIfNeeded: $0) }
        let baseKey = subspace.pack(Tuple(["large"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        // Verify: is split
        let isSplit = try await database.withTransaction { transaction in
            try await splitter.isSplit(for: baseKey, transaction: transaction)
        }
        #expect(isSplit == true)

        // Verify: loads correctly
        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Very large value split into many parts")
    func veryLargeValueSplit() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        // 500KB - should be split into 6 parts
        let testData: [UInt8] = (0..<500_000).map { UInt8(truncatingIfNeeded: $0 * 7) }
        let baseKey = subspace.pack(Tuple(["very_large"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Delete Tests

    @Test("Delete small value")
    func deleteSmallValue() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let testData: [UInt8] = Array(repeating: 0x43, count: 1000)
        let baseKey = subspace.pack(Tuple(["delete_small"]))

        // Save
        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        // Delete
        try await database.withTransaction { transaction in
            try await splitter.delete(for: baseKey, transaction: transaction)
        }

        // Verify: not found
        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == nil)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Delete split value removes all parts")
    func deleteSplitValueRemovesAllParts() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        // Large value that will be split
        let testData: [UInt8] = Array(repeating: 0x44, count: 200_000)
        let baseKey = subspace.pack(Tuple(["delete_large"]))

        // Save
        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        // Verify split
        let isSplit = try await database.withTransaction { transaction in
            try await splitter.isSplit(for: baseKey, transaction: transaction)
        }
        #expect(isSplit == true)

        // Delete
        try await database.withTransaction { transaction in
            try await splitter.delete(for: baseKey, transaction: transaction)
        }

        // Verify: not found
        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == nil)

        // Verify: header also deleted
        let stillSplit = try await database.withTransaction { transaction in
            try await splitter.isSplit(for: baseKey, transaction: transaction)
        }
        #expect(stillSplit == false)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - getSize Tests

    @Test("getSize returns correct size for small value")
    func getSizeSmallValue() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let testData: [UInt8] = Array(repeating: 0x45, count: 5000)
        let baseKey = subspace.pack(Tuple(["size_small"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        let size = try await database.withTransaction { transaction in
            try await splitter.getSize(for: baseKey, transaction: transaction)
        }
        #expect(size == 5000)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("getSize returns correct size for split value")
    func getSizeSplitValue() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let testData: [UInt8] = Array(repeating: 0x46, count: 200_000)
        let baseKey = subspace.pack(Tuple(["size_large"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        let size = try await database.withTransaction { transaction in
            try await splitter.getSize(for: baseKey, transaction: transaction)
        }
        #expect(size == 200_000)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("getSize returns nil for non-existent key")
    func getSizeNonExistent() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let baseKey = subspace.pack(Tuple(["nonexistent"]))

        let size = try await database.withTransaction { transaction in
            try await splitter.getSize(for: baseKey, transaction: transaction)
        }
        #expect(size == nil)
    }

    // MARK: - Edge Cases

    @Test("Empty value")
    func emptyValue() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let testData: [UInt8] = []
        let baseKey = subspace.pack(Tuple(["empty"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Load non-existent key returns nil")
    func loadNonExistentReturnsNil() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let baseKey = subspace.pack(Tuple(["does_not_exist"]))

        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == nil)
    }

    @Test("Overwrite small with large value")
    func overwriteSmallWithLarge() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .default)

        let baseKey = subspace.pack(Tuple(["overwrite"]))

        // Save small value first
        let smallData: [UInt8] = Array(repeating: 0x47, count: 1000)
        try await database.withTransaction { transaction in
            try splitter.save(smallData, for: baseKey, transaction: transaction)
        }

        // Overwrite with large value
        let largeData: [UInt8] = (0..<200_000).map { UInt8(truncatingIfNeeded: $0) }
        try await database.withTransaction { transaction in
            // Delete old first (important for clean overwrite)
            try await splitter.delete(for: baseKey, transaction: transaction)
            try splitter.save(largeData, for: baseKey, transaction: transaction)
        }

        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == largeData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Configuration Tests

    @Test("Disabled configuration stores directly regardless of size")
    func disabledConfigurationStoresDirectly() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let splitter = LargeValueSplitter(configuration: .disabled)

        // This would normally be split, but with disabled config it won't
        // Note: This test uses a size under FDB's 100KB limit
        let testData: [UInt8] = Array(repeating: 0x48, count: 95_000)
        let baseKey = subspace.pack(Tuple(["disabled"]))

        try await database.withTransaction { transaction in
            try splitter.save(testData, for: baseKey, transaction: transaction)
        }

        let isSplit = try await database.withTransaction { transaction in
            try await splitter.isSplit(for: baseKey, transaction: transaction)
        }
        #expect(isSplit == false)

        let loaded = try await database.withTransaction { transaction in
            try await splitter.load(for: baseKey, transaction: transaction)
        }
        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Error Cases

    @Test("Value too large throws error")
    func valueTooLargeThrows() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()

        // Custom config with very small max size
        let config = SplitConfiguration(maxValueSize: 1000, enabled: true)
        let splitter = LargeValueSplitter(configuration: config)

        // Data that exceeds max parts * max size
        // 254 parts * 1000 bytes = 254,000 bytes max
        let testData: [UInt8] = Array(repeating: 0x49, count: 300_000)
        let baseKey = subspace.pack(Tuple(["too_large"]))

        await #expect(throws: SplitError.self) {
            try await database.withTransaction { transaction in
                try splitter.save(testData, for: baseKey, transaction: transaction)
            }
        }
    }
}
