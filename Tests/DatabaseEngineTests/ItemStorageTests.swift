// ItemStorageTests.swift
// DatabaseEngine Tests - ItemStorage integration tests (compression + splitting)

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine

@Suite("ItemStorage Tests", .serialized)
struct ItemStorageTests {

    // MARK: - Setup

    private func setupDatabase() async throws -> any DatabaseProtocol {
        try await FDBTestEnvironment.shared.ensureInitialized()
        return try FDBClient.openDatabase()
    }

    /// Create isolated test subspaces - returns both items and blobs subspaces
    /// CRITICAL: blobsSubspace must be the same instance for write and read!
    private func createTestSubspaces() -> (items: Subspace, blobs: Subspace) {
        let testId = UUID().uuidString
        let items = Subspace(prefix: Tuple("test", "storage", testId).pack())
        let blobs = Subspace(prefix: Tuple("test", "blobs", testId).pack())
        return (items, blobs)
    }

    // MARK: - Basic Write/Read Tests

    @Test("Write and read small data")
    func writeAndReadSmallData() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["small"]))

        let testData: [UInt8] = Array("Hello, World!".utf8)

        // Write and read in same transaction to ensure same blobsSubspace
        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
            let (blobBegin, blobEnd) = blobsSubspace.range()
            transaction.clearRange(beginKey: blobBegin, endKey: blobEnd)
        }
    }

    @Test("Write and read medium data with compression")
    func writeAndReadMediumData() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["medium"]))

        // Compressible data (repeated pattern)
        let testData: [UInt8] = Array(repeating: 0x41, count: 10_000)

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
            let (blobBegin, blobEnd) = blobsSubspace.range()
            transaction.clearRange(beginKey: blobBegin, endKey: blobEnd)
        }
    }

    @Test("Write and read large data with compression and splitting")
    func writeAndReadLargeData() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["large"]))

        // Large random data (200KB) - won't compress well, will trigger splitting
        var testData: [UInt8] = []
        for _ in 0..<200_000 {
            testData.append(UInt8.random(in: 0...255))
        }

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
            let (blobBegin, blobEnd) = blobsSubspace.range()
            transaction.clearRange(beginKey: blobBegin, endKey: blobEnd)
        }
    }

    // MARK: - Delete Tests

    @Test("Delete small value")
    func deleteSmallValue() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["delete_small"]))

        let testData: [UInt8] = Array("Delete me".utf8)

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.delete(for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == nil)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Delete large split value")
    func deleteLargeSplitValue() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["delete_large"]))

        // Large random data that will be split
        var testData: [UInt8] = []
        for _ in 0..<300_000 {
            testData.append(UInt8.random(in: 0...255))
        }

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        // Verify blobs were created
        let blobCountBefore = try await database.withTransaction { transaction in
            let (begin, end) = blobsSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }
        #expect(blobCountBefore > 0, "Blobs should be created for large data")

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.delete(for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == nil)

        // Verify blobs were cleaned up
        let blobCountAfter = try await database.withTransaction { transaction in
            let (begin, end) = blobsSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }
        #expect(blobCountAfter == 0, "Blobs should be cleaned up after delete")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Exists Tests

    @Test("Exists returns true for existing key")
    func existsReturnsTrueForExisting() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["exists"]))

        let testData: [UInt8] = Array("I exist".utf8)

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let exists = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.exists(for: key)
        }

        #expect(exists == true)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Exists returns false for non-existent key")
    func existsReturnsFalseForNonExistent() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["does_not_exist"]))

        let exists = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.exists(for: key)
        }

        #expect(exists == false)
    }

    // MARK: - Real-world Data Tests

    @Test("JSON data round-trip")
    func jsonDataRoundTrip() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["json"]))

        let json = """
        {
            "users": [
                {"id": 1, "name": "Alice", "email": "alice@example.com", "roles": ["admin", "user"]},
                {"id": 2, "name": "Bob", "email": "bob@example.com", "roles": ["user"]},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com", "roles": ["guest"]}
            ],
            "metadata": {
                "version": "1.0.0",
                "generated": "2024-01-01T00:00:00Z"
            }
        }
        """
        let testData: [UInt8] = Array(json.utf8)

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        if let loadedBytes = loaded {
            let loadedString = String(bytes: loadedBytes, encoding: .utf8)
            #expect(loadedString == json)
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Binary data with varied content")
    func binaryDataVariedContent() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["binary"]))

        // Create varied binary data
        var testData: [UInt8] = []
        for i in 0..<50_000 {
            testData.append(UInt8(truncatingIfNeeded: i * 7 + 13))
        }

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Edge Cases

    @Test("Empty data")
    func emptyData() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["empty"]))

        let testData: [UInt8] = []

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(testData, for: key)
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Read non-existent key returns nil")
    func readNonExistentReturnsNil() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["nonexistent"]))

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == nil)
    }

    @Test("Overwrite existing value cleans up old blobs")
    func overwriteExistingValue() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let key = itemsSubspace.pack(Tuple(["overwrite"]))

        // First write: large random data that will be split
        var firstData: [UInt8] = []
        for _ in 0..<200_000 {
            firstData.append(UInt8.random(in: 0...255))
        }

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(firstData, for: key)
        }

        // Verify blobs were created
        let blobCountAfterFirst = try await database.withTransaction { transaction in
            let (begin, end) = blobsSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }
        #expect(blobCountAfterFirst > 0, "First write should create blobs")

        // Second write: small data (inline) - should clean up old blobs
        let secondData: [UInt8] = Array("Small replacement".utf8)

        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(secondData, for: key)  // async write cleans up old blobs
        }

        let loaded = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: key)
        }

        #expect(loaded == secondData)

        // Verify old blobs were cleaned up
        let blobCountAfterSecond = try await database.withTransaction { transaction in
            let (begin, end) = blobsSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }
        #expect(blobCountAfterSecond == 0, "Old blobs should be cleaned up on overwrite")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Underlying Transaction Access

    @Test("Underlying transaction access for index operations")
    func underlyingTransactionAccess() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()
        let dataKey = itemsSubspace.pack(Tuple(["data"]))
        let indexKey = itemsSubspace.pack(Tuple(["index", "value1"]))

        let testData: [UInt8] = Array("Record data".utf8)

        // Write record via storage, index via underlying
        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)

            // Record data - uses compression/splitting
            try await storage.write(testData, for: dataKey)

            // Index entry - uses underlying directly (empty value)
            storage.underlying.setValue([], for: indexKey)
        }

        // Verify both exist
        let loadedData = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            return try await storage.read(for: dataKey)
        }
        #expect(loadedData == testData)

        let indexExists = try await database.withTransaction { transaction in
            try await transaction.getValue(for: indexKey) != nil
        }
        #expect(indexExists == true)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Multiple Keys Test

    @Test("Multiple keys in same transaction")
    func multipleKeysInSameTransaction() async throws {
        let database = try await setupDatabase()
        let (itemsSubspace, blobsSubspace) = createTestSubspaces()

        let keys = (0..<5).map { itemsSubspace.pack(Tuple(["key\($0)"])) }
        let values = (0..<5).map { Array(repeating: UInt8($0 + 0x41), count: 1000 * ($0 + 1)) }

        // Write all
        try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            for (key, value) in zip(keys, values) {
                try await storage.write(value, for: key)
            }
        }

        // Read all
        for (key, expectedValue) in zip(keys, values) {
            let loaded = try await database.withTransaction { transaction in
                let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
                return try await storage.read(for: key)
            }
            #expect(loaded == expectedValue)
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = itemsSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
