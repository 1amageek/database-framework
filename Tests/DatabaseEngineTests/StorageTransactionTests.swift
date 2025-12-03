// StorageTransactionTests.swift
// DatabaseEngine Tests - StorageTransaction integration tests (compression + splitting)

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine

@Suite("StorageTransaction Tests", .serialized)
struct StorageTransactionTests {

    // MARK: - Setup

    private func setupDatabase() async throws -> any DatabaseProtocol {
        try await FDBTestEnvironment.shared.ensureInitialized()
        return try FDBClient.openDatabase()
    }

    private func testSubspace() -> Subspace {
        Subspace(prefix: Tuple("test", "storage", UUID().uuidString).pack())
    }

    // MARK: - Basic Write/Read Tests

    @Test("Write and read small data")
    func writeAndReadSmallData() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["small"]))

        let testData: [UInt8] = Array("Hello, World!".utf8)

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Read
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Write and read medium data with compression")
    func writeAndReadMediumData() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["medium"]))

        // Compressible data (repeated pattern)
        let testData: [UInt8] = Array(repeating: 0x41, count: 10_000)

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Read
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Write and read large data with compression and splitting")
    func writeAndReadLargeData() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["large"]))

        // Large compressible data (200KB before compression)
        let testData: [UInt8] = Array(repeating: 0x42, count: 200_000)

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Read
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
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
        let key = subspace.pack(Tuple(["delete_small"]))

        let testData: [UInt8] = Array("Delete me".utf8)

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Delete
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try await storage.delete(for: key)
        }

        // Verify deleted
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == nil)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Delete large split value")
    func deleteLargeSplitValue() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["delete_large"]))

        // Large data that gets split
        let testData: [UInt8] = (0..<300_000).map { UInt8(truncatingIfNeeded: $0) }

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Delete
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try await storage.delete(for: key)
        }

        // Verify deleted
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == nil)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Exists Tests

    @Test("Exists returns true for existing key")
    func existsReturnsTrueForExisting() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["exists"]))

        let testData: [UInt8] = Array("I exist".utf8)

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Check exists
        let exists = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.exists(for: key)
        }

        #expect(exists == true)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Exists returns false for non-existent key")
    func existsReturnsFalseForNonExistent() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["does_not_exist"]))

        let exists = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.exists(for: key)
        }

        #expect(exists == false)
    }

    // MARK: - Size Tests

    @Test("Size returns correct value for small data")
    func sizeSmallData() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["size_small"]))

        let testData: [UInt8] = Array(repeating: 0x43, count: 5000)

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Get size (returns compressed size)
        let size = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.size(for: key)
        }

        // Size should exist and be less than original (compressed)
        #expect(size != nil)
        #expect(size! > 0)
        // Compressed + header should be much smaller than original
        #expect(size! < testData.count)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Size returns nil for non-existent key")
    func sizeNonExistent() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["size_nonexistent"]))

        let size = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.size(for: key)
        }

        #expect(size == nil)
    }

    // MARK: - Real-world Data Tests

    @Test("JSON data round-trip")
    func jsonDataRoundTrip() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["json"]))

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

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Read
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Verify as string
        if let loadedBytes = loaded {
            let loadedString = String(bytes: loadedBytes, encoding: .utf8)
            #expect(loadedString == json)
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Binary data with varied content")
    func binaryDataVariedContent() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["binary"]))

        // Create varied binary data
        var testData: [UInt8] = []
        for i in 0..<50_000 {
            testData.append(UInt8(truncatingIfNeeded: i * 7 + 13))
        }

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Read
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Edge Cases

    @Test("Empty data")
    func emptyData() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["empty"]))

        let testData: [UInt8] = []

        // Write
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(testData, for: key)
        }

        // Read
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == testData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Read non-existent key returns nil")
    func readNonExistentReturnsNil() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["nonexistent"]))

        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == nil)
    }

    @Test("Overwrite existing value")
    func overwriteExistingValue() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let key = subspace.pack(Tuple(["overwrite"]))

        let firstData: [UInt8] = Array("First value".utf8)
        let secondData: [UInt8] = Array("Second value - this is longer".utf8)

        // Write first
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try storage.write(firstData, for: key)
        }

        // Overwrite with second
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            try await storage.delete(for: key)
            try storage.write(secondData, for: key)
        }

        // Read - should get second value
        let loaded = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: key)
        }

        #expect(loaded == secondData)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Underlying Transaction Access

    @Test("Underlying transaction access for index operations")
    func underlyingTransactionAccess() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()
        let dataKey = subspace.pack(Tuple(["data"]))
        let indexKey = subspace.pack(Tuple(["index", "value1"]))

        let testData: [UInt8] = Array("Record data".utf8)

        // Write record via storage, index via underlying
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)

            // Record data - uses compression/splitting
            try storage.write(testData, for: dataKey)

            // Index entry - uses underlying directly (empty value)
            storage.underlying.setValue([], for: indexKey)
        }

        // Verify both exist
        let loadedData = try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            return try await storage.read(for: dataKey)
        }
        #expect(loadedData == testData)

        let indexExists = try await database.withTransaction { transaction in
            try await transaction.getValue(for: indexKey) != nil
        }
        #expect(indexExists == true)

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Multiple Keys Test

    @Test("Multiple keys in same transaction")
    func multipleKeysInSameTransaction() async throws {
        let database = try await setupDatabase()
        let subspace = testSubspace()

        let keys = (0..<5).map { subspace.pack(Tuple(["key\($0)"])) }
        let values = (0..<5).map { Array(repeating: UInt8($0 + 0x41), count: 1000 * ($0 + 1)) }

        // Write all
        try await database.withTransaction { transaction in
            let storage = StorageTransaction(transaction)
            for (key, value) in zip(keys, values) {
                try storage.write(value, for: key)
            }
        }

        // Read all
        for (key, expectedValue) in zip(keys, values) {
            let loaded = try await database.withTransaction { transaction in
                let storage = StorageTransaction(transaction)
                return try await storage.read(for: key)
            }
            #expect(loaded == expectedValue)
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
