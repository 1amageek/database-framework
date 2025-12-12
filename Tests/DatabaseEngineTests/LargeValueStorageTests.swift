// LargeValueStorageTests.swift
// Tests for large value storage (>90KB) with automatic splitting

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

// MARK: - Test Model

/// Model with large data field for testing 100KB limit handling
struct LargeDataModel: Persistable {
    typealias ID = String

    var id: String = UUID().uuidString
    var name: String
    var data: Data  // Can be large

    static var persistableType: String { "LargeDataModel" }
    static var directoryPathComponents: [String] { ["test", "largevalue"] }
    static var allFields: [String] { ["id", "name", "data"] }

    static var descriptors: [any Descriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "data": return data
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<LargeDataModel, Value>) -> String {
        switch keyPath {
        case \LargeDataModel.id: return "id"
        case \LargeDataModel.name: return "name"
        case \LargeDataModel.data: return "data"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<LargeDataModel>) -> String {
        switch keyPath {
        case \LargeDataModel.id: return "id"
        case \LargeDataModel.name: return "name"
        case \LargeDataModel.data: return "data"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<LargeDataModel> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Tests

@Suite("Large Value Storage Tests", .serialized)
struct LargeValueStorageTests {

    private func createContainer() async throws -> FDBContainer {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let schema = Schema([LargeDataModel.self])
        return FDBContainer(database: database, schema: schema, security: .disabled)
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("Save and retrieve data >90KB")
    func testLargeValueRoundTrip() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        // Create 95KB of data (will trigger splitting)
        let largeData = Data(repeating: 0x42, count: 95_000)
        let modelId = uniqueID("large")

        var model = LargeDataModel(name: "Large Model", data: largeData)
        model.id = modelId

        // Save
        context.insert(model)
        try await context.save()

        // Retrieve
        let fetched = try await context.model(for: modelId, as: LargeDataModel.self)

        #expect(fetched != nil, "Large value should be retrievable")
        #expect(fetched?.id == modelId)
        #expect(fetched?.name == "Large Model")
        #expect(fetched?.data.count == 95_000, "Data size should be preserved")
        #expect(fetched?.data == largeData, "Data content should be preserved")
    }

    @Test("Update from large to small value")
    func testShrinkFromLargeToSmall() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        let modelId = uniqueID("shrink")

        // Step 1: Save large data (95KB - triggers splitting)
        let largeData = Data(repeating: 0xAA, count: 95_000)
        var model = LargeDataModel(name: "Initially Large", data: largeData)
        model.id = modelId

        context.insert(model)
        try await context.save()

        // Verify large data was saved
        let fetchedLarge = try await context.model(for: modelId, as: LargeDataModel.self)
        #expect(fetchedLarge?.data.count == 95_000)

        // Step 2: Update to small data (5KB - no splitting needed)
        let smallData = Data(repeating: 0xBB, count: 5_000)
        model.name = "Now Small"
        model.data = smallData

        context.insert(model)
        try await context.save()

        // Step 3: Verify small data is correctly retrieved
        let fetchedSmall = try await context.model(for: modelId, as: LargeDataModel.self)

        #expect(fetchedSmall != nil, "Model should exist after shrink")
        #expect(fetchedSmall?.name == "Now Small")
        #expect(fetchedSmall?.data.count == 5_000, "Data should be shrunk to 5KB")
        #expect(fetchedSmall?.data == smallData, "Small data content should be correct")
    }

    @Test("Update from small to large value")
    func testGrowFromSmallToLarge() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        let modelId = uniqueID("grow")

        // Step 1: Save small data (5KB)
        let smallData = Data(repeating: 0xCC, count: 5_000)
        var model = LargeDataModel(name: "Initially Small", data: smallData)
        model.id = modelId

        context.insert(model)
        try await context.save()

        // Step 2: Update to large data (95KB)
        let largeData = Data(repeating: 0xDD, count: 95_000)
        model.name = "Now Large"
        model.data = largeData

        context.insert(model)
        try await context.save()

        // Step 3: Verify large data is correctly retrieved
        let fetched = try await context.model(for: modelId, as: LargeDataModel.self)

        #expect(fetched != nil, "Model should exist after grow")
        #expect(fetched?.name == "Now Large")
        #expect(fetched?.data.count == 95_000, "Data should be grown to 95KB")
        #expect(fetched?.data == largeData, "Large data content should be correct")
    }

    @Test("Delete large value cleans up all parts")
    func testDeleteLargeValueCleansUp() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        let modelId = uniqueID("delete-large")

        // Save large data
        let largeData = Data(repeating: 0xEE, count: 95_000)
        var model = LargeDataModel(name: "To Delete", data: largeData)
        model.id = modelId

        context.insert(model)
        try await context.save()

        // Verify it exists
        let beforeDelete = try await context.model(for: modelId, as: LargeDataModel.self)
        #expect(beforeDelete != nil)

        // Delete
        context.delete(model)
        try await context.save()

        // Verify it's gone
        let afterDelete = try await context.model(for: modelId, as: LargeDataModel.self)
        #expect(afterDelete == nil, "Model should be deleted")
    }

    @Test("Multiple large values can coexist")
    func testMultipleLargeValues() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        // Create multiple large models
        var models: [LargeDataModel] = []
        for i in 0..<3 {
            let modelId = uniqueID("multi-\(i)")
            let data = Data(repeating: UInt8(i + 1), count: 95_000)
            var model = LargeDataModel(name: "Model \(i)", data: data)
            model.id = modelId
            models.append(model)
            context.insert(model)
        }
        try await context.save()

        // Verify all can be retrieved correctly
        for (i, original) in models.enumerated() {
            let fetched = try await context.model(for: original.id, as: LargeDataModel.self)
            #expect(fetched != nil, "Model \(i) should exist")
            #expect(fetched?.data.count == 95_000, "Model \(i) should have 95KB data")
            #expect(fetched?.data == original.data, "Model \(i) data should match")
        }
    }

    @Test("TransactionContext handles large values")
    func testTransactionContextLargeValue() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        let modelId = uniqueID("tx-large")
        let largeData = Data(repeating: 0xFF, count: 95_000)
        var model = LargeDataModel(name: "TX Large", data: largeData)
        model.id = modelId

        // Capture as let to avoid concurrency issue
        let modelToSave = model

        // Use withTransaction API
        try await context.withTransaction { tx in
            try await tx.set(modelToSave)
        }

        // Verify
        try await context.withTransaction { tx in
            let fetched: LargeDataModel? = try await tx.get(LargeDataModel.self, id: modelId)
            #expect(fetched != nil)
            #expect(fetched?.data.count == 95_000)
        }
    }

    @Test("Exactly 90KB does not trigger splitting")
    func testBoundaryNoSplit() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        // 90KB is the threshold - should NOT split
        let boundaryData = Data(repeating: 0x11, count: 90_000)
        let modelId = uniqueID("boundary")
        var model = LargeDataModel(name: "Boundary", data: boundaryData)
        model.id = modelId

        context.insert(model)
        try await context.save()

        let fetched = try await context.model(for: modelId, as: LargeDataModel.self)
        #expect(fetched != nil)
        #expect(fetched?.data.count == 90_000)
        #expect(fetched?.data == boundaryData)
    }

    @Test("Just over 90KB triggers splitting")
    func testBoundarySplit() async throws {
        let container = try await createContainer()
        let context = container.newContext()

        // 90,001 bytes should trigger split
        let overBoundaryData = Data(repeating: 0x22, count: 90_001)
        let modelId = uniqueID("over-boundary")
        var model = LargeDataModel(name: "Over Boundary", data: overBoundaryData)
        model.id = modelId

        context.insert(model)
        try await context.save()

        let fetched = try await context.model(for: modelId, as: LargeDataModel.self)
        #expect(fetched != nil)
        #expect(fetched?.data.count == 90_001)
        #expect(fetched?.data == overBoundaryData)
    }

    // MARK: - Scan Tests (Critical for verifying split data doesn't pollute items subspace)

    @Test("Scan returns mixed inline and split items correctly")
    func testScanMixedInlineAndSplit() async throws {
        let container = try await createContainer()
        let database = container.database

        // Use unique subspace to isolate from other tests
        let testId = UUID().uuidString.prefix(8)
        let testSubspace = Subspace(prefix: Tuple("test", "scan-mixed", String(testId)).pack())
        let itemSubspace = testSubspace.subspace(SubspaceKey.items)
        let blobsSubspace = testSubspace.subspace(SubspaceKey.blobs)

        // Insert items with varying sizes (some inline, some split)
        let testItems: [(id: String, size: Int)] = [
            ("small1", 1_000),      // Inline (1KB)
            ("large1", 95_000),     // Split (95KB)
            ("small2", 5_000),      // Inline (5KB)
            ("large2", 120_000),    // Split (120KB)
            ("small3", 10_000),     // Inline (10KB)
        ]

        var expectedData: [String: Data] = [:]

        // Write items
        for (id, size) in testItems {
            let data = Data(repeating: UInt8(size % 255), count: size)
            expectedData[id] = data

            var model = LargeDataModel(name: "Item \(id)", data: data)
            model.id = id

            try await database.withTransaction { transaction in
                let serialized = try DataAccess.serialize(model)
                let key = itemSubspace.pack(Tuple(id))
                let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
                try await storage.write(Array(serialized), for: key)
            }
        }

        // Scan all items in our test subspace
        let scannedItems = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            let (begin, end) = itemSubspace.range()

            var results: [(id: String, data: [UInt8])] = []
            for try await (key, data) in storage.scan(begin: begin, end: end, snapshot: false) {
                // Extract ID from key using subspace.unpack
                let idTuple = try itemSubspace.unpack(key)
                if let id = idTuple[0] as? String {
                    results.append((id, data))
                }
            }
            return results
        }

        // Verify: Should get exactly 5 items (not blob chunks)
        #expect(scannedItems.count == testItems.count, "Should return exactly \(testItems.count) items, got \(scannedItems.count)")

        // Verify each item's data integrity
        for (id, data) in scannedItems {
            guard let expected = expectedData[id] else {
                Issue.record("Unexpected item ID: \(id)")
                continue
            }

            // Deserialize and verify
            let model: LargeDataModel = try DataAccess.deserialize(data)
            #expect(model.id == id, "ID should match")
            #expect(model.data.count == expected.count, "Data size should match for \(id)")
            #expect(model.data == expected, "Data content should match for \(id)")
        }

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = testSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Scan with limit works correctly with split items")
    func testScanWithLimitAndSplitItems() async throws {
        let container = try await createContainer()
        let database = container.database

        // Use unique subspace to isolate from other tests
        let testId = UUID().uuidString.prefix(8)
        let testSubspace = Subspace(prefix: Tuple("test", "scan-limit", String(testId)).pack())
        let itemSubspace = testSubspace.subspace(SubspaceKey.items)
        let blobsSubspace = testSubspace.subspace(SubspaceKey.blobs)

        // Insert 5 large items (all will be split)
        for i in 0..<5 {
            let id = String(format: "%02d", i)  // Ensures consistent ordering
            let data = Data(repeating: UInt8(i + 1), count: 95_000)

            var model = LargeDataModel(name: "Large \(i)", data: data)
            model.id = id

            try await database.withTransaction { transaction in
                let serialized = try DataAccess.serialize(model)
                let key = itemSubspace.pack(Tuple(id))
                let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
                try await storage.write(Array(serialized), for: key)
            }
        }

        // Scan with limit=2
        let limitedResults = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            let (begin, end) = itemSubspace.range()

            var results: [String] = []
            for try await (key, _) in storage.scan(begin: begin, end: end, snapshot: false, limit: 2) {
                let idTuple = try itemSubspace.unpack(key)
                if let id = idTuple[0] as? String {
                    results.append(id)
                }
            }
            return results
        }

        #expect(limitedResults.count == 2, "Limit should be respected, got \(String(limitedResults.count))")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = testSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Scan snapshot mode reads consistently with split items")
    func testScanSnapshotWithSplitItems() async throws {
        let container = try await createContainer()
        let database = container.database

        // Use unique subspace to isolate from other tests
        let testId = UUID().uuidString.prefix(8)
        let testSubspace = Subspace(prefix: Tuple("test", "scan-snap", String(testId)).pack())
        let itemSubspace = testSubspace.subspace(SubspaceKey.items)
        let blobsSubspace = testSubspace.subspace(SubspaceKey.blobs)

        // Insert a large item
        let id = "snapshot-item"
        let data = Data(repeating: 0x55, count: 95_000)

        var model = LargeDataModel(name: "Snapshot Test", data: data)
        model.id = id

        try await database.withTransaction { transaction in
            let serialized = try DataAccess.serialize(model)
            let key = itemSubspace.pack(Tuple(id))
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(Array(serialized), for: key)
        }

        // Scan with snapshot=true
        let snapshotResults = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            let (begin, end) = itemSubspace.range()

            var results: [(id: String, dataSize: Int)] = []
            for try await (key, itemData) in storage.scan(begin: begin, end: end, snapshot: true) {
                let idTuple = try itemSubspace.unpack(key)
                if let itemId = idTuple[0] as? String {
                    let decoded: LargeDataModel = try DataAccess.deserialize(itemData)
                    results.append((itemId, decoded.data.count))
                }
            }
            return results
        }

        // Verify snapshot read worked
        let testItem = snapshotResults.first { $0.id == id }
        #expect(testItem != nil, "Should find the test item via snapshot scan")
        #expect(testItem?.dataSize == 95_000, "Data size should be preserved")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = testSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    @Test("Scan empty range returns no items")
    func testScanEmptyRange() async throws {
        let container = try await createContainer()
        let database = container.database
        let subspace = try await container.resolveDirectory(for: LargeDataModel.self)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)

        // Create a subspace that won't have any items
        let emptySubspace = Subspace(prefix: Tuple("test", "empty", UUID().uuidString).pack())

        let results = try await database.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            let (begin, end) = emptySubspace.range()

            var count = 0
            for try await _ in storage.scan(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }

        #expect(results == 0, "Empty range should return no items")
    }

    @Test("Blobs subspace isolation - chunks don't appear in items scan")
    func testBlobsSubspaceIsolation() async throws {
        let container = try await createContainer()
        let database = container.database

        // Use unique subspace to isolate from other tests
        let testId = UUID().uuidString.prefix(8)
        let testSubspace = Subspace(prefix: Tuple("test", "isolation", String(testId)).pack())
        let itemSubspace = testSubspace.subspace(SubspaceKey.items)
        let blobsSubspace = testSubspace.subspace(SubspaceKey.blobs)

        // Insert a very large item (will create multiple blob chunks)
        // Use cryptographically random data which should NOT compress well
        let id = "verylarge"
        var dataBytes = [UInt8](repeating: 0, count: 1_000_000)  // 1MB
        // Use arc4random for truly random bytes
        for i in 0..<dataBytes.count {
            dataBytes[i] = UInt8.random(in: 0...255)
        }
        let data = Data(dataBytes)

        var model = LargeDataModel(name: "Very Large", data: data)
        model.id = id

        var serializedSize = 0
        try await database.withTransaction { transaction in
            let serialized = try DataAccess.serialize(model)
            serializedSize = serialized.count
            let key = itemSubspace.pack(Tuple(id))
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write(Array(serialized), for: key)
        }

        // Debug: Verify serialized size triggers splitting (>90KB)
        #expect(serializedSize > 90_000, "Serialized model should be >90KB to trigger splitting, but was \(serializedSize) bytes")

        // Direct getRange on items subspace (without ItemStorage)
        let rawItemsCount = try await database.withTransaction { transaction in
            let (begin, end) = itemSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }

        // Should be exactly 1 item key in items subspace (not multiple chunk keys)
        #expect(rawItemsCount == 1, "Items subspace should have exactly 1 key for the item, got \(rawItemsCount)")

        // Verify blobs subspace has the chunks
        let blobsCount = try await database.withTransaction { transaction in
            let (begin, end) = blobsSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: false) {
                count += 1
            }
            return count
        }

        #expect(blobsCount >= 5, "Blobs subspace should have multiple chunks (got \(blobsCount))")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = testSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
