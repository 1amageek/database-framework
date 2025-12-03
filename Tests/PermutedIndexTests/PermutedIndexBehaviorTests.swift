// PermutedIndexBehaviorTests.swift
// Integration tests for PermutedIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import Permuted
import TestSupport
@testable import DatabaseEngine
@testable import PermutedIndex

// MARK: - Test Model

struct TestLocation: Persistable {
    typealias ID = String

    var id: String
    var country: String
    var city: String
    var name: String

    init(id: String = UUID().uuidString, country: String, city: String, name: String) {
        self.id = id
        self.country = country
        self.city = city
        self.name = name
    }

    static var persistableType: String { "TestLocation" }
    static var allFields: [String] { ["id", "country", "city", "name"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "country": return country
        case "city": return city
        case "name": return name
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestLocation, Value>) -> String {
        switch keyPath {
        case \TestLocation.id: return "id"
        case \TestLocation.country: return "country"
        case \TestLocation.city: return "city"
        case \TestLocation.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestLocation>) -> String {
        switch keyPath {
        case \TestLocation.id: return "id"
        case \TestLocation.country: return "country"
        case \TestLocation.city: return "city"
        case \TestLocation.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestLocation> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: PermutedIndexMaintainer<TestLocation>
    let kind: PermutedIndexKind<TestLocation>

    /// Creates a test context with a permutation that reorders (country, city, name) to (city, country, name)
    init(permutation: Permutation? = nil, indexName: String = "TestLocation_compound") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "permuted", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Default permutation: [1, 0, 2] - (city, country, name)
        let perm = permutation ?? (try! Permutation(indices: [1, 0, 2]))
        self.kind = PermutedIndexKind<TestLocation>(
            fields: [\TestLocation.country, \TestLocation.city, \TestLocation.name],
            permutation: perm
        )

        // Expression: country + city + name (compound fields)
        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "country"),
                FieldKeyExpression(fieldName: "city"),
                FieldKeyExpression(fieldName: "name")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["TestLocation"])
        )

        self.maintainer = PermutedIndexMaintainer<TestLocation>(
            index: index,
            permutation: perm,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func scanByPrefix(prefixValues: [any TupleElement]) async throws -> [[any TupleElement]] {
        try await database.withTransaction { transaction in
            try await maintainer.scanByPrefix(
                prefixValues: prefixValues,
                transaction: transaction
            )
        }
    }

    func scanByExactMatch(values: [any TupleElement]) async throws -> [[any TupleElement]] {
        try await database.withTransaction { transaction in
            try await maintainer.scanByExactMatch(
                values: values,
                transaction: transaction
            )
        }
    }

    func scanAll() async throws -> [(permutedFields: [any TupleElement], primaryKey: [any TupleElement])] {
        try await database.withTransaction { transaction in
            try await maintainer.scanAll(transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("PermutedIndex Behavior Tests", .tags(.fdb), .serialized)
struct PermutedIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert creates permuted key entry")
    func testInsertCreatesPermutedKeyEntry() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let location = TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 index entry after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts create multiple entries")
    func testMultipleInserts() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let locations = [
            TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A"),
            TestLocation(id: "loc2", country: "Japan", city: "Osaka", name: "Station B"),
            TestLocation(id: "loc3", country: "USA", city: "New York", name: "Station C")
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 index entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes permuted key entry")
    func testDeleteRemovesEntry() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let location = TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: location,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update changes permuted key")
    func testUpdateChangesKey() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let location = TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Update city
        let updatedLocation = TestLocation(id: "loc1", country: "Japan", city: "Osaka", name: "Station A")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: location,
                newItem: updatedLocation,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should still have 1 entry after update")

        // Search for old city should find nothing
        let tokyoResults = try await ctx.scanByPrefix(prefixValues: ["Tokyo"])
        #expect(tokyoResults.isEmpty, "Should not find Tokyo after update")

        // Search for new city should find the entry
        let osakaResults = try await ctx.scanByPrefix(prefixValues: ["Osaka"])
        #expect(osakaResults.count == 1, "Should find Osaka after update")

        try await ctx.cleanup()
    }

    // MARK: - Prefix Search Tests

    @Test("scanByPrefix finds entries by permuted prefix")
    func testScanByPrefixFindsEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Permutation is [1, 0, 2]: (city, country, name)
        let locations = [
            TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A"),
            TestLocation(id: "loc2", country: "Japan", city: "Tokyo", name: "Station B"),
            TestLocation(id: "loc3", country: "USA", city: "Tokyo", name: "Station C"),
            TestLocation(id: "loc4", country: "Japan", city: "Osaka", name: "Station D")
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Search by city prefix (first field in permuted order)
        let tokyoResults = try await ctx.scanByPrefix(prefixValues: ["Tokyo"])
        #expect(tokyoResults.count == 3, "Should find 3 entries with city=Tokyo")

        // Search by city + country prefix
        let tokyoJapanResults = try await ctx.scanByPrefix(prefixValues: ["Tokyo", "Japan"])
        #expect(tokyoJapanResults.count == 2, "Should find 2 entries with city=Tokyo, country=Japan")

        try await ctx.cleanup()
    }

    @Test("scanByPrefix with empty prefix returns all")
    func testScanByPrefixEmptyReturnsAll() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let locations = [
            TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "A"),
            TestLocation(id: "loc2", country: "USA", city: "New York", name: "B")
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        let results = try await ctx.scanByPrefix(prefixValues: [])
        #expect(results.count == 2, "Should find all 2 entries with empty prefix")

        try await ctx.cleanup()
    }

    // MARK: - Exact Match Tests

    @Test("scanByExactMatch finds entries with exact values")
    func testScanByExactMatchFindsEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Permutation is [1, 0, 2]: (city, country, name)
        let locations = [
            TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A"),
            TestLocation(id: "loc2", country: "Japan", city: "Tokyo", name: "Station B"),
            TestLocation(id: "loc3", country: "Japan", city: "Tokyo", name: "Station A")  // Same permuted key, different ID
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Exact match in permuted order: (city, country, name) = (Tokyo, Japan, Station A)
        let results = try await ctx.scanByExactMatch(values: ["Tokyo", "Japan", "Station A"])
        #expect(results.count == 2, "Should find 2 entries with exact match")

        try await ctx.cleanup()
    }

    @Test("scanByExactMatch throws for wrong field count")
    func testScanByExactMatchThrowsForWrongFieldCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        await #expect(throws: PermutedIndexError.self) {
            _ = try await ctx.scanByExactMatch(values: ["Tokyo", "Japan"])  // Only 2 values, need 3
        }

        try await ctx.cleanup()
    }

    // MARK: - Scan All Tests

    @Test("scanAll returns all entries with permuted fields")
    func testScanAllReturnsAllEntries() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let locations = [
            TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "A"),
            TestLocation(id: "loc2", country: "USA", city: "NY", name: "B")
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        let results = try await ctx.scanAll()
        #expect(results.count == 2, "Should return 2 entries")

        // Check permuted field order: (city, country, name)
        let firstEntry = results.first { entry in
            if let firstField = entry.permutedFields.first as? String {
                return firstField == "Tokyo"
            }
            return false
        }
        #expect(firstEntry != nil, "Should find entry with city=Tokyo")

        if let entry = firstEntry {
            // Permuted order: [city, country, name]
            #expect(entry.permutedFields.count == 3, "Should have 3 permuted fields")
            if entry.permutedFields.count >= 3 {
                #expect((entry.permutedFields[0] as? String) == "Tokyo", "First field should be city (Tokyo)")
                #expect((entry.permutedFields[1] as? String) == "Japan", "Second field should be country (Japan)")
                #expect((entry.permutedFields[2] as? String) == "A", "Third field should be name (A)")
            }
        }

        try await ctx.cleanup()
    }

    // MARK: - Inverse Permutation Tests

    @Test("toOriginalOrder converts permuted values back")
    func testToOriginalOrderConvertsBack() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Permutation is [1, 0, 2]: original (country, city, name) -> permuted (city, country, name)
        let permutedValues: [any TupleElement] = ["Tokyo", "Japan", "Station A"]

        let originalValues = try ctx.maintainer.toOriginalOrder(permutedValues)

        // Original order: (country, city, name) = (Japan, Tokyo, Station A)
        #expect(originalValues.count == 3)
        #expect((originalValues[0] as? String) == "Japan", "First should be country (Japan)")
        #expect((originalValues[1] as? String) == "Tokyo", "Second should be city (Tokyo)")
        #expect((originalValues[2] as? String) == "Station A", "Third should be name (Station A)")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds permuted entry")
    func testScanItemAddsPermutedEntry() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let locations = [
            TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "A"),
            TestLocation(id: "loc2", country: "USA", city: "NY", name: "B")
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.scanItem(
                    location,
                    id: Tuple(location.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 entries after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Different Permutation Tests

    @Test("Different permutation orders fields differently")
    func testDifferentPermutationOrdersFieldsDifferently() async throws {
        try await FDBTestSetup.shared.initialize()
        // Use permutation [2, 0, 1]: (name, country, city)
        let ctx = try TestContext(permutation: try! Permutation(indices: [2, 0, 1]))

        let location = TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Search by name prefix (first field in this permutation)
        let results = try await ctx.scanByPrefix(prefixValues: ["Station A"])
        #expect(results.count == 1, "Should find entry by name prefix")

        try await ctx.cleanup()
    }

    // MARK: - Identity Permutation Tests

    @Test("Identity permutation maintains original order")
    func testIdentityPermutationMaintainsOrder() async throws {
        try await FDBTestSetup.shared.initialize()
        // Use identity permutation [0, 1, 2]: (country, city, name)
        let ctx = try TestContext(permutation: Permutation.identity(size: 3))

        let location = TestLocation(id: "loc1", country: "Japan", city: "Tokyo", name: "Station A")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Search by country prefix (first field in identity order)
        let results = try await ctx.scanByPrefix(prefixValues: ["Japan"])
        #expect(results.count == 1, "Should find entry by country prefix")

        // Search by city prefix (second field) should not work as sole prefix
        let cityResults = try await ctx.scanByPrefix(prefixValues: ["Tokyo"])
        #expect(cityResults.isEmpty, "Should not find entry by city-only prefix in identity permutation")

        try await ctx.cleanup()
    }
}
