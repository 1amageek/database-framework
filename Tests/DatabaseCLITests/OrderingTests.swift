// OrderingTests.swift
// DatabaseCLI - Tests for ordering functionality

import Testing
import Foundation
import FoundationDB
@testable import DatabaseCLI
import QueryAST
import TestSupport

// MARK: - DynamicSortDescriptor Tests

@Suite("DynamicSortDescriptor Tests")
struct DynamicSortDescriptorTests {

    @Test("Default direction is ascending")
    func testDefaultDirection() {
        let desc = DynamicSortDescriptor(field: "name")
        #expect(desc.direction == .ascending)
        #expect(desc.nulls == nil)
    }

    @Test("Descending direction")
    func testDescendingDirection() {
        let desc = DynamicSortDescriptor(field: "age", direction: .descending)
        #expect(desc.direction == .descending)
        #expect(desc.field == "age")
    }

    @Test("With nulls ordering")
    func testNullsOrdering() {
        let desc = DynamicSortDescriptor(
            field: "score",
            direction: .descending,
            nulls: .last
        )
        #expect(desc.nulls == .last)
    }

    @Test("Static ascending factory")
    func testAscendingFactory() {
        let desc = DynamicSortDescriptor.ascending("name", nulls: .first)
        #expect(desc.field == "name")
        #expect(desc.direction == .ascending)
        #expect(desc.nulls == .first)
    }

    @Test("Static descending factory")
    func testDescendingFactory() {
        let desc = DynamicSortDescriptor.descending("age")
        #expect(desc.field == "age")
        #expect(desc.direction == .descending)
        #expect(desc.nulls == nil)
    }

    @Test("Equatable conformance")
    func testEquatable() {
        let desc1 = DynamicSortDescriptor(field: "name", direction: .ascending)
        let desc2 = DynamicSortDescriptor(field: "name", direction: .ascending)
        let desc3 = DynamicSortDescriptor(field: "name", direction: .descending)

        #expect(desc1 == desc2)
        #expect(desc1 != desc3)
    }

    @Test("Hashable conformance")
    func testHashable() {
        let desc1 = DynamicSortDescriptor(field: "name", direction: .ascending)
        let desc2 = DynamicSortDescriptor(field: "name", direction: .ascending)

        var set = Set<DynamicSortDescriptor>()
        set.insert(desc1)
        set.insert(desc2)

        #expect(set.count == 1)
    }
}

// MARK: - Compare Values Tests

@Suite("Compare Values Tests", .serialized)
struct CompareValuesTests {

    private let storage: SchemaStorage

    init() async throws {
        // Initialize FDB and create storage
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        storage = SchemaStorage(database: database)
    }

    @Test("Compare strings - ascending order")
    func testCompareStrings() {
        let result1 = storage.compareValues("Alice", "Bob", nulls: nil)
        let result2 = storage.compareValues("Bob", "Alice", nulls: nil)
        let result3 = storage.compareValues("Alice", "Alice", nulls: nil)

        #expect(result1 < 0)  // Alice < Bob
        #expect(result2 > 0)  // Bob > Alice
        #expect(result3 == 0) // Alice == Alice
    }

    @Test("Compare integers")
    func testCompareIntegers() {
        let result1 = storage.compareValues(10, 20, nulls: nil)
        let result2 = storage.compareValues(20, 10, nulls: nil)
        let result3 = storage.compareValues(15, 15, nulls: nil)

        #expect(result1 < 0)  // 10 < 20
        #expect(result2 > 0)  // 20 > 10
        #expect(result3 == 0) // 15 == 15
    }

    @Test("Compare Int64")
    func testCompareInt64() {
        let result1 = storage.compareValues(Int64(100), Int64(200), nulls: nil)
        let result2 = storage.compareValues(Int64(200), Int64(100), nulls: nil)

        #expect(result1 < 0)
        #expect(result2 > 0)
    }

    @Test("Compare doubles")
    func testCompareDoubles() {
        let result1 = storage.compareValues(1.5, 2.5, nulls: nil)
        let result2 = storage.compareValues(2.5, 1.5, nulls: nil)
        let result3 = storage.compareValues(1.5, 1.5, nulls: nil)

        #expect(result1 < 0)
        #expect(result2 > 0)
        #expect(result3 == 0)
    }

    @Test("Compare booleans")
    func testCompareBooleans() {
        let result1 = storage.compareValues(false, true, nulls: nil)
        let result2 = storage.compareValues(true, false, nulls: nil)
        let result3 = storage.compareValues(true, true, nulls: nil)

        #expect(result1 < 0)  // false < true
        #expect(result2 > 0)  // true > false
        #expect(result3 == 0) // true == true
    }

    @Test("Compare nil values - default (nulls last)")
    func testCompareNilDefault() {
        let result1 = storage.compareValues(nil, "Bob", nulls: nil)
        let result2 = storage.compareValues("Alice", nil, nulls: nil)
        let result3: Int = storage.compareValues(nil, nil, nulls: nil)

        #expect(result1 > 0)  // nil > any value (nulls last by default)
        #expect(result2 < 0)  // any value < nil
        #expect(result3 == 0) // nil == nil
    }

    @Test("Compare nil values - nulls first")
    func testCompareNilFirst() {
        let result1 = storage.compareValues(nil, "Bob", nulls: .first)
        let result2 = storage.compareValues("Alice", nil, nulls: .first)

        #expect(result1 < 0)  // nil < any value (nulls first)
        #expect(result2 > 0)  // any value > nil
    }

    @Test("Compare nil values - nulls last")
    func testCompareNilLast() {
        let result1 = storage.compareValues(nil, "Bob", nulls: .last)
        let result2 = storage.compareValues("Alice", nil, nulls: .last)

        #expect(result1 > 0)  // nil > any value (nulls last)
        #expect(result2 < 0)  // any value < nil
    }

    @Test("Compare mixed types falls back to string")
    func testCompareMixedTypes() {
        // Mixed types fall back to string comparison
        let result = storage.compareValues("10", 20, nulls: nil)
        // "10" as string vs "20" as string
        #expect(result != 0)  // Just verify it doesn't crash
    }
}

// MARK: - Sort Results Tests

@Suite("Sort Results Tests", .serialized)
struct SortResultsTests {

    private let storage: SchemaStorage

    init() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        storage = SchemaStorage(database: database)
    }

    @Test("Sort by string field ascending")
    func testSortByStringAscending() {
        let results: [(id: String, values: [String: Any])] = [
            (id: "1", values: ["name": "Charlie" as Any]),
            (id: "2", values: ["name": "Alice" as Any]),
            (id: "3", values: ["name": "Bob" as Any]),
        ]

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "name", direction: .ascending)
        )

        #expect(sorted[0].id == "2") // Alice
        #expect(sorted[1].id == "3") // Bob
        #expect(sorted[2].id == "1") // Charlie
    }

    @Test("Sort by string field descending")
    func testSortByStringDescending() {
        let results: [(id: String, values: [String: Any])] = [
            (id: "1", values: ["name": "Charlie" as Any]),
            (id: "2", values: ["name": "Alice" as Any]),
            (id: "3", values: ["name": "Bob" as Any]),
        ]

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "name", direction: .descending)
        )

        #expect(sorted[0].id == "1") // Charlie
        #expect(sorted[1].id == "3") // Bob
        #expect(sorted[2].id == "2") // Alice
    }

    @Test("Sort by integer field ascending")
    func testSortByIntAscending() {
        let results: [(id: String, values: [String: Any])] = [
            (id: "1", values: ["age": 35 as Any]),
            (id: "2", values: ["age": 25 as Any]),
            (id: "3", values: ["age": 30 as Any]),
        ]

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "age", direction: .ascending)
        )

        #expect(sorted[0].id == "2") // 25
        #expect(sorted[1].id == "3") // 30
        #expect(sorted[2].id == "1") // 35
    }

    @Test("Sort by integer field descending")
    func testSortByIntDescending() {
        let results: [(id: String, values: [String: Any])] = [
            (id: "1", values: ["age": 25 as Any]),
            (id: "2", values: ["age": 35 as Any]),
            (id: "3", values: ["age": 30 as Any]),
        ]

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "age", direction: .descending)
        )

        #expect(sorted[0].id == "2") // 35
        #expect(sorted[1].id == "3") // 30
        #expect(sorted[2].id == "1") // 25
    }

    @Test("Sort with missing field values")
    func testSortWithMissingValues() {
        let results: [(id: String, values: [String: Any])] = [
            (id: "1", values: ["name": "Bob" as Any]),
            (id: "2", values: [:]),  // Missing "name" field
            (id: "3", values: ["name": "Alice" as Any]),
        ]

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "name", direction: .ascending, nulls: .last)
        )

        // Alice < Bob < nil
        #expect(sorted[0].id == "3") // Alice
        #expect(sorted[1].id == "1") // Bob
        #expect(sorted[2].id == "2") // nil (missing field)
    }

    @Test("Sort empty results")
    func testSortEmptyResults() {
        let results: [(id: String, values: [String: Any])] = []

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "name")
        )

        #expect(sorted.isEmpty)
    }

    @Test("Sort single result")
    func testSortSingleResult() {
        let results: [(id: String, values: [String: Any])] = [
            (id: "1", values: ["name": "Alice" as Any]),
        ]

        let sorted = storage.sortResults(
            results,
            by: DynamicSortDescriptor(field: "name")
        )

        #expect(sorted.count == 1)
        #expect(sorted[0].id == "1")
    }
}
