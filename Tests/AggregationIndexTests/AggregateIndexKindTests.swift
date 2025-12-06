// AggregateIndexKindTests.swift
// FDBIndexing Tests - Aggregate index (Count, Sum, Min, Max) tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine
@testable import AggregationIndex

// Test model for aggregate index tests
@Persistable
private struct AggTestItem {
    var category: String
    var subcategory: String
    var value: Int64
    var score: Double
    var createdAt: Date = Date()
}

// MARK: - CountIndexKind Tests

@Suite("CountIndexKind Tests")
struct CountIndexKindTests {

    @Test("CountIndexKind has correct identifier")
    func testIdentifier() {
        #expect(CountIndexKind<AggTestItem>.identifier == "count")
    }

    @Test("CountIndexKind has aggregation subspace structure")
    func testSubspaceStructure() {
        #expect(CountIndexKind<AggTestItem>.subspaceStructure == .aggregation)
    }

    @Test("CountIndexKind validates single grouping field")
    func testValidateSingleGroupingField() throws {
        try CountIndexKind<AggTestItem>.validateTypes([String.self])
        try CountIndexKind<AggTestItem>.validateTypes([Int64.self])
    }

    @Test("CountIndexKind validates composite grouping fields")
    func testValidateCompositeGroupingFields() throws {
        try CountIndexKind<AggTestItem>.validateTypes([String.self, String.self])
        try CountIndexKind<AggTestItem>.validateTypes([String.self, Int64.self])
    }

    @Test("CountIndexKind rejects empty fields")
    func testRejectEmptyFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try CountIndexKind<AggTestItem>.validateTypes([])
        }
    }

    @Test("CountIndexKind rejects non-Comparable grouping fields")
    func testRejectNonComparableGroupingFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try CountIndexKind<AggTestItem>.validateTypes([[Int].self])
        }
    }
}

// MARK: - SumIndexKind Tests

@Suite("SumIndexKind Tests")
struct SumIndexKindTests {

    @Test("SumIndexKind has correct identifier")
    func testIdentifier() {
        #expect(SumIndexKind<AggTestItem, Int64>.identifier == "sum")
    }

    @Test("SumIndexKind has aggregation subspace structure")
    func testSubspaceStructure() {
        #expect(SumIndexKind<AggTestItem, Int64>.subspaceStructure == .aggregation)
    }

    @Test("SumIndexKind validates grouping + numeric value field")
    func testValidateGroupingAndNumericField() throws {
        // String + Int64
        try SumIndexKind<AggTestItem, Int64>.validateTypes([String.self, Int64.self])

        // String + Double
        try SumIndexKind<AggTestItem, Double>.validateTypes([String.self, Double.self])

        // String + String + Int64 (composite grouping + value)
        try SumIndexKind<AggTestItem, Int64>.validateTypes([String.self, String.self, Int64.self])
    }

    @Test("SumIndexKind rejects less than 2 fields")
    func testRejectLessThanTwoFields() {
        // 0 fields
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggTestItem, Int64>.validateTypes([])
        }

        // 1 field
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggTestItem, Int64>.validateTypes([Int64.self])
        }
    }

    @Test("SumIndexKind rejects non-Comparable grouping fields")
    func testRejectNonComparableGroupingFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggTestItem, Int64>.validateTypes([[Int].self, Int64.self])
        }
    }

    @Test("SumIndexKind rejects non-numeric value field")
    func testRejectNonNumericValueField() {
        // Value field is String (not numeric)
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggTestItem, Int64>.validateTypes([String.self, String.self])
        }

        // Value field is Date (not numeric)
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggTestItem, Int64>.validateTypes([String.self, Date.self])
        }
    }
}

// MARK: - MinIndexKind Tests

@Suite("MinIndexKind Tests")
struct MinIndexKindTests {

    @Test("MinIndexKind has correct identifier")
    func testIdentifier() {
        #expect(MinIndexKind<AggTestItem, Int64>.identifier == "min")
    }

    @Test("MinIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(MinIndexKind<AggTestItem, Int64>.subspaceStructure == .flat)
    }

    @Test("MinIndexKind validates grouping + Comparable value field")
    func testValidateGroupingAndComparableField() throws {
        // String + Double
        try MinIndexKind<AggTestItem, Double>.validateTypes([String.self, Double.self])

        // String + Int64
        try MinIndexKind<AggTestItem, Int64>.validateTypes([String.self, Int64.self])

        // String + String + Date (composite grouping + value)
        try MinIndexKind<AggTestItem, Date>.validateTypes([String.self, String.self, Date.self])
    }

    @Test("MinIndexKind rejects less than 2 fields")
    func testRejectLessThanTwoFields() {
        // 0 fields
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggTestItem, Double>.validateTypes([])
        }

        // 1 field
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggTestItem, Double>.validateTypes([Double.self])
        }
    }

    @Test("MinIndexKind rejects non-Comparable fields")
    func testRejectNonComparableFields() {
        // Grouping field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggTestItem, Double>.validateTypes([[Int].self, Double.self])
        }

        // Value field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggTestItem, Int64>.validateTypes([String.self, [Int].self])
        }
    }
}

// MARK: - MaxIndexKind Tests

@Suite("MaxIndexKind Tests")
struct MaxIndexKindTests {

    @Test("MaxIndexKind has correct identifier")
    func testIdentifier() {
        #expect(MaxIndexKind<AggTestItem, Int64>.identifier == "max")
    }

    @Test("MaxIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(MaxIndexKind<AggTestItem, Int64>.subspaceStructure == .flat)
    }

    @Test("MaxIndexKind validates grouping + Comparable value field")
    func testValidateGroupingAndComparableField() throws {
        // String + Double
        try MaxIndexKind<AggTestItem, Double>.validateTypes([String.self, Double.self])

        // String + Int64
        try MaxIndexKind<AggTestItem, Int64>.validateTypes([String.self, Int64.self])

        // String + String + Date (composite grouping + value)
        try MaxIndexKind<AggTestItem, Date>.validateTypes([String.self, String.self, Date.self])
    }

    @Test("MaxIndexKind rejects less than 2 fields")
    func testRejectLessThanTwoFields() {
        // 0 fields
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggTestItem, Double>.validateTypes([])
        }

        // 1 field
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggTestItem, Double>.validateTypes([Double.self])
        }
    }

    @Test("MaxIndexKind rejects non-Comparable fields")
    func testRejectNonComparableFields() {
        // Grouping field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggTestItem, Double>.validateTypes([[Int].self, Double.self])
        }

        // Value field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggTestItem, Int64>.validateTypes([String.self, [Int].self])
        }
    }
}
