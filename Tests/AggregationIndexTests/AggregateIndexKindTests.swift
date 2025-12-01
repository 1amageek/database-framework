// AggregateIndexKindTests.swift
// FDBIndexing Tests - Aggregate index (Count, Sum, Min, Max) tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - CountIndexKind Tests

@Suite("CountIndexKind Tests")
struct CountIndexKindTests {

    @Test("CountIndexKind has correct identifier")
    func testIdentifier() {
        #expect(CountIndexKind.identifier == "count")
    }

    @Test("CountIndexKind has aggregation subspace structure")
    func testSubspaceStructure() {
        #expect(CountIndexKind.subspaceStructure == .aggregation)
    }

    @Test("CountIndexKind validates single grouping field")
    func testValidateSingleGroupingField() throws {
        try CountIndexKind.validateTypes([String.self])
        try CountIndexKind.validateTypes([Int64.self])
    }

    @Test("CountIndexKind validates composite grouping fields")
    func testValidateCompositeGroupingFields() throws {
        try CountIndexKind.validateTypes([String.self, String.self])
        try CountIndexKind.validateTypes([String.self, Int64.self])
    }

    @Test("CountIndexKind rejects empty fields")
    func testRejectEmptyFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try CountIndexKind.validateTypes([])
        }
    }

    @Test("CountIndexKind rejects non-Comparable grouping fields")
    func testRejectNonComparableGroupingFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try CountIndexKind.validateTypes([[Int].self])
        }
    }
}

// MARK: - SumIndexKind Tests

@Suite("SumIndexKind Tests")
struct SumIndexKindTests {

    @Test("SumIndexKind has correct identifier")
    func testIdentifier() {
        #expect(SumIndexKind.identifier == "sum")
    }

    @Test("SumIndexKind has aggregation subspace structure")
    func testSubspaceStructure() {
        #expect(SumIndexKind.subspaceStructure == .aggregation)
    }

    @Test("SumIndexKind validates grouping + numeric value field")
    func testValidateGroupingAndNumericField() throws {
        // String + Int64
        try SumIndexKind.validateTypes([String.self, Int64.self])

        // String + Double
        try SumIndexKind.validateTypes([String.self, Double.self])

        // String + String + Int64 (composite grouping + value)
        try SumIndexKind.validateTypes([String.self, String.self, Int64.self])
    }

    @Test("SumIndexKind rejects less than 2 fields")
    func testRejectLessThanTwoFields() {
        // 0 fields
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind.validateTypes([])
        }

        // 1 field
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind.validateTypes([Int64.self])
        }
    }

    @Test("SumIndexKind rejects non-Comparable grouping fields")
    func testRejectNonComparableGroupingFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind.validateTypes([[Int].self, Int64.self])
        }
    }

    @Test("SumIndexKind rejects non-numeric value field")
    func testRejectNonNumericValueField() {
        // Value field is String (not numeric)
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind.validateTypes([String.self, String.self])
        }

        // Value field is Date (not numeric)
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind.validateTypes([String.self, Date.self])
        }
    }
}

// MARK: - MinIndexKind Tests

@Suite("MinIndexKind Tests")
struct MinIndexKindTests {

    @Test("MinIndexKind has correct identifier")
    func testIdentifier() {
        #expect(MinIndexKind.identifier == "min")
    }

    @Test("MinIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(MinIndexKind.subspaceStructure == .flat)
    }

    @Test("MinIndexKind validates grouping + Comparable value field")
    func testValidateGroupingAndComparableField() throws {
        // String + Double
        try MinIndexKind.validateTypes([String.self, Double.self])

        // String + Int64
        try MinIndexKind.validateTypes([String.self, Int64.self])

        // String + String + Date (composite grouping + value)
        try MinIndexKind.validateTypes([String.self, String.self, Date.self])
    }

    @Test("MinIndexKind rejects less than 2 fields")
    func testRejectLessThanTwoFields() {
        // 0 fields
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind.validateTypes([])
        }

        // 1 field
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind.validateTypes([Double.self])
        }
    }

    @Test("MinIndexKind rejects non-Comparable fields")
    func testRejectNonComparableFields() {
        // Grouping field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind.validateTypes([[Int].self, Double.self])
        }

        // Value field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind.validateTypes([String.self, [Int].self])
        }
    }
}

// MARK: - MaxIndexKind Tests

@Suite("MaxIndexKind Tests")
struct MaxIndexKindTests {

    @Test("MaxIndexKind has correct identifier")
    func testIdentifier() {
        #expect(MaxIndexKind.identifier == "max")
    }

    @Test("MaxIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(MaxIndexKind.subspaceStructure == .flat)
    }

    @Test("MaxIndexKind validates grouping + Comparable value field")
    func testValidateGroupingAndComparableField() throws {
        // String + Double
        try MaxIndexKind.validateTypes([String.self, Double.self])

        // String + Int64
        try MaxIndexKind.validateTypes([String.self, Int64.self])

        // String + String + Date (composite grouping + value)
        try MaxIndexKind.validateTypes([String.self, String.self, Date.self])
    }

    @Test("MaxIndexKind rejects less than 2 fields")
    func testRejectLessThanTwoFields() {
        // 0 fields
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind.validateTypes([])
        }

        // 1 field
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind.validateTypes([Double.self])
        }
    }

    @Test("MaxIndexKind rejects non-Comparable fields")
    func testRejectNonComparableFields() {
        // Grouping field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind.validateTypes([[Int].self, Double.self])
        }

        // Value field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind.validateTypes([String.self, [Int].self])
        }
    }
}
