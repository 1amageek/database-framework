#if FOUNDATION_DB
// AggregateIndexKindTests.swift
// FDBIndexing Tests - Aggregate index (Count, Sum, Min, Max) tests

import Testing
import TestHeartbeat
import Foundation
import Core
import DatabaseValue
@testable import DatabaseEngine
@testable import AggregationIndex

// Test model for aggregate index tests
@Persistable
private struct AggregateIndexEntity {
    var category: String
    var subcategory: String
    var value: Int64
    var score: Double
    var createdAt: Date = Date()
}

// MARK: - CountIndexKind Tests

@Suite("CountIndexKind Tests", .heartbeat)
struct CountIndexKindTests {

    @Test("CountIndexKind has correct identifier")
    func testIdentifier() {
        #expect(CountIndexKind<AggregateIndexEntity>.identifier == "count")
    }

    @Test("CountIndexKind has aggregation subspace structure")
    func testSubspaceStructure() {
        #expect(CountIndexKind<AggregateIndexEntity>.subspaceStructure == .aggregation)
    }

    @Test("CountIndexKind validates single grouping field")
    func testValidateSingleGroupingField() throws {
        try CountIndexKind<AggregateIndexEntity>.validateTypes([String.self])
        try CountIndexKind<AggregateIndexEntity>.validateTypes([Int64.self])
    }

    @Test("CountIndexKind validates composite grouping fields")
    func testValidateCompositeGroupingFields() throws {
        try CountIndexKind<AggregateIndexEntity>.validateTypes([String.self, String.self])
        try CountIndexKind<AggregateIndexEntity>.validateTypes([String.self, Int64.self])
    }

    @Test("CountIndexKind accepts an empty global grouping")
    func testAcceptEmptyGlobalGrouping() throws {
        try CountIndexKind<AggregateIndexEntity>.validateTypes([])
    }

    @Test("CountIndexKind rejects non-Comparable grouping fields")
    func testRejectNonComparableGroupingFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try CountIndexKind<AggregateIndexEntity>.validateTypes([[Int].self])
        }
    }
}

// MARK: - SumIndexKind Tests

@Suite("SumIndexKind Tests", .heartbeat)
struct SumIndexKindTests {

    @Test("SumIndexKind has correct identifier")
    func testIdentifier() {
        #expect(SumIndexKind<AggregateIndexEntity, Int64>.identifier == "sum")
    }

    @Test("SumIndexKind has aggregation subspace structure")
    func testSubspaceStructure() {
        #expect(SumIndexKind<AggregateIndexEntity, Int64>.subspaceStructure == .aggregation)
    }

    @Test("SumIndexKind validates grouping + numeric value field")
    func testValidateGroupingAndNumericField() throws {
        // String + Int64
        try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, Int64.self])

        // String + Double
        try SumIndexKind<AggregateIndexEntity, Double>.validateTypes([String.self, Double.self])

        // String + String + Int64 (composite grouping + value)
        try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, String.self, Int64.self])
    }

    @Test("SumIndexKind accepts one global value field and rejects no fields")
    func testValidateGlobalValueField() throws {
        try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([Int64.self])

        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([])
        }
    }

    @Test("SumIndexKind rejects non-Comparable grouping fields")
    func testRejectNonComparableGroupingFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([[Int].self, Int64.self])
        }
    }

    @Test("SumIndexKind rejects non-numeric value field")
    func testRejectNonNumericValueField() {
        // Value field is String (not numeric)
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, String.self])
        }

        // Value field is Date (not numeric)
        #expect(throws: IndexTypeValidationError.self) {
            try SumIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, Date.self])
        }
    }
}

// MARK: - MinIndexKind Tests

@Suite("MinIndexKind Tests", .heartbeat)
struct MinIndexKindTests {

    @Test("MinIndexKind has correct identifier")
    func testIdentifier() {
        #expect(MinIndexKind<AggregateIndexEntity, Int64>.identifier == "min")
    }

    @Test("MinIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(MinIndexKind<AggregateIndexEntity, Int64>.subspaceStructure == .flat)
    }

    @Test("MinIndexKind validates grouping + Comparable value field")
    func testValidateGroupingAndComparableField() throws {
        // String + Double
        try MinIndexKind<AggregateIndexEntity, Double>.validateTypes([String.self, Double.self])

        // String + Int64
        try MinIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, Int64.self])

        // String + String + Date (composite grouping + value)
        try MinIndexKind<AggregateIndexEntity, Date>.validateTypes([String.self, String.self, Date.self])
    }

    @Test("MinIndexKind accepts one global value field and rejects no fields")
    func testValidateGlobalValueField() throws {
        try MinIndexKind<AggregateIndexEntity, Double>.validateTypes([Double.self])

        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggregateIndexEntity, Double>.validateTypes([])
        }
    }

    @Test("MinIndexKind rejects non-Comparable fields")
    func testRejectNonComparableFields() {
        // Grouping field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggregateIndexEntity, Double>.validateTypes([[Int].self, Double.self])
        }

        // Value field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MinIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, [Int].self])
        }
    }
}

// MARK: - MaxIndexKind Tests

@Suite("MaxIndexKind Tests", .heartbeat)
struct MaxIndexKindTests {

    @Test("MaxIndexKind has correct identifier")
    func testIdentifier() {
        #expect(MaxIndexKind<AggregateIndexEntity, Int64>.identifier == "max")
    }

    @Test("MaxIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(MaxIndexKind<AggregateIndexEntity, Int64>.subspaceStructure == .flat)
    }

    @Test("MaxIndexKind validates grouping + Comparable value field")
    func testValidateGroupingAndComparableField() throws {
        // String + Double
        try MaxIndexKind<AggregateIndexEntity, Double>.validateTypes([String.self, Double.self])

        // String + Int64
        try MaxIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, Int64.self])

        // String + String + Date (composite grouping + value)
        try MaxIndexKind<AggregateIndexEntity, Date>.validateTypes([String.self, String.self, Date.self])
    }

    @Test("MaxIndexKind accepts one global value field and rejects no fields")
    func testValidateGlobalValueField() throws {
        try MaxIndexKind<AggregateIndexEntity, Double>.validateTypes([Double.self])

        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggregateIndexEntity, Double>.validateTypes([])
        }
    }

    @Test("MaxIndexKind rejects non-Comparable fields")
    func testRejectNonComparableFields() {
        // Grouping field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggregateIndexEntity, Double>.validateTypes([[Int].self, Double.self])
        }

        // Value field is not Comparable
        #expect(throws: IndexTypeValidationError.self) {
            try MaxIndexKind<AggregateIndexEntity, Int64>.validateTypes([String.self, [Int].self])
        }
    }
}
#endif
