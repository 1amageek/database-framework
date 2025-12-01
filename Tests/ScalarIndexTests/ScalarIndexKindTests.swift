// ScalarIndexKindTests.swift
// FDBIndexing Tests - ScalarIndexKind tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine
@testable import ScalarIndex

@Suite("ScalarIndexKind Tests")
struct ScalarIndexKindTests {

    // MARK: - Metadata Tests

    @Test("ScalarIndexKind has correct identifier")
    func testIdentifier() {
        #expect(ScalarIndexKind.identifier == "scalar")
    }

    @Test("ScalarIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(ScalarIndexKind.subspaceStructure == .flat)
    }

    // MARK: - Type Validation Tests

    @Test("ScalarIndexKind validates single Comparable field")
    func testValidateSingleComparableField() throws {
        // String
        try ScalarIndexKind.validateTypes([String.self])

        // Int64
        try ScalarIndexKind.validateTypes([Int64.self])

        // Double
        try ScalarIndexKind.validateTypes([Double.self])

        // Date
        try ScalarIndexKind.validateTypes([Date.self])

        // UUID
        try ScalarIndexKind.validateTypes([UUID.self])
    }

    @Test("ScalarIndexKind validates composite Comparable fields")
    func testValidateCompositeComparableFields() throws {
        // String + Int64
        try ScalarIndexKind.validateTypes([String.self, Int64.self])

        // String + String + Double
        try ScalarIndexKind.validateTypes([String.self, String.self, Double.self])

        // Date + UUID
        try ScalarIndexKind.validateTypes([Date.self, UUID.self])
    }

    @Test("ScalarIndexKind rejects empty fields")
    func testRejectEmptyFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try ScalarIndexKind.validateTypes([])
        }
    }

    @Test("ScalarIndexKind rejects non-Comparable types")
    func testRejectNonComparableTypes() {
        // Array type (not Comparable)
        #expect(throws: IndexTypeValidationError.self) {
            try ScalarIndexKind.validateTypes([[Int].self])
        }

        // Optional type (not Comparable)
        #expect(throws: IndexTypeValidationError.self) {
            try ScalarIndexKind.validateTypes([Int?.self])
        }
    }

    // MARK: - Codable Tests

    @Test("ScalarIndexKind is Codable")
    func testCodable() throws {
        let kind = ScalarIndexKind()

        // JSON encoding
        let encoder = JSONEncoder()
        let data = try encoder.encode(kind)

        // JSON decoding
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(ScalarIndexKind.self, from: data)

        #expect(decoded == kind)
    }

    // MARK: - Hashable Tests

    @Test("ScalarIndexKind is Hashable")
    func testHashable() {
        let kind1 = ScalarIndexKind()
        let kind2 = ScalarIndexKind()

        #expect(kind1 == kind2)
        #expect(kind1.hashValue == kind2.hashValue)
    }
}
