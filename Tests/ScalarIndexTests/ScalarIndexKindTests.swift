// ScalarIndexKindTests.swift
// FDBIndexing Tests - ScalarIndexKind tests

import Testing
import Foundation
import Core
import TestSupport
@testable import DatabaseEngine
@testable import ScalarIndex

// Test model for ScalarIndexKind
struct TestProduct: Persistable {
    typealias ID = String
    var id: String
    var name: String
    var price: Int64

    static var persistableType: String { "TestProduct" }
    static var allFields: [String] { ["id", "name", "price"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "price": return price
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestProduct, Value>) -> String {
        switch keyPath {
        case \TestProduct.id: return "id"
        case \TestProduct.name: return "name"
        case \TestProduct.price: return "price"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestProduct>) -> String {
        switch keyPath {
        case \TestProduct.id: return "id"
        case \TestProduct.name: return "name"
        case \TestProduct.price: return "price"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestProduct> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

@Suite("ScalarIndexKind Tests")
struct ScalarIndexKindTests {

    // MARK: - Metadata Tests

    @Test("ScalarIndexKind has correct identifier")
    func testIdentifier() {
        #expect(ScalarIndexKind<TestProduct>.identifier == "scalar")
    }

    @Test("ScalarIndexKind has flat subspace structure")
    func testSubspaceStructure() {
        #expect(ScalarIndexKind<TestProduct>.subspaceStructure == .flat)
    }

    // MARK: - Type Validation Tests

    @Test("ScalarIndexKind validates single Comparable field")
    func testValidateSingleComparableField() throws {
        // String
        try ScalarIndexKind<TestProduct>.validateTypes([String.self])

        // Int64
        try ScalarIndexKind<TestProduct>.validateTypes([Int64.self])

        // Double
        try ScalarIndexKind<TestProduct>.validateTypes([Double.self])

        // Date
        try ScalarIndexKind<TestProduct>.validateTypes([Date.self])

        // UUID
        try ScalarIndexKind<TestProduct>.validateTypes([UUID.self])
    }

    @Test("ScalarIndexKind validates composite Comparable fields")
    func testValidateCompositeComparableFields() throws {
        // String + Int64
        try ScalarIndexKind<TestProduct>.validateTypes([String.self, Int64.self])

        // String + String + Double
        try ScalarIndexKind<TestProduct>.validateTypes([String.self, String.self, Double.self])

        // Date + UUID
        try ScalarIndexKind<TestProduct>.validateTypes([Date.self, UUID.self])
    }

    @Test("ScalarIndexKind rejects empty fields")
    func testRejectEmptyFields() {
        #expect(throws: IndexTypeValidationError.self) {
            try ScalarIndexKind<TestProduct>.validateTypes([])
        }
    }

    @Test("ScalarIndexKind rejects non-Comparable types")
    func testRejectNonComparableTypes() {
        // Array type (not Comparable)
        #expect(throws: IndexTypeValidationError.self) {
            try ScalarIndexKind<TestProduct>.validateTypes([[Int].self])
        }

        // Optional type (not Comparable)
        #expect(throws: IndexTypeValidationError.self) {
            try ScalarIndexKind<TestProduct>.validateTypes([Int?.self])
        }
    }

    // MARK: - Codable Tests

    @Test("ScalarIndexKind is Codable")
    func testCodable() throws {
        let kind = ScalarIndexKind<TestProduct>(fields: [\.name])

        // JSON encoding
        let encoder = JSONEncoder()
        let data = try encoder.encode(kind)

        // JSON decoding
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(ScalarIndexKind<TestProduct>.self, from: data)

        #expect(decoded == kind)
    }

    // MARK: - Hashable Tests

    @Test("ScalarIndexKind is Hashable")
    func testHashable() {
        let kind1 = ScalarIndexKind<TestProduct>(fields: [\.name])
        let kind2 = ScalarIndexKind<TestProduct>(fields: [\.name])

        #expect(kind1 == kind2)
        #expect(kind1.hashValue == kind2.hashValue)
    }
}
