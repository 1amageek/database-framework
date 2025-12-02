// VersionIndexKindTests.swift
// FDBIndexing Tests - VersionIndexKind tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine
@testable import VersionIndex

// Test model for VersionIndexKind
struct TestDoc: Persistable {
    typealias ID = String
    var id: String
    var title: String

    static var persistableType: String { "TestDoc" }
    static var allFields: [String] { ["id", "title"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestDoc, Value>) -> String {
        switch keyPath {
        case \TestDoc.id: return "id"
        case \TestDoc.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestDoc>) -> String {
        switch keyPath {
        case \TestDoc.id: return "id"
        case \TestDoc.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestDoc> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

@Suite("VersionIndexKind Tests")
struct VersionIndexKindTests {

    // MARK: - Metadata Tests

    @Test("VersionIndexKind has correct identifier")
    func testIdentifier() {
        #expect(VersionIndexKind<TestDoc>.identifier == "version")
    }

    @Test("VersionIndexKind has hierarchical subspace structure")
    func testSubspaceStructure() {
        // Version indexes store history hierarchically by versionstamp
        #expect(VersionIndexKind<TestDoc>.subspaceStructure == .hierarchical)
    }

    // MARK: - Type Validation Tests

    @Test("VersionIndexKind accepts any types")
    func testAcceptsAnyTypes() throws {
        // Version index accepts any types without validation
        try VersionIndexKind<TestDoc>.validateTypes([Int.self])
        try VersionIndexKind<TestDoc>.validateTypes([String.self])
        try VersionIndexKind<TestDoc>.validateTypes([Double.self])
        try VersionIndexKind<TestDoc>.validateTypes([Int.self, String.self])
        try VersionIndexKind<TestDoc>.validateTypes([])
    }

    // MARK: - Codable Tests

    @Test("VersionIndexKind is Codable")
    func testCodable() throws {
        let kind = VersionIndexKind<TestDoc>(field: \.id)

        // JSON encoding
        let encoder = JSONEncoder()
        let data = try encoder.encode(kind)

        // JSON decoding
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(VersionIndexKind<TestDoc>.self, from: data)

        #expect(decoded == kind)
    }

    // MARK: - Hashable Tests

    @Test("VersionIndexKind is Hashable")
    func testHashable() {
        let kind1 = VersionIndexKind<TestDoc>(field: \.id)
        let kind2 = VersionIndexKind<TestDoc>(field: \.id)

        #expect(kind1 == kind2)
        #expect(kind1.hashValue == kind2.hashValue)
    }
}
