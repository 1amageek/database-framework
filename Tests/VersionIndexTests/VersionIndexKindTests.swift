// VersionIndexKindTests.swift
// FDBIndexing Tests - VersionIndexKind tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine
@testable import VersionIndex

@Suite("VersionIndexKind Tests")
struct VersionIndexKindTests {

    // MARK: - Metadata Tests

    @Test("VersionIndexKind has correct identifier")
    func testIdentifier() {
        #expect(VersionIndexKind.identifier == "version")
    }

    @Test("VersionIndexKind has hierarchical subspace structure")
    func testSubspaceStructure() {
        // Version indexes store history hierarchically by versionstamp
        #expect(VersionIndexKind.subspaceStructure == .hierarchical)
    }

    // MARK: - Type Validation Tests

    @Test("VersionIndexKind accepts any types")
    func testAcceptsAnyTypes() throws {
        // Version index accepts any types without validation
        try VersionIndexKind.validateTypes([Int.self])
        try VersionIndexKind.validateTypes([String.self])
        try VersionIndexKind.validateTypes([Double.self])
        try VersionIndexKind.validateTypes([Int.self, String.self])
        try VersionIndexKind.validateTypes([])
    }

    // MARK: - Codable Tests

    @Test("VersionIndexKind is Codable")
    func testCodable() throws {
        let kind = VersionIndexKind()

        // JSON encoding
        let encoder = JSONEncoder()
        let data = try encoder.encode(kind)

        // JSON decoding
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(VersionIndexKind.self, from: data)

        #expect(decoded == kind)
    }

    // MARK: - Hashable Tests

    @Test("VersionIndexKind is Hashable")
    func testHashable() {
        let kind1 = VersionIndexKind()
        let kind2 = VersionIndexKind()

        #expect(kind1 == kind2)
        #expect(kind1.hashValue == kind2.hashValue)
    }
}
