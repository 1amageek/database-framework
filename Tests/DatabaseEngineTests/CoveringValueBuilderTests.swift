// CoveringValueBuilderTests.swift
// Tests for CoveringValueBuilder bitmap encoding/decoding

import Testing
import Foundation
@testable import DatabaseEngine
import Core

@Suite("CoveringValueBuilder Tests", .serialized)
struct CoveringValueBuilderTests {

    @Persistable
    struct TestItem {
        #Directory<TestItem>("test", "items")

        var id: String = UUID().uuidString
        var name: String? = nil
        var age: Int? = nil
        var status: String? = nil
    }

    @Test("Build and decode: all fields present")
    func testAllFieldsPresent() throws {
        var item = TestItem()
        item.name = "Alice"
        item.age = 30
        item.status = "active"

        let fieldNames = ["name", "age", "status"]
        let encoded = try CoveringValueBuilder.build(for: item, storedFieldNames: fieldNames)
        let decoded = try CoveringValueBuilder.decode(encoded, storedFieldNames: fieldNames)

        #expect(decoded["name"] as? String == "Alice")
        #expect(decoded["age"] as? Int64 == 30)
        #expect(decoded["status"] as? String == "active")
    }

    @Test("Build and decode: nil vs empty string")
    func testNilVsEmptyString() throws {
        var item1 = TestItem()
        item1.name = nil        // nil
        item1.status = ""       // empty string
        item1.age = 30

        var item2 = TestItem()
        item2.name = ""         // empty string
        item2.status = nil      // nil
        item2.age = 30

        let fieldNames = ["name", "age", "status"]

        // Item 1: nil name, empty status
        let encoded1 = try CoveringValueBuilder.build(for: item1, storedFieldNames: fieldNames)
        let decoded1 = try CoveringValueBuilder.decode(encoded1, storedFieldNames: fieldNames)

        #expect(decoded1["name"] == nil, "name should be nil")
        #expect(decoded1["age"] as? Int64 == 30)
        #expect(decoded1["status"] as? String == "", "status should be empty string")

        // Item 2: empty name, nil status
        let encoded2 = try CoveringValueBuilder.build(for: item2, storedFieldNames: fieldNames)
        let decoded2 = try CoveringValueBuilder.decode(encoded2, storedFieldNames: fieldNames)

        #expect(decoded2["name"] as? String == "", "name should be empty string")
        #expect(decoded2["age"] as? Int64 == 30)
        #expect(decoded2["status"] == nil, "status should be nil")
    }

    @Test("Build and decode: first field nil")
    func testFirstFieldNil() throws {
        var item = TestItem()
        item.name = nil
        item.age = 30
        item.status = "active"

        let fieldNames = ["name", "age", "status"]
        let encoded = try CoveringValueBuilder.build(for: item, storedFieldNames: fieldNames)
        let decoded = try CoveringValueBuilder.decode(encoded, storedFieldNames: fieldNames)

        #expect(decoded["name"] == nil)
        #expect(decoded["age"] as? Int64 == 30)
        #expect(decoded["status"] as? String == "active")
    }

    @Test("Build and decode: middle field nil")
    func testMiddleFieldNil() throws {
        var item = TestItem()
        item.name = "Alice"
        item.age = nil
        item.status = "active"

        let fieldNames = ["name", "age", "status"]
        let encoded = try CoveringValueBuilder.build(for: item, storedFieldNames: fieldNames)
        let decoded = try CoveringValueBuilder.decode(encoded, storedFieldNames: fieldNames)

        #expect(decoded["name"] as? String == "Alice")
        #expect(decoded["age"] == nil)
        #expect(decoded["status"] as? String == "active")
    }

    @Test("Build and decode: last field nil")
    func testLastFieldNil() throws {
        var item = TestItem()
        item.name = "Alice"
        item.age = 30
        item.status = nil

        let fieldNames = ["name", "age", "status"]
        let encoded = try CoveringValueBuilder.build(for: item, storedFieldNames: fieldNames)
        let decoded = try CoveringValueBuilder.decode(encoded, storedFieldNames: fieldNames)

        #expect(decoded["name"] as? String == "Alice")
        #expect(decoded["age"] as? Int64 == 30)
        #expect(decoded["status"] == nil)
    }

    @Test("Build and decode: all fields nil")
    func testAllFieldsNil() throws {
        let item = TestItem()  // All fields nil

        let fieldNames = ["name", "age", "status"]
        let encoded = try CoveringValueBuilder.build(for: item, storedFieldNames: fieldNames)
        let decoded = try CoveringValueBuilder.decode(encoded, storedFieldNames: fieldNames)

        #expect(decoded["name"] == nil)
        #expect(decoded["age"] == nil)
        #expect(decoded["status"] == nil)
    }

    @Test("Build with empty field list")
    func testEmptyFieldList() throws {
        let item = TestItem()
        let encoded = try CoveringValueBuilder.build(for: item, storedFieldNames: [])

        #expect(encoded.isEmpty)

        let decoded = try CoveringValueBuilder.decode(encoded, storedFieldNames: [])
        #expect(decoded.isEmpty)
    }
}
