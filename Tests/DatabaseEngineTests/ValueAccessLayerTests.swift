#if !os(WASI)
#if FOUNDATION_DB
// ValueAccessLayerTests.swift
// Tests for the 3-layer value access architecture:
// Layer 1: FieldComparison.evaluate(on:) / SortDescriptor.orderedComparison
// Layer 2: FieldReader
// Layer 3: DataAccess (tested elsewhere)

import Testing
import TestHeartbeat
import Foundation
import DatabaseEngine
import DatabaseValue
@testable import Core

/// Disambiguate from Foundation.Predicate
private typealias Predicate = DatabaseEngine.Predicate

// MARK: - Test Models

/// Model with various field types for value access testing
struct ValueAccessRecord: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var age: Int
    var score: Double
    var isActive: Bool
    var tag: String?

    init(
        id: String = UUID().uuidString,
        name: String = "",
        age: Int = 0,
        score: Double = 0.0,
        isActive: Bool = true,
        tag: String? = nil
    ) {
        self.id = id
        self.name = name
        self.age = age
        self.score = score
        self.isActive = isActive
        self.tag = tag
    }

    static var persistableType: String { "ValueAccessRecord" }
    static var allFields: [String] { ["id", "name", "age", "score", "isActive", "tag"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "age": return age
        case "score": return score
        case "isActive": return isActive
        case "tag": return tag
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<ValueAccessRecord, Value>) -> String {
        switch keyPath {
        case \ValueAccessRecord.id: return "id"
        case \ValueAccessRecord.name: return "name"
        case \ValueAccessRecord.age: return "age"
        case \ValueAccessRecord.score: return "score"
        case \ValueAccessRecord.isActive: return "isActive"
        case \ValueAccessRecord.tag: return "tag"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<ValueAccessRecord>) -> String {
        switch keyPath {
        case \ValueAccessRecord.id: return "id"
        case \ValueAccessRecord.name: return "name"
        case \ValueAccessRecord.age: return "age"
        case \ValueAccessRecord.score: return "score"
        case \ValueAccessRecord.isActive: return "isActive"
        case \ValueAccessRecord.tag: return "tag"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<ValueAccessRecord> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

/// Model with nested struct for dot-notation testing
struct VALNestedItem: Persistable {
    typealias ID = String

    struct Address: Sendable, Codable {
        var city: String
        var zip: String
    }

    var id: String
    var address: Address

    init(id: String = UUID().uuidString, address: Address = Address(city: "", zip: "")) {
        self.id = id
        self.address = address
    }

    static var persistableType: String { "VALNestedItem" }
    static var allFields: [String] { ["id", "address"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "address": return address
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<VALNestedItem, Value>) -> String {
        switch keyPath {
        case \VALNestedItem.id: return "id"
        case \VALNestedItem.address: return "address"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<VALNestedItem>) -> String {
        switch keyPath {
        case \VALNestedItem.id: return "id"
        case \VALNestedItem.address: return "address"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<VALNestedItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

/// Model with an array field for QueryRow encoding/decoding tests.
struct VALVectorItem: Persistable {
    typealias ID = String

    var id: String
    var embedding: [Float]

    init(
        id: String = UUID().uuidString,
        embedding: [Float] = []
    ) {
        self.id = id
        self.embedding = embedding
    }

    static var persistableType: String { "VALVectorItem" }
    static var allFields: [String] { ["id", "embedding"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "embedding": return embedding
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<VALVectorItem, Value>) -> String {
        switch keyPath {
        case \VALVectorItem.id: return "id"
        case \VALVectorItem.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<VALVectorItem>) -> String {
        switch keyPath {
        case \VALVectorItem.id: return "id"
        case \VALVectorItem.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<VALVectorItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - FieldReader Tests

@Suite("FieldReader Tests", .heartbeat)
struct FieldReaderTests {

    // MARK: - read(from:keyPath:fieldName:) — PartialKeyPath path

    @Test("Read field via PartialKeyPath returns correct value")
    func readViaPartialKeyPath() {
        let item = ValueAccessRecord(name: "Alice", age: 30, score: 95.5)
        let kp: AnyKeyPath = \ValueAccessRecord.name

        let result = FieldReader.read(from: item, keyPath: kp, fieldName: "name")
        #expect(result as? String == "Alice")
    }

    @Test("Read Int field via PartialKeyPath")
    func readIntViaPartialKeyPath() {
        let item = ValueAccessRecord(name: "Bob", age: 25)
        let result = FieldReader.read(from: item, keyPath: \ValueAccessRecord.age as AnyKeyPath, fieldName: "age")
        #expect(result as? Int == 25)
    }

    @Test("Read Double field via PartialKeyPath")
    func readDoubleViaPartialKeyPath() {
        let item = ValueAccessRecord(score: 88.5)
        let result = FieldReader.read(from: item, keyPath: \ValueAccessRecord.score as AnyKeyPath, fieldName: "score")
        #expect(result as? Double == 88.5)
    }

    @Test("Read Bool field via PartialKeyPath")
    func readBoolViaPartialKeyPath() {
        let item = ValueAccessRecord(isActive: false)
        let result = FieldReader.read(from: item, keyPath: \ValueAccessRecord.isActive as AnyKeyPath, fieldName: "isActive")
        #expect(result as? Bool == false)
    }

    // MARK: - read(from:fieldName:) — dynamicMember path

    @Test("Read field by name returns correct value")
    func readByFieldName() {
        let item = ValueAccessRecord(name: "Charlie", age: 40)

        #expect(FieldReader.read(from: item, fieldName: "name") as? String == "Charlie")
        #expect(FieldReader.read(from: item, fieldName: "age") as? Int == 40)
    }

    @Test("Read unknown field returns nil")
    func readUnknownField() {
        let item = ValueAccessRecord(name: "Alice")
        let result = FieldReader.read(from: item, fieldName: "nonExistent")
        #expect(result == nil)
    }

    @Test("Read optional field with value returns the value")
    func readOptionalFieldWithValue() {
        let item = ValueAccessRecord(tag: "vip")
        let result = FieldReader.read(from: item, fieldName: "tag")
        #expect(result as? String == "vip")
    }

    @Test("Read optional field without value returns nil")
    func readOptionalFieldNil() {
        let item = ValueAccessRecord(tag: nil)
        let result = FieldReader.read(from: item, fieldName: "tag")
        #expect(result == nil)
    }

    // MARK: - Nested field access via dot notation

    @Test("Read nested field via dot notation")
    func readNestedField() {
        let item = VALNestedItem(address: .init(city: "Tokyo", zip: "100-0001"))
        let result = FieldReader.read(from: item, fieldName: "address.city")
        #expect(result as? String == "Tokyo")
    }

    @Test("Read nested field second level")
    func readNestedFieldZip() {
        let item = VALNestedItem(address: .init(city: "Osaka", zip: "530-0001"))
        let result = FieldReader.read(from: item, fieldName: "address.zip")
        #expect(result as? String == "530-0001")
    }

    @Test("Read nested field with invalid path returns nil")
    func readNestedFieldInvalid() {
        let item = VALNestedItem(address: .init(city: "Tokyo", zip: "100-0001"))
        let result = FieldReader.read(from: item, fieldName: "address.country")
        #expect(result == nil)
    }

    // MARK: - readFieldValue

    @Test("readFieldValue converts string to FieldValue")
    func readFieldValueString() {
        let item = ValueAccessRecord(name: "Alice")
        let fv = FieldReader.readFieldValue(from: item, fieldName: "name")
        #expect(fv == .string("Alice"))
    }

    @Test("readFieldValue converts int to FieldValue")
    func readFieldValueInt() {
        let item = ValueAccessRecord(age: 30)
        let fv = FieldReader.readFieldValue(from: item, fieldName: "age")
        #expect(fv == .int64(30))
    }

    @Test("readFieldValue returns .null for unknown field")
    func readFieldValueUnknown() {
        let item = ValueAccessRecord()
        let fv = FieldReader.readFieldValue(from: item, fieldName: "nonExistent")
        #expect(fv == .null)
    }

    @Test("readFieldValue returns .null for nil optional")
    func readFieldValueNilOptional() {
        let item = ValueAccessRecord(tag: nil)
        let fv = FieldReader.readFieldValue(from: item, fieldName: "tag")
        #expect(fv == .null)
    }

    @Test("readFieldValue converts FieldValueConvertible arrays")
    func readFieldValueArray() {
        let item = VALVectorItem(embedding: [1.0, 0.5, 0.0])
        let fv = FieldReader.readFieldValue(from: item, fieldName: "embedding")

        #expect(fv == .array([.double(1.0), .double(0.5), .double(0.0)]))
    }

    @Test("QueryRowCodec round-trips array fields")
    func queryRowCodecRoundTripsArrayFields() throws {
        let item = VALVectorItem(id: "vector-1", embedding: [1.0, 0.5, 0.0])

        let row = try QueryRowCodec.encodeAny(item)
        let decoded = try QueryRowCodec.decode(row, as: VALVectorItem.self)

        #expect(row.fields["embedding"] == .array([.double(1.0), .double(0.5), .double(0.0)]))
        #expect(decoded.id == item.id)
        #expect(decoded.embedding == item.embedding)
    }
}

// MARK: - FieldComparison.evaluate Tests

@Suite("FieldComparison.evaluate Tests", .heartbeat)
struct FieldComparisonEvaluateTests {

    let alice = ValueAccessRecord(name: "Alice", age: 30, score: 95.5, isActive: true, tag: "vip")
    let bob = ValueAccessRecord(name: "Bob", age: 25, score: 80.0, isActive: false, tag: nil)

    // MARK: - Fast path (operator-constructed with closure)

    @Test("Fast path: == evaluates correctly")
    func fastPathEqual() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.age == 30
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == true)
            #expect(cmp.evaluate(on: bob) == false)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: != evaluates correctly")
    func fastPathNotEqual() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.name != "Alice"
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == false)
            #expect(cmp.evaluate(on: bob) == true)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: < evaluates correctly")
    func fastPathLessThan() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.age < 28
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == false)
            #expect(cmp.evaluate(on: bob) == true)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: <= evaluates correctly")
    func fastPathLessThanOrEqual() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.age <= 25
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == false)
            #expect(cmp.evaluate(on: bob) == true)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: > evaluates correctly")
    func fastPathGreaterThan() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.score > 90.0
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == true)
            #expect(cmp.evaluate(on: bob) == false)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: >= evaluates correctly")
    func fastPathGreaterThanOrEqual() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.score >= 80.0
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == true)
            #expect(cmp.evaluate(on: bob) == true)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: String.contains evaluates correctly")
    func fastPathStringContains() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.name).contains("lic")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == true)
            #expect(cmp.evaluate(on: bob) == false)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: String.hasPrefix evaluates correctly")
    func fastPathStringHasPrefix() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.name).hasPrefix("Bo")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == false)
            #expect(cmp.evaluate(on: bob) == true)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: String.hasSuffix evaluates correctly")
    func fastPathStringHasSuffix() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.name).hasSuffix("ce")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == true)
            #expect(cmp.evaluate(on: bob) == false)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: IN evaluates correctly")
    func fastPathIn() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.age).in([25, 35, 45])
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == false)
            #expect(cmp.evaluate(on: bob) == true)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: isNil evaluates correctly")
    func fastPathIsNil() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.tag == Optional<String>.self
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == false, "alice has tag='vip', should not be nil")
            #expect(cmp.evaluate(on: bob) == true, "bob has tag=nil, should be nil")
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    @Test("Fast path: isNotNil evaluates correctly")
    func fastPathIsNotNil() {
        let predicate: Predicate<ValueAccessRecord> = \ValueAccessRecord.tag != Optional<String>.self
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: alice) == true)
            #expect(cmp.evaluate(on: bob) == false)
        } else {
            Issue.record("Expected comparison predicate")
        }
    }

    // MARK: - Canonical field-name fallback path

    @Test("Fallback path: == via FieldReader")
    func fallbackEqual() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "age",
            op: .equal,
            value: .int64(30)
        )
        #expect(cmp.evaluate(on: alice) == true)
        #expect(cmp.evaluate(on: bob) == false)
    }

    @Test("Fallback path: != via FieldReader")
    func fallbackNotEqual() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "name",
            op: .notEqual,
            value: .string("Alice")
        )
        #expect(cmp.evaluate(on: alice) == false)
        #expect(cmp.evaluate(on: bob) == true)
    }

    @Test("Fallback path: < via FieldReader")
    func fallbackLessThan() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "age",
            op: .lessThan,
            value: .int64(28)
        )
        #expect(cmp.evaluate(on: alice) == false)
        #expect(cmp.evaluate(on: bob) == true)
    }

    @Test("Fallback path: <= via FieldReader")
    func fallbackLessThanOrEqual() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "age",
            op: .lessThanOrEqual,
            value: .int64(25)
        )
        #expect(cmp.evaluate(on: alice) == false)
        #expect(cmp.evaluate(on: bob) == true)
    }

    @Test("Fallback path: > via FieldReader")
    func fallbackGreaterThan() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "score",
            op: .greaterThan,
            value: .double(90.0)
        )
        #expect(cmp.evaluate(on: alice) == true)
        #expect(cmp.evaluate(on: bob) == false)
    }

    @Test("Fallback path: >= via FieldReader")
    func fallbackGreaterThanOrEqual() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "score",
            op: .greaterThanOrEqual,
            value: .double(80.0)
        )
        #expect(cmp.evaluate(on: alice) == true)
        #expect(cmp.evaluate(on: bob) == true)
    }

    @Test("Fallback path: contains via FieldReader")
    func fallbackContains() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "name",
            op: .contains,
            value: .string("lic")
        )
        #expect(cmp.evaluate(on: alice) == true)
        #expect(cmp.evaluate(on: bob) == false)
    }

    @Test("Fallback path: hasPrefix via FieldReader")
    func fallbackHasPrefix() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "name",
            op: .hasPrefix,
            value: .string("Bo")
        )
        #expect(cmp.evaluate(on: alice) == false)
        #expect(cmp.evaluate(on: bob) == true)
    }

    @Test("Fallback path: hasSuffix via FieldReader")
    func fallbackHasSuffix() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "name",
            op: .hasSuffix,
            value: .string("ce")
        )
        #expect(cmp.evaluate(on: alice) == true)
        #expect(cmp.evaluate(on: bob) == false)
    }

    @Test("Fallback path: IN via FieldReader")
    func fallbackIn() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "age",
            op: .in,
            value: .array([.int64(25), .int64(35)])
        )
        #expect(cmp.evaluate(on: alice) == false)
        #expect(cmp.evaluate(on: bob) == true)
    }

    @Test("Fallback path: isNil via FieldReader with nil optional")
    func fallbackIsNilTrue() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "tag",
            op: .isNil,
            value: .null
        )
        #expect(cmp.evaluate(on: bob) == true, "bob.tag is nil → isNil should be true")
    }

    @Test("Fallback path: isNil via FieldReader with non-nil optional")
    func fallbackIsNilFalse() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "tag",
            op: .isNil,
            value: .null
        )
        #expect(cmp.evaluate(on: alice) == false, "alice.tag is 'vip' → isNil should be false")
    }

    @Test("Fallback path: isNotNil via FieldReader")
    func fallbackIsNotNil() {
        let cmp = FieldComparison<ValueAccessRecord>(
            fieldName: "tag",
            op: .isNotNil,
            value: .null
        )
        #expect(cmp.evaluate(on: alice) == true)
        #expect(cmp.evaluate(on: bob) == false)
    }

    @Test("Fallback path: null field returns false for non-nil operators")
    func fallbackNullFieldReturnsFalse() {
        // bob.tag is nil; comparisons other than isNil/isNotNil should return false
        let ops: [ComparisonOperator] = [.equal, .notEqual, .lessThan, .greaterThan, .contains]
        for op in ops {
            let cmp = FieldComparison<ValueAccessRecord>(
                fieldName: "tag",
                op: op,
                value: .string("anything")
            )
            #expect(cmp.evaluate(on: bob) == false, "null field with op=\(op) should return false")
        }
    }

    // MARK: - Fast path vs Fallback consistency

    @Test("Fast path and fallback produce identical results for all comparison operators")
    func fastPathFallbackConsistency() {
        let item = ValueAccessRecord(name: "Test", age: 30, score: 75.0)

        // == 30
        let fastEq: Predicate<ValueAccessRecord> = \ValueAccessRecord.age == 30
        let fallbackEq = FieldComparison<ValueAccessRecord>(
            fieldName: "age", op: .equal, value: .int64(30)
        )
        if case .comparison(let fast) = fastEq {
            #expect(fast.evaluate(on: item) == fallbackEq.evaluate(on: item), "== consistency")
        }

        // < 28
        let fastLt: Predicate<ValueAccessRecord> = \ValueAccessRecord.age < 28
        let fallbackLt = FieldComparison<ValueAccessRecord>(
            fieldName: "age", op: .lessThan, value: .int64(28)
        )
        if case .comparison(let fast) = fastLt {
            #expect(fast.evaluate(on: item) == fallbackLt.evaluate(on: item), "< consistency")
        }

        // > 28
        let fastGt: Predicate<ValueAccessRecord> = \ValueAccessRecord.age > 28
        let fallbackGt = FieldComparison<ValueAccessRecord>(
            fieldName: "age", op: .greaterThan, value: .int64(28)
        )
        if case .comparison(let fast) = fastGt {
            #expect(fast.evaluate(on: item) == fallbackGt.evaluate(on: item), "> consistency")
        }

        // contains
        let fastC: Predicate<ValueAccessRecord> = (\ValueAccessRecord.name).contains("es")
        let fallbackC = FieldComparison<ValueAccessRecord>(
            fieldName: "name", op: .contains, value: .string("es")
        )
        if case .comparison(let fast) = fastC {
            #expect(fast.evaluate(on: item) == fallbackC.evaluate(on: item), "contains consistency")
        }
    }
}

// MARK: - SortDescriptor.orderedComparison Tests

@Suite("SortDescriptor.orderedComparison Tests", .heartbeat)
struct SortDescriptorOrderedComparisonTests {

    let alice = ValueAccessRecord(name: "Alice", age: 30, score: 95.5)
    let bob = ValueAccessRecord(name: "Bob", age: 25, score: 80.0)
    let charlie = ValueAccessRecord(name: "Charlie", age: 30, score: 95.5)

    // MARK: - Ascending order

    @Test("Ascending: lhs < rhs returns .orderedAscending")
    func ascendingLessThan() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .ascending)
        #expect(sd.orderedComparison(bob, alice) == .orderedAscending)
    }

    @Test("Ascending: lhs > rhs returns .orderedDescending")
    func ascendingGreaterThan() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .ascending)
        #expect(sd.orderedComparison(alice, bob) == .orderedDescending)
    }

    @Test("Ascending: lhs == rhs returns .orderedSame")
    func ascendingEqual() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .ascending)
        #expect(sd.orderedComparison(alice, charlie) == .orderedSame)
    }

    // MARK: - Descending order (flipped)

    @Test("Descending: lhs < rhs returns .orderedDescending (flipped)")
    func descendingLessThan() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .descending)
        #expect(sd.orderedComparison(bob, alice) == .orderedDescending)
    }

    @Test("Descending: lhs > rhs returns .orderedAscending (flipped)")
    func descendingGreaterThan() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .descending)
        #expect(sd.orderedComparison(alice, bob) == .orderedAscending)
    }

    @Test("Descending: lhs == rhs returns .orderedSame")
    func descendingEqual() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .descending)
        #expect(sd.orderedComparison(alice, charlie) == .orderedSame)
    }

    // MARK: - String sorting

    @Test("String sort ascending: alphabetical order")
    func stringSortAscending() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.name, order: .ascending)
        #expect(sd.orderedComparison(alice, bob) == .orderedAscending)
        #expect(sd.orderedComparison(bob, alice) == .orderedDescending)
        #expect(sd.orderedComparison(alice, alice) == .orderedSame)
    }

    // MARK: - Double sorting

    @Test("Double sort ascending")
    func doubleSortAscending() {
        let sd = SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.score, order: .ascending)
        #expect(sd.orderedComparison(bob, alice) == .orderedAscending) // 80 < 95.5
        #expect(sd.orderedComparison(alice, bob) == .orderedDescending)
    }

    // MARK: - Multi-descriptor sort

    @Test("Multi-descriptor sort: primary then secondary")
    func multiDescriptorSort() {
        let items = [
            ValueAccessRecord(name: "Charlie", age: 30, score: 70.0),
            ValueAccessRecord(name: "Alice", age: 25, score: 90.0),
            ValueAccessRecord(name: "Bob", age: 30, score: 85.0),
            ValueAccessRecord(name: "Diana", age: 25, score: 60.0),
        ]

        let descriptors = [
            SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.age, order: .ascending),
            SortDescriptor<ValueAccessRecord>(keyPath: \ValueAccessRecord.name, order: .ascending),
        ]

        let sorted = items.sorted { lhs, rhs in
            for descriptor in descriptors {
                let result = descriptor.orderedComparison(lhs, rhs)
                if result != .orderedSame {
                    return result == .orderedAscending
                }
            }
            return false
        }

        // age=25: Alice, Diana; age=30: Bob, Charlie
        #expect(sorted.map(\.name) == ["Alice", "Diana", "Bob", "Charlie"])
    }
}

// MARK: - SortDescriptor Fallback Path Tests (compareViaFieldReader)

@Suite("SortDescriptor Fallback Path Tests", .heartbeat)
struct SortDescriptorFallbackTests {

    /// Test null handling through FieldReader directly,
    /// which is what compareViaFieldReader delegates to.
    @Test("FieldReader: nil optional returns null FieldValue")
    func fieldReaderNilOptional() {
        let noTag = ValueAccessRecord(name: "A", tag: nil)
        let withTag = ValueAccessRecord(name: "B", tag: "vip")

        let nullFV = FieldReader.readFieldValue(from: noTag, fieldName: "tag")
        let nonNullFV = FieldReader.readFieldValue(from: withTag, fieldName: "tag")

        #expect(nullFV == .null)
        #expect(nonNullFV == .string("vip"))
    }

    @Test("FieldValue comparison: null vs non-null ordering")
    func fieldValueNullOrdering() {
        // null < non-null in ascending (null-first semantics)
        let nullFV = FieldValue.null
        let valueFV = FieldValue.string("vip")

        // .null is less than .string in FieldValue's natural ordering
        #expect(nullFV < valueFV)
    }

    @Test("FieldReader consistency: PartialKeyPath and fieldName return same values")
    func fieldReaderConsistency() {
        let item = ValueAccessRecord(name: "Alice", age: 30, score: 95.5, tag: "vip")

        // Via keyPath
        let viaKP = FieldReader.read(from: item, keyPath: \ValueAccessRecord.age as AnyKeyPath, fieldName: "age")
        // Via fieldName only
        let viaName = FieldReader.read(from: item, fieldName: "age")

        #expect(viaKP as? Int == 30)
        #expect(viaName as? Int == 30)
    }
}

// MARK: - Optional String Extension Tests

@Suite("Optional String Predicate Tests", .heartbeat)
struct OptionalStringPredicateTests {

    let withTag = ValueAccessRecord(name: "Alice", tag: "premium_user")
    let noTag = ValueAccessRecord(name: "Bob", tag: nil)

    @Test("Optional String contains: non-nil matches")
    func optionalContainsNonNil() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.tag).contains("premium")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: withTag) == true)
        } else {
            Issue.record("Expected comparison")
        }
    }

    @Test("Optional String contains: nil returns false")
    func optionalContainsNil() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.tag).contains("premium")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: noTag) == false)
        } else {
            Issue.record("Expected comparison")
        }
    }

    @Test("Optional String hasPrefix: non-nil matches")
    func optionalHasPrefixNonNil() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.tag).hasPrefix("prem")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: withTag) == true)
        } else {
            Issue.record("Expected comparison")
        }
    }

    @Test("Optional String hasPrefix: nil returns false")
    func optionalHasPrefixNil() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.tag).hasPrefix("prem")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: noTag) == false)
        } else {
            Issue.record("Expected comparison")
        }
    }

    @Test("Optional String hasSuffix: non-nil matches")
    func optionalHasSuffixNonNil() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.tag).hasSuffix("user")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: withTag) == true)
        } else {
            Issue.record("Expected comparison")
        }
    }

    @Test("Optional String hasSuffix: nil returns false")
    func optionalHasSuffixNil() {
        let predicate: Predicate<ValueAccessRecord> = (\ValueAccessRecord.tag).hasSuffix("user")
        if case .comparison(let cmp) = predicate {
            #expect(cmp.evaluate(on: noTag) == false)
        } else {
            Issue.record("Expected comparison")
        }
    }
}
#endif

#endif
