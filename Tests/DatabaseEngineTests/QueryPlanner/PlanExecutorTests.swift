#if !os(WASI)
#if FOUNDATION_DB
// PlanExecutorTests.swift
// Tests for PlanExecutor predicate evaluation, especially .in and isNil/isNotNil

import Testing
import TestHeartbeat
import Foundation
@testable import DatabaseEngine
import DatabaseValue
@testable import ScalarIndex
@testable import Core

// MARK: - Test Model

/// Simple user model for predicate testing
struct PredicateUser: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var age: Int
    var isActive: Bool
    var department: String?

    init(
        id: String = UUID().uuidString,
        name: String,
        age: Int,
        isActive: Bool = true,
        department: String? = nil
    ) {
        self.id = id
        self.name = name
        self.age = age
        self.isActive = isActive
        self.department = department
    }

    static var persistableType: String { "PredicateUser" }
    static var allFields: [String] { ["id", "name", "age", "isActive", "department"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "age": return age
        case "isActive": return isActive
        case "department": return department
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<PredicateUser, Value>) -> String {
        switch keyPath {
        case \PredicateUser.id: return "id"
        case \PredicateUser.name: return "name"
        case \PredicateUser.age: return "age"
        case \PredicateUser.isActive: return "isActive"
        case \PredicateUser.department: return "department"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PredicateUser>) -> String {
        switch keyPath {
        case \PredicateUser.id: return "id"
        case \PredicateUser.name: return "name"
        case \PredicateUser.age: return "age"
        case \PredicateUser.isActive: return "isActive"
        case \PredicateUser.department: return "department"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PredicateUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Predicate Evaluation Tests

@Suite("Predicate Evaluation Tests", .heartbeat)
struct PredicateEvaluationTests {

    // MARK: - IN Predicate Tests

    @Test("IN predicate matches values in array")
    func testInPredicateMatches() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = .comparison(
            FieldComparison(keyPath: \PredicateUser.age, values: [25, 30, 35])
        )

        let user25 = PredicateUser(name: "Alice", age: 25)
        let user30 = PredicateUser(name: "Bob", age: 30)
        let user35 = PredicateUser(name: "Charlie", age: 35)

        #expect(evaluate(predicate, on: user25) == true)
        #expect(evaluate(predicate, on: user30) == true)
        #expect(evaluate(predicate, on: user35) == true)
    }

    @Test("IN predicate rejects non-matching values")
    func testInPredicateRejects() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = .comparison(
            FieldComparison(keyPath: \PredicateUser.age, values: [25, 30, 35])
        )

        let user20 = PredicateUser(name: "David", age: 20)
        let user40 = PredicateUser(name: "Eve", age: 40)

        #expect(evaluate(predicate, on: user20) == false)
        #expect(evaluate(predicate, on: user40) == false)
    }

    @Test("IN predicate with string values")
    func testInPredicateStrings() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = .comparison(
            FieldComparison(keyPath: \PredicateUser.name, values: ["Alice", "Bob"])
        )

        let alice = PredicateUser(name: "Alice", age: 25)
        let charlie = PredicateUser(name: "Charlie", age: 30)

        #expect(evaluate(predicate, on: alice) == true)
        #expect(evaluate(predicate, on: charlie) == false)
    }

    @Test("IN predicate with empty array")
    func testInPredicateEmpty() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = .comparison(
            FieldComparison(keyPath: \PredicateUser.age, values: [Int]())
        )

        let user = PredicateUser(name: "Alice", age: 25)

        #expect(evaluate(predicate, on: user) == false)
    }

    // MARK: - isNil/isNotNil Tests

    @Test("isNil predicate matches nil")
    func testIsNilMatches() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.department == Optional<String>.self

        let userNil = PredicateUser(name: "Alice", age: 25, department: nil)

        #expect(evaluate(predicate, on: userNil) == true)
    }

    @Test("isNil predicate rejects non-nil")
    func testIsNilRejects() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.department == Optional<String>.self

        let userDept = PredicateUser(name: "Bob", age: 30, department: "Engineering")

        #expect(evaluate(predicate, on: userDept) == false)
    }

    @Test("isNotNil predicate matches non-nil")
    func testIsNotNilMatches() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.department != Optional<String>.self

        let userDept = PredicateUser(name: "Bob", age: 30, department: "Engineering")

        #expect(evaluate(predicate, on: userDept) == true)
    }

    @Test("isNotNil predicate rejects nil")
    func testIsNotNilRejects() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.department != Optional<String>.self

        let userNil = PredicateUser(name: "Alice", age: 25, department: nil)

        #expect(evaluate(predicate, on: userNil) == false)
    }

    // MARK: - Basic Comparison Tests

    @Test("Equality predicate")
    func testEquality() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.age == 25

        let user25 = PredicateUser(name: "Alice", age: 25)
        let user30 = PredicateUser(name: "Bob", age: 30)

        #expect(evaluate(predicate, on: user25) == true)
        #expect(evaluate(predicate, on: user30) == false)
    }

    @Test("Greater than predicate")
    func testGreaterThan() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.age > 25

        let user25 = PredicateUser(name: "Alice", age: 25)
        let user30 = PredicateUser(name: "Bob", age: 30)

        #expect(evaluate(predicate, on: user25) == false)
        #expect(evaluate(predicate, on: user30) == true)
    }

    @Test("Less than predicate")
    func testLessThan() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = \PredicateUser.age < 30

        let user25 = PredicateUser(name: "Alice", age: 25)
        let user30 = PredicateUser(name: "Bob", age: 30)

        #expect(evaluate(predicate, on: user25) == true)
        #expect(evaluate(predicate, on: user30) == false)
    }

    // MARK: - Logical Operator Tests

    @Test("AND predicate")
    func testAndPredicate() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = .and([
            \PredicateUser.age > 18,
            \PredicateUser.isActive == true
        ])

        let activeAdult = PredicateUser(name: "Alice", age: 25, isActive: true)
        let inactiveAdult = PredicateUser(name: "Bob", age: 30, isActive: false)

        #expect(evaluate(predicate, on: activeAdult) == true)
        #expect(evaluate(predicate, on: inactiveAdult) == false)
    }

    @Test("OR predicate")
    func testOrPredicate() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = .or([
            \PredicateUser.age < 18,
            \PredicateUser.age > 65
        ])

        let minor = PredicateUser(name: "Kid", age: 15)
        let adult = PredicateUser(name: "Adult", age: 30)
        let senior = PredicateUser(name: "Senior", age: 70)

        #expect(evaluate(predicate, on: minor) == true)
        #expect(evaluate(predicate, on: adult) == false)
        #expect(evaluate(predicate, on: senior) == true)
    }

    @Test("NOT predicate")
    func testNotPredicate() {
        let predicate: DatabaseEngine.Predicate<PredicateUser> = !(\PredicateUser.isActive == true)

        let active = PredicateUser(name: "Active", age: 25, isActive: true)
        let inactive = PredicateUser(name: "Inactive", age: 30, isActive: false)

        #expect(evaluate(predicate, on: active) == false)
        #expect(evaluate(predicate, on: inactive) == true)
    }

    // MARK: - Helper

    private func evaluate<T: Persistable>(_ predicate: DatabaseEngine.Predicate<T>, on model: T) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return evaluateComparison(comparison, on: model)
        case .and(let predicates):
            return predicates.allSatisfy { evaluate($0, on: model) }
        case .or(let predicates):
            return predicates.contains { evaluate($0, on: model) }
        case .not(let inner):
            return !evaluate(inner, on: model)
        case .true:
            return true
        case .false:
            return false
        }
    }

    private func evaluateComparison<T: Persistable>(_ comparison: FieldComparison<T>, on model: T) -> Bool {
        let fieldName = comparison.fieldName
        let modelValue = model[dynamicMember: fieldName]

        // Handle nil checks
        switch comparison.op {
        case .isNil:
            return modelValue == nil || isNilValue(modelValue!)
        case .isNotNil:
            guard let value = modelValue else { return false }
            return !isNilValue(value)
        default:
            break
        }

        guard let modelValue = modelValue else { return false }
        let expectedValue = comparison.value

        // Convert model value to FieldValue for comparison
        let modelFieldValue = toFieldValue(modelValue)

        switch comparison.op {
        case .equal:
            return modelFieldValue.isEqual(to: expectedValue)
        case .notEqual:
            return !modelFieldValue.isEqual(to: expectedValue)
        case .lessThan:
            return modelFieldValue.isLessThan(expectedValue)
        case .lessThanOrEqual:
            return modelFieldValue.isLessThan(expectedValue) || modelFieldValue.isEqual(to: expectedValue)
        case .greaterThan:
            return expectedValue.isLessThan(modelFieldValue)
        case .greaterThanOrEqual:
            return expectedValue.isLessThan(modelFieldValue) || modelFieldValue.isEqual(to: expectedValue)
        case .in:
            // Check if model value is in the expected array
            if let arrayValues = expectedValue.arrayValue {
                return arrayValues.contains { modelFieldValue.isEqual(to: $0) }
            }
            return false
        default:
            return false
        }
    }

    private func isNilValue(_ value: Any) -> Bool {
        let mirror = Mirror(reflecting: value)
        return mirror.displayStyle == .optional && mirror.children.isEmpty
    }

    private func toFieldValue(_ value: Any) -> FieldValue {
        switch value {
        case let v as Bool: return .bool(v)
        case let v as Int: return .int64(Int64(v))
        case let v as Int64: return .int64(v)
        case let v as Double: return .double(v)
        case let v as String: return .string(v)
        default:
            return .string(String(describing: value))
        }
    }
}
#endif

#endif
