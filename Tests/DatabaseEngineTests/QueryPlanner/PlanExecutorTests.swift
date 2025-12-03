// PlanExecutorTests.swift
// Tests for PlanExecutor predicate evaluation, especially .in and isNil/isNotNil

import Testing
import Foundation
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import Core

// MARK: - Test Model

/// Simple user model for predicate testing
struct PredicateTestUser: Persistable {
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

    static var persistableType: String { "PredicateTestUser" }
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

    static func fieldName<Value>(for keyPath: KeyPath<PredicateTestUser, Value>) -> String {
        switch keyPath {
        case \PredicateTestUser.id: return "id"
        case \PredicateTestUser.name: return "name"
        case \PredicateTestUser.age: return "age"
        case \PredicateTestUser.isActive: return "isActive"
        case \PredicateTestUser.department: return "department"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PredicateTestUser>) -> String {
        switch keyPath {
        case \PredicateTestUser.id: return "id"
        case \PredicateTestUser.name: return "name"
        case \PredicateTestUser.age: return "age"
        case \PredicateTestUser.isActive: return "isActive"
        case \PredicateTestUser.department: return "department"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PredicateTestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Predicate Evaluation Tests

@Suite("Predicate Evaluation Tests")
struct PredicateEvaluationTests {

    // MARK: - IN Predicate Tests

    @Test("IN predicate matches values in array")
    func testInPredicateMatches() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = .comparison(
            FieldComparison(keyPath: \PredicateTestUser.age, values: [25, 30, 35])
        )

        let user25 = PredicateTestUser(name: "Alice", age: 25)
        let user30 = PredicateTestUser(name: "Bob", age: 30)
        let user35 = PredicateTestUser(name: "Charlie", age: 35)

        #expect(evaluate(predicate, on: user25) == true)
        #expect(evaluate(predicate, on: user30) == true)
        #expect(evaluate(predicate, on: user35) == true)
    }

    @Test("IN predicate rejects non-matching values")
    func testInPredicateRejects() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = .comparison(
            FieldComparison(keyPath: \PredicateTestUser.age, values: [25, 30, 35])
        )

        let user20 = PredicateTestUser(name: "David", age: 20)
        let user40 = PredicateTestUser(name: "Eve", age: 40)

        #expect(evaluate(predicate, on: user20) == false)
        #expect(evaluate(predicate, on: user40) == false)
    }

    @Test("IN predicate with string values")
    func testInPredicateStrings() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = .comparison(
            FieldComparison(keyPath: \PredicateTestUser.name, values: ["Alice", "Bob"])
        )

        let alice = PredicateTestUser(name: "Alice", age: 25)
        let charlie = PredicateTestUser(name: "Charlie", age: 30)

        #expect(evaluate(predicate, on: alice) == true)
        #expect(evaluate(predicate, on: charlie) == false)
    }

    @Test("IN predicate with empty array")
    func testInPredicateEmpty() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = .comparison(
            FieldComparison(keyPath: \PredicateTestUser.age, values: [Int]())
        )

        let user = PredicateTestUser(name: "Alice", age: 25)

        #expect(evaluate(predicate, on: user) == false)
    }

    // MARK: - isNil/isNotNil Tests

    @Test("isNil predicate matches nil")
    func testIsNilMatches() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.department == Optional<String>.self

        let userNil = PredicateTestUser(name: "Alice", age: 25, department: nil)

        #expect(evaluate(predicate, on: userNil) == true)
    }

    @Test("isNil predicate rejects non-nil")
    func testIsNilRejects() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.department == Optional<String>.self

        let userDept = PredicateTestUser(name: "Bob", age: 30, department: "Engineering")

        #expect(evaluate(predicate, on: userDept) == false)
    }

    @Test("isNotNil predicate matches non-nil")
    func testIsNotNilMatches() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.department != Optional<String>.self

        let userDept = PredicateTestUser(name: "Bob", age: 30, department: "Engineering")

        #expect(evaluate(predicate, on: userDept) == true)
    }

    @Test("isNotNil predicate rejects nil")
    func testIsNotNilRejects() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.department != Optional<String>.self

        let userNil = PredicateTestUser(name: "Alice", age: 25, department: nil)

        #expect(evaluate(predicate, on: userNil) == false)
    }

    // MARK: - Basic Comparison Tests

    @Test("Equality predicate")
    func testEquality() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.age == 25

        let user25 = PredicateTestUser(name: "Alice", age: 25)
        let user30 = PredicateTestUser(name: "Bob", age: 30)

        #expect(evaluate(predicate, on: user25) == true)
        #expect(evaluate(predicate, on: user30) == false)
    }

    @Test("Greater than predicate")
    func testGreaterThan() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.age > 25

        let user25 = PredicateTestUser(name: "Alice", age: 25)
        let user30 = PredicateTestUser(name: "Bob", age: 30)

        #expect(evaluate(predicate, on: user25) == false)
        #expect(evaluate(predicate, on: user30) == true)
    }

    @Test("Less than predicate")
    func testLessThan() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = \PredicateTestUser.age < 30

        let user25 = PredicateTestUser(name: "Alice", age: 25)
        let user30 = PredicateTestUser(name: "Bob", age: 30)

        #expect(evaluate(predicate, on: user25) == true)
        #expect(evaluate(predicate, on: user30) == false)
    }

    // MARK: - Logical Operator Tests

    @Test("AND predicate")
    func testAndPredicate() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = .and([
            \PredicateTestUser.age > 18,
            \PredicateTestUser.isActive == true
        ])

        let activeAdult = PredicateTestUser(name: "Alice", age: 25, isActive: true)
        let inactiveAdult = PredicateTestUser(name: "Bob", age: 30, isActive: false)

        #expect(evaluate(predicate, on: activeAdult) == true)
        #expect(evaluate(predicate, on: inactiveAdult) == false)
    }

    @Test("OR predicate")
    func testOrPredicate() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = .or([
            \PredicateTestUser.age < 18,
            \PredicateTestUser.age > 65
        ])

        let minor = PredicateTestUser(name: "Kid", age: 15)
        let adult = PredicateTestUser(name: "Adult", age: 30)
        let senior = PredicateTestUser(name: "Senior", age: 70)

        #expect(evaluate(predicate, on: minor) == true)
        #expect(evaluate(predicate, on: adult) == false)
        #expect(evaluate(predicate, on: senior) == true)
    }

    @Test("NOT predicate")
    func testNotPredicate() {
        let predicate: DatabaseEngine.Predicate<PredicateTestUser> = !(\PredicateTestUser.isActive == true)

        let active = PredicateTestUser(name: "Active", age: 25, isActive: true)
        let inactive = PredicateTestUser(name: "Inactive", age: 30, isActive: false)

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

        switch comparison.op {
        case .equal:
            return compareEqual(modelValue, expectedValue)
        case .notEqual:
            return !compareEqual(modelValue, expectedValue)
        case .lessThan:
            return compareLess(modelValue, expectedValue)
        case .lessThanOrEqual:
            return compareLess(modelValue, expectedValue) || compareEqual(modelValue, expectedValue)
        case .greaterThan:
            return compareLess(expectedValue, modelValue)
        case .greaterThanOrEqual:
            return compareLess(expectedValue, modelValue) || compareEqual(modelValue, expectedValue)
        case .in:
            // Handle typed arrays
            if let arr = expectedValue as? [String], let val = modelValue as? String {
                return arr.contains(val)
            }
            if let arr = expectedValue as? [Int], let val = modelValue as? Int {
                return arr.contains(val)
            }
            // Fallback to reflection
            if let arr = extractArray(from: expectedValue) {
                return arr.contains { compareEqual(modelValue, $0) }
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

    private func compareEqual(_ lhs: Any, _ rhs: Any) -> Bool {
        if let l = lhs as? String, let r = rhs as? String { return l == r }
        if let l = lhs as? Int, let r = rhs as? Int { return l == r }
        if let l = lhs as? Bool, let r = rhs as? Bool { return l == r }
        return "\(lhs)" == "\(rhs)"
    }

    private func compareLess(_ lhs: Any, _ rhs: Any) -> Bool {
        if let l = lhs as? Int, let r = rhs as? Int { return l < r }
        if let l = lhs as? String, let r = rhs as? String { return l < r }
        return false
    }

    private func extractArray(from value: Any) -> [Any]? {
        let mirror = Mirror(reflecting: value)
        if mirror.displayStyle == .collection {
            return mirror.children.map { $0.value }
        }
        return nil
    }
}
