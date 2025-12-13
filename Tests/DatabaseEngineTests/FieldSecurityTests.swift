// FieldSecurityTests.swift
// Tests for field-level security functionality

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

// MARK: - Test Models

/// Employee with restricted salary and SSN fields
///
/// Uses @Restricted property wrapper for field-level security.
/// Access levels are preserved through custom init.
private struct Employee: Persistable {
    typealias ID = String

    var id: String
    var name: String

    /// Salary - only HR and managers can read/write
    @Restricted(read: .roles(["hr", "manager"]), write: .roles(["hr"]))
    var salary: Double = 0

    /// SSN - only HR can read, no one can write (except initially)
    @Restricted(read: .roles(["hr"]), write: .roles(["hr"]))
    var ssn: String = ""

    /// Department - anyone can read, only admin can write
    @Restricted(write: .roles(["admin"]))
    var department: String = ""

    /// Internal notes - only authenticated users can read
    @Restricted(read: .authenticated)
    var internalNotes: String = ""

    /// Custom init that preserves access levels from property wrapper declarations
    init(
        id: String = UUID().uuidString,
        name: String = "",
        salary: Double = 0,
        ssn: String = "",
        department: String = "",
        internalNotes: String = ""
    ) {
        self.id = id
        self.name = name
        // Explicitly create Restricted with proper access levels
        self._salary = Restricted(wrappedValue: salary, read: .roles(["hr", "manager"]), write: .roles(["hr"]))
        self._ssn = Restricted(wrappedValue: ssn, read: .roles(["hr"]), write: .roles(["hr"]))
        self._department = Restricted(wrappedValue: department, read: .public, write: .roles(["admin"]))
        self._internalNotes = Restricted(wrappedValue: internalNotes, read: .authenticated, write: .public)
    }

    static var persistableType: String { "Employee" }
    static var allFields: [String] { ["id", "name", "salary", "ssn", "department", "internalNotes"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "salary": return salary
        case "ssn": return ssn
        case "department": return department
        case "internalNotes": return internalNotes
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<Employee, Value>) -> String {
        switch keyPath {
        case \Employee.id: return "id"
        case \Employee.name: return "name"
        case \Employee.salary: return "salary"
        case \Employee.ssn: return "ssn"
        case \Employee.department: return "department"
        case \Employee.internalNotes: return "internalNotes"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<Employee>) -> String {
        switch keyPath {
        case \Employee.id: return "id"
        case \Employee.name: return "name"
        case \Employee.salary: return "salary"
        case \Employee.ssn: return "ssn"
        case \Employee.department: return "department"
        case \Employee.internalNotes: return "internalNotes"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<Employee> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

/// Simple test auth context
private struct TestAuth: AuthContext {
    let userID: String
    var roles: Set<String>

    init(userID: String, roles: Set<String> = []) {
        self.userID = userID
        self.roles = roles
    }
}

// MARK: - FieldAccessLevel Tests

@Suite("FieldAccessLevel")
struct FieldAccessLevelTests {

    @Test("Public access allows everyone")
    func publicAccessAllowsEveryone() {
        let level = FieldAccessLevel.public

        // Unauthenticated
        #expect(level.evaluate(auth: nil) == true)

        // Authenticated without roles
        #expect(level.evaluate(auth: TestAuth(userID: "user1")) == true)

        // Authenticated with roles
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["admin"])) == true)
    }

    @Test("Authenticated access requires auth")
    func authenticatedAccessRequiresAuth() {
        let level = FieldAccessLevel.authenticated

        // Unauthenticated
        #expect(level.evaluate(auth: nil) == false)

        // Authenticated
        #expect(level.evaluate(auth: TestAuth(userID: "user1")) == true)
    }

    @Test("Role-based access checks roles")
    func roleBasedAccessChecksRoles() {
        let level = FieldAccessLevel.roles(["hr", "manager"])

        // Unauthenticated
        #expect(level.evaluate(auth: nil) == false)

        // Authenticated without required roles
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["employee"])) == false)

        // Authenticated with one required role
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["hr"])) == true)
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["manager"])) == true)

        // Authenticated with multiple roles including required
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["employee", "hr"])) == true)
    }

    @Test("Custom access uses predicate")
    func customAccessUsesPredicate() {
        let level = FieldAccessLevel.custom { auth in
            auth.userID.hasPrefix("admin_")
        }

        // Unauthenticated
        #expect(level.evaluate(auth: nil) == false)

        // Not matching predicate
        #expect(level.evaluate(auth: TestAuth(userID: "user1")) == false)

        // Matching predicate
        #expect(level.evaluate(auth: TestAuth(userID: "admin_1")) == true)
    }

    @Test("FieldAccessLevel equality")
    func fieldAccessLevelEquality() {
        #expect(FieldAccessLevel.public == FieldAccessLevel.public)
        #expect(FieldAccessLevel.authenticated == FieldAccessLevel.authenticated)
        #expect(FieldAccessLevel.roles(["a", "b"]) == FieldAccessLevel.roles(["a", "b"]))
        #expect(FieldAccessLevel.roles(["a"]) != FieldAccessLevel.roles(["b"]))

        // Custom closures cannot be compared
        let custom1 = FieldAccessLevel.custom { _ in true }
        let custom2 = FieldAccessLevel.custom { _ in true }
        #expect(custom1 != custom2)
    }
}

// MARK: - Restricted Property Wrapper Tests

@Suite("Restricted Property Wrapper")
struct RestrictedPropertyWrapperTests {

    @Test("Restricted wraps value correctly")
    func restrictedWrapsValue() {
        var restricted = Restricted(wrappedValue: 100.0, read: .roles(["hr"]), write: .roles(["admin"]))

        #expect(restricted.wrappedValue == 100.0)
        #expect(restricted.readAccess == .roles(["hr"]))
        #expect(restricted.writeAccess == .roles(["admin"]))

        // Can modify wrapped value
        restricted.wrappedValue = 200.0
        #expect(restricted.wrappedValue == 200.0)
    }

    @Test("Restricted conforms to RestrictedProtocol")
    func restrictedConformsToProtocol() {
        let restricted: any RestrictedProtocol = Restricted(
            wrappedValue: "secret",
            read: .authenticated,
            write: .roles(["admin"])
        )

        #expect(restricted.readAccess == .authenticated)
        #expect(restricted.writeAccess == .roles(["admin"]))
        #expect(restricted.anyValue as? String == "secret")
    }

    @Test("Restricted Codable encodes only value")
    func restrictedCodableEncodesOnlyValue() throws {
        let restricted = Restricted(
            wrappedValue: 42,
            read: .roles(["hr"]),
            write: .roles(["admin"])
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(restricted)
        let json = String(data: data, encoding: .utf8)

        // Should encode just the value, not access levels
        #expect(json == "42")

        // Decode back
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(Restricted<Int>.self, from: data)
        #expect(decoded.wrappedValue == 42)
        // Access levels are not encoded, so they default to .public
        #expect(decoded.readAccess == .public)
        #expect(decoded.writeAccess == .public)
    }
}

// MARK: - FieldSecurityEvaluator Tests

@Suite("FieldSecurityEvaluator")
struct FieldSecurityEvaluatorTests {

    @Test("Extract restricted fields from model")
    func extractRestrictedFields() {
        let employee = Employee(
            name: "Alice",
            salary: 100000,
            ssn: "123-45-6789",
            department: "Engineering",
            internalNotes: "Good performer"
        )

        let restrictions = FieldSecurityEvaluator.extractRestrictedFields(from: employee)

        // Should find 4 restricted fields
        #expect(restrictions.count == 4)
        #expect(restrictions["salary"] != nil)
        #expect(restrictions["ssn"] != nil)
        #expect(restrictions["department"] != nil)
        #expect(restrictions["internalNotes"] != nil)

        // Check specific restrictions
        #expect(restrictions["salary"]?.readAccess == .roles(["hr", "manager"]))
        #expect(restrictions["salary"]?.writeAccess == .roles(["hr"]))
        #expect(restrictions["ssn"]?.readAccess == .roles(["hr"]))
        #expect(restrictions["department"]?.writeAccess == .roles(["admin"]))
        #expect(restrictions["internalNotes"]?.readAccess == .authenticated)
    }

    @Test("canRead evaluates correctly")
    func canReadEvaluates() {
        let employee = Employee(
            name: "Alice",
            salary: 100000,
            ssn: "123-45-6789"
        )

        // Unauthenticated user
        #expect(FieldSecurityEvaluator.canRead(field: "name", in: employee, auth: nil) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: nil) == false)
        #expect(FieldSecurityEvaluator.canRead(field: "internalNotes", in: employee, auth: nil) == false)

        // Regular employee
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])
        #expect(FieldSecurityEvaluator.canRead(field: "name", in: employee, auth: employee_auth) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: employee_auth) == false)
        #expect(FieldSecurityEvaluator.canRead(field: "internalNotes", in: employee, auth: employee_auth) == true)

        // HR user
        let hr_auth = TestAuth(userID: "hr1", roles: ["hr"])
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: hr_auth) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "ssn", in: employee, auth: hr_auth) == true)

        // Manager
        let manager_auth = TestAuth(userID: "mgr1", roles: ["manager"])
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: manager_auth) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "ssn", in: employee, auth: manager_auth) == false)
    }

    @Test("canWrite evaluates correctly")
    func canWriteEvaluates() {
        let employee = Employee(name: "Alice")

        // Regular employee - cannot write restricted fields
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])
        #expect(FieldSecurityEvaluator.canWrite(field: "name", in: employee, auth: employee_auth) == true)
        #expect(FieldSecurityEvaluator.canWrite(field: "salary", in: employee, auth: employee_auth) == false)
        #expect(FieldSecurityEvaluator.canWrite(field: "department", in: employee, auth: employee_auth) == false)

        // HR - can write salary
        let hr_auth = TestAuth(userID: "hr1", roles: ["hr"])
        #expect(FieldSecurityEvaluator.canWrite(field: "salary", in: employee, auth: hr_auth) == true)

        // Admin - can write department
        let admin_auth = TestAuth(userID: "admin1", roles: ["admin"])
        #expect(FieldSecurityEvaluator.canWrite(field: "department", in: employee, auth: admin_auth) == true)
    }

    @Test("unreadableFields returns correct list")
    func unreadableFieldsReturnsCorrectList() {
        let employee = Employee(
            name: "Alice",
            salary: 100000,
            ssn: "123-45-6789",
            department: "Engineering",
            internalNotes: "Notes"
        )

        // Unauthenticated
        let unreadable_nil = FieldSecurityEvaluator.unreadableFields(in: employee, auth: nil)
        #expect(unreadable_nil.contains("salary"))
        #expect(unreadable_nil.contains("ssn"))
        #expect(unreadable_nil.contains("internalNotes"))
        #expect(!unreadable_nil.contains("department")) // department is public to read

        // Regular employee
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])
        let unreadable_employee = FieldSecurityEvaluator.unreadableFields(in: employee, auth: employee_auth)
        #expect(unreadable_employee.contains("salary"))
        #expect(unreadable_employee.contains("ssn"))
        #expect(!unreadable_employee.contains("internalNotes")) // authenticated can read
    }

    @Test("unwritableFields returns correct list")
    func unwritableFieldsReturnsCorrectList() {
        let employee = Employee(name: "Alice")

        // Regular employee
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])
        let unwritable = FieldSecurityEvaluator.unwritableFields(in: employee, auth: employee_auth)
        #expect(unwritable.contains("salary"))
        #expect(unwritable.contains("ssn"))
        #expect(unwritable.contains("department"))
    }

    @Test("validateWrite throws for unauthorized field changes")
    func validateWriteThrowsForUnauthorizedChanges() {
        let original = Employee(name: "Alice", salary: 50000)
        var updated = original
        updated.salary = 100000 // Attempt to change salary

        // Regular employee cannot change salary
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])

        #expect(throws: FieldSecurityError.self) {
            try FieldSecurityEvaluator.validateWrite(
                original: original,
                updated: updated,
                auth: employee_auth
            )
        }
    }

    @Test("validateWrite allows authorized field changes")
    func validateWriteAllowsAuthorizedChanges() throws {
        let original = Employee(name: "Alice", salary: 50000)
        var updated = original
        updated.salary = 100000 // HR can change salary

        // HR user can change salary
        let hr_auth = TestAuth(userID: "hr1", roles: ["hr"])

        // Should not throw
        try FieldSecurityEvaluator.validateWrite(
            original: original,
            updated: updated,
            auth: hr_auth
        )
    }

    @Test("validateWrite allows changes to unrestricted fields")
    func validateWriteAllowsUnrestrictedChanges() throws {
        let original = Employee(name: "Alice")
        var updated = original
        updated.name = "Alice Smith" // Name is not restricted

        // Anyone can change name
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])

        try FieldSecurityEvaluator.validateWrite(
            original: original,
            updated: updated,
            auth: employee_auth
        )
    }

    @Test("validateWrite for new insert checks non-default values")
    func validateWriteForNewInsertChecksNonDefaults() {
        // New employee with salary set (HR only can set salary)
        let newEmployee = Employee(name: "Bob", salary: 75000)

        // Regular employee cannot insert with salary
        let employee_auth = TestAuth(userID: "user1", roles: ["employee"])

        #expect(throws: FieldSecurityError.self) {
            try FieldSecurityEvaluator.validateWrite(
                original: nil,
                updated: newEmployee,
                auth: employee_auth
            )
        }
    }
}

// MARK: - FieldSecurityError Tests

@Suite("FieldSecurityError")
struct FieldSecurityErrorTests {

    @Test("Error contains type and field info")
    func errorContainsTypeAndFieldInfo() {
        let error = FieldSecurityError.writeNotAllowed(type: "Employee", fields: ["salary", "ssn"])

        #expect(error.description.contains("Employee"))
        #expect(error.description.contains("salary"))
        #expect(error.description.contains("ssn"))
    }

    @Test("Error is equatable")
    func errorIsEquatable() {
        let error1 = FieldSecurityError.writeNotAllowed(type: "Employee", fields: ["salary"])
        let error2 = FieldSecurityError.writeNotAllowed(type: "Employee", fields: ["salary"])
        let error3 = FieldSecurityError.writeNotAllowed(type: "Employee", fields: ["ssn"])

        #expect(error1 == error2)
        #expect(error1 != error3)
    }
}
