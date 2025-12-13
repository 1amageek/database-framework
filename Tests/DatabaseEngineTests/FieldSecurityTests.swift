// FieldSecurityTests.swift
// Tests for field-level security functionality

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

// MARK: - Test Models Using @Persistable Macro

/// Employee with restricted salary and SSN fields
/// Uses @Persistable macro - access levels are stored as static metadata
@Persistable
struct SecureEmployee {
    var name: String = ""

    /// Salary - only HR and managers can read/write
    @Restricted(read: .roles(["hr", "manager"]), write: .roles(["hr"]))
    var salary: Double = 0

    /// SSN - only HR can read/write
    @Restricted(read: .roles(["hr"]), write: .roles(["hr"]))
    var ssn: String = ""

    /// Department - anyone can read, only admin can write
    @Restricted(write: .roles(["admin"]))
    var department: String = ""

    /// Internal notes - only authenticated users can read
    @Restricted(read: .authenticated)
    var internalNotes: String = ""
}

/// Simple model without restrictions for comparison
@Persistable
struct PublicProfile {
    var name: String = ""
    var bio: String = ""
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

// MARK: - Static Metadata Tests (Key Fix Verification)

@Suite("Static Metadata Generation")
struct StaticMetadataTests {

    @Test("@Persistable generates restrictedFieldsMetadata")
    func persistableGeneratesMetadata() {
        // This is the key test - metadata should be static, not instance-based
        let metadata = SecureEmployee.restrictedFieldsMetadata

        #expect(metadata.count == 4)

        // Find salary metadata
        let salaryMeta = metadata.first { $0.fieldName == "salary" }
        #expect(salaryMeta != nil)
        #expect(salaryMeta?.readAccess == .roles(["hr", "manager"]))
        #expect(salaryMeta?.writeAccess == .roles(["hr"]))

        // Find ssn metadata
        let ssnMeta = metadata.first { $0.fieldName == "ssn" }
        #expect(ssnMeta != nil)
        #expect(ssnMeta?.readAccess == .roles(["hr"]))

        // Find department metadata
        let deptMeta = metadata.first { $0.fieldName == "department" }
        #expect(deptMeta != nil)
        #expect(deptMeta?.readAccess == .public)
        #expect(deptMeta?.writeAccess == .roles(["admin"]))

        // Find internalNotes metadata
        let notesMeta = metadata.first { $0.fieldName == "internalNotes" }
        #expect(notesMeta != nil)
        #expect(notesMeta?.readAccess == .authenticated)
    }

    @Test("Models without @Restricted have empty metadata")
    func modelsWithoutRestrictedHaveEmptyMetadata() {
        #expect(PublicProfile.restrictedFieldsMetadata.isEmpty)
    }

    @Test("Metadata is preserved after encode/decode - CRITICAL TEST")
    func metadataPreservedAfterDecode() throws {
        // Create an employee
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000
        employee.ssn = "123-45-6789"

        // Encode to JSON
        let encoder = JSONEncoder()
        let data = try encoder.encode(employee)

        // Decode back
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(SecureEmployee.self, from: data)

        // Values should be preserved
        #expect(decoded.name == "Alice")
        #expect(decoded.salary == 100000)
        #expect(decoded.ssn == "123-45-6789")

        // CRITICAL: Static metadata should still be correct after decode
        // This would fail with reflection-based approach because @Restricted
        // property wrapper loses access levels after decode
        let metadata = SecureEmployee.restrictedFieldsMetadata
        let salaryMeta = metadata.first { $0.fieldName == "salary" }
        #expect(salaryMeta?.readAccess == .roles(["hr", "manager"]))
        #expect(salaryMeta?.writeAccess == .roles(["hr"]))

        // Verify evaluator uses static metadata
        let hrAuth = TestAuth(userID: "hr1", roles: ["hr"])
        let employeeAuth = TestAuth(userID: "emp1", roles: ["employee"])

        // Should use static metadata, not instance reflection
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: decoded, auth: hrAuth) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: decoded, auth: employeeAuth) == false)
    }
}

// MARK: - Masking Tests (Key Fix Verification)

@Suite("Field Masking")
struct FieldMaskingTests {

    @Test("masked(auth:) masks restricted fields")
    func maskedMasksRestrictedFields() {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000
        employee.ssn = "123-45-6789"
        employee.department = "Engineering"
        employee.internalNotes = "Good performer"

        // Regular employee - cannot see salary, ssn
        let employeeAuth = TestAuth(userID: "emp1", roles: ["employee"])
        let masked = employee.masked(auth: employeeAuth)

        #expect(masked.name == "Alice")
        #expect(masked.salary == 0) // Masked to default
        #expect(masked.ssn == "") // Masked to default
        #expect(masked.department == "Engineering") // Visible
        #expect(masked.internalNotes == "Good performer") // Visible (authenticated)
    }

    @Test("masked(auth:) preserves visible fields")
    func maskedPreservesVisibleFields() {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000
        employee.ssn = "123-45-6789"

        // HR user - can see everything
        let hrAuth = TestAuth(userID: "hr1", roles: ["hr"])
        let masked = employee.masked(auth: hrAuth)

        #expect(masked.name == "Alice")
        #expect(masked.salary == 100000) // Visible to HR
        #expect(masked.ssn == "123-45-6789") // Visible to HR
    }

    @Test("FieldSecurityEvaluator.mask uses generated method")
    func evaluatorMaskUsesGeneratedMethod() {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000

        let employeeAuth = TestAuth(userID: "emp1", roles: ["employee"])
        let masked = FieldSecurityEvaluator.mask(employee, auth: employeeAuth)

        #expect(masked.salary == 0) // Should be masked
    }

    @Test("Masking works after encode/decode - CRITICAL TEST")
    func maskingWorksAfterDecode() throws {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000

        // Encode and decode
        let encoder = JSONEncoder()
        let data = try encoder.encode(employee)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(SecureEmployee.self, from: data)

        // CRITICAL: Masking should still work on decoded instance
        // This would fail if masking relied on @Restricted property wrapper's
        // instance access levels (which are lost after decode)
        let employeeAuth = TestAuth(userID: "emp1", roles: ["employee"])
        let masked = decoded.masked(auth: employeeAuth)

        #expect(masked.salary == 0) // Should be masked even after decode
    }

    @Test("Batch masking works")
    func batchMaskingWorks() {
        var emp1 = SecureEmployee(name: "Alice")
        emp1.salary = 100000

        var emp2 = SecureEmployee(name: "Bob")
        emp2.salary = 80000

        let employees = [emp1, emp2]
        let employeeAuth = TestAuth(userID: "emp1", roles: ["employee"])

        let masked = FieldSecurityEvaluator.mask(employees, auth: employeeAuth)

        #expect(masked.count == 2)
        #expect(masked[0].salary == 0)
        #expect(masked[1].salary == 0)
        #expect(masked[0].name == "Alice")
        #expect(masked[1].name == "Bob")
    }
}

// MARK: - FieldAccessLevel Tests

@Suite("FieldAccessLevel")
struct FieldAccessLevelTests {

    @Test("Public access allows everyone")
    func publicAccessAllowsEveryone() {
        let level = FieldAccessLevel.public

        #expect(level.evaluate(auth: nil) == true)
        #expect(level.evaluate(auth: TestAuth(userID: "user1")) == true)
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["admin"])) == true)
    }

    @Test("Authenticated access requires auth")
    func authenticatedAccessRequiresAuth() {
        let level = FieldAccessLevel.authenticated

        #expect(level.evaluate(auth: nil) == false)
        #expect(level.evaluate(auth: TestAuth(userID: "user1")) == true)
    }

    @Test("Role-based access checks roles")
    func roleBasedAccessChecksRoles() {
        let level = FieldAccessLevel.roles(["hr", "manager"])

        #expect(level.evaluate(auth: nil) == false)
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["employee"])) == false)
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["hr"])) == true)
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["manager"])) == true)
        #expect(level.evaluate(auth: TestAuth(userID: "user1", roles: ["employee", "hr"])) == true)
    }

    @Test("Custom access uses predicate")
    func customAccessUsesPredicate() {
        let level = FieldAccessLevel.custom { auth in
            auth.userID.hasPrefix("admin_")
        }

        #expect(level.evaluate(auth: nil) == false)
        #expect(level.evaluate(auth: TestAuth(userID: "user1")) == false)
        #expect(level.evaluate(auth: TestAuth(userID: "admin_1")) == true)
    }

    @Test("FieldAccessLevel equality")
    func fieldAccessLevelEquality() {
        #expect(FieldAccessLevel.public == FieldAccessLevel.public)
        #expect(FieldAccessLevel.authenticated == FieldAccessLevel.authenticated)
        #expect(FieldAccessLevel.roles(["a", "b"]) == FieldAccessLevel.roles(["a", "b"]))
        #expect(FieldAccessLevel.roles(["a"]) != FieldAccessLevel.roles(["b"]))
    }
}

// MARK: - FieldSecurityEvaluator Tests

@Suite("FieldSecurityEvaluator")
struct FieldSecurityEvaluatorTests {

    @Test("Extract restricted fields from type (static)")
    func extractRestrictedFieldsFromType() {
        let restrictions = FieldSecurityEvaluator.extractRestrictedFields(for: SecureEmployee.self)

        #expect(restrictions.count == 4)
        #expect(restrictions["salary"] != nil)
        #expect(restrictions["ssn"] != nil)
        #expect(restrictions["department"] != nil)
        #expect(restrictions["internalNotes"] != nil)

        #expect(restrictions["salary"]?.readAccess == .roles(["hr", "manager"]))
        #expect(restrictions["salary"]?.writeAccess == .roles(["hr"]))
    }

    @Test("canRead evaluates correctly using static metadata")
    func canReadEvaluatesUsingStaticMetadata() {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000

        // Unauthenticated user
        #expect(FieldSecurityEvaluator.canRead(field: "name", in: employee, auth: nil) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: nil) == false)
        #expect(FieldSecurityEvaluator.canRead(field: "internalNotes", in: employee, auth: nil) == false)

        // Regular employee
        let employeeAuth = TestAuth(userID: "user1", roles: ["employee"])
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: employeeAuth) == false)
        #expect(FieldSecurityEvaluator.canRead(field: "internalNotes", in: employee, auth: employeeAuth) == true)

        // HR user
        let hrAuth = TestAuth(userID: "hr1", roles: ["hr"])
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: hrAuth) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "ssn", in: employee, auth: hrAuth) == true)

        // Manager
        let managerAuth = TestAuth(userID: "mgr1", roles: ["manager"])
        #expect(FieldSecurityEvaluator.canRead(field: "salary", in: employee, auth: managerAuth) == true)
        #expect(FieldSecurityEvaluator.canRead(field: "ssn", in: employee, auth: managerAuth) == false)
    }

    @Test("canWrite evaluates correctly")
    func canWriteEvaluates() {
        let employee = SecureEmployee(name: "Alice")

        let employeeAuth = TestAuth(userID: "user1", roles: ["employee"])
        #expect(FieldSecurityEvaluator.canWrite(field: "name", in: employee, auth: employeeAuth) == true)
        #expect(FieldSecurityEvaluator.canWrite(field: "salary", in: employee, auth: employeeAuth) == false)
        #expect(FieldSecurityEvaluator.canWrite(field: "department", in: employee, auth: employeeAuth) == false)

        let hrAuth = TestAuth(userID: "hr1", roles: ["hr"])
        #expect(FieldSecurityEvaluator.canWrite(field: "salary", in: employee, auth: hrAuth) == true)

        let adminAuth = TestAuth(userID: "admin1", roles: ["admin"])
        #expect(FieldSecurityEvaluator.canWrite(field: "department", in: employee, auth: adminAuth) == true)
    }

    @Test("unreadableFields returns correct list")
    func unreadableFieldsReturnsCorrectList() {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100000

        // Unauthenticated
        let unreadableNil = FieldSecurityEvaluator.unreadableFields(in: employee, auth: nil)
        #expect(unreadableNil.contains("salary"))
        #expect(unreadableNil.contains("ssn"))
        #expect(unreadableNil.contains("internalNotes"))
        #expect(!unreadableNil.contains("department"))

        // Regular employee
        let employeeAuth = TestAuth(userID: "user1", roles: ["employee"])
        let unreadableEmployee = FieldSecurityEvaluator.unreadableFields(in: employee, auth: employeeAuth)
        #expect(unreadableEmployee.contains("salary"))
        #expect(unreadableEmployee.contains("ssn"))
        #expect(!unreadableEmployee.contains("internalNotes"))
    }

    @Test("validateWrite throws for unauthorized field changes")
    func validateWriteThrowsForUnauthorizedChanges() {
        var original = SecureEmployee(name: "Alice")
        original.salary = 50000
        var updated = original
        updated.salary = 100000

        let employeeAuth = TestAuth(userID: "user1", roles: ["employee"])

        #expect(throws: FieldSecurityError.self) {
            try FieldSecurityEvaluator.validateWrite(
                original: original,
                updated: updated,
                auth: employeeAuth
            )
        }
    }

    @Test("validateWrite allows authorized field changes")
    func validateWriteAllowsAuthorizedChanges() throws {
        var original = SecureEmployee(name: "Alice")
        original.salary = 50000
        var updated = original
        updated.salary = 100000

        let hrAuth = TestAuth(userID: "hr1", roles: ["hr"])

        try FieldSecurityEvaluator.validateWrite(
            original: original,
            updated: updated,
            auth: hrAuth
        )
    }

    @Test("validateWrite allows changes to unrestricted fields")
    func validateWriteAllowsUnrestrictedChanges() throws {
        let original = SecureEmployee(name: "Alice")
        var updated = original
        updated.name = "Alice Smith"

        let employeeAuth = TestAuth(userID: "user1", roles: ["employee"])

        try FieldSecurityEvaluator.validateWrite(
            original: original,
            updated: updated,
            auth: employeeAuth
        )
    }

    @Test("validateWrite for new insert checks non-default values")
    func validateWriteForNewInsertChecksNonDefaults() {
        var newEmployee = SecureEmployee(name: "Bob")
        newEmployee.salary = 75000

        let employeeAuth = TestAuth(userID: "user1", roles: ["employee"])

        #expect(throws: FieldSecurityError.self) {
            try FieldSecurityEvaluator.validateWrite(
                original: nil,
                updated: newEmployee,
                auth: employeeAuth
            )
        }
    }
}

// MARK: - FieldSecurityError Tests

@Suite("FieldSecurityError")
struct FieldSecurityErrorTests {

    @Test("Error contains type and field info")
    func errorContainsTypeAndFieldInfo() {
        let error = FieldSecurityError.writeNotAllowed(type: "SecureEmployee", fields: ["salary", "ssn"])

        #expect(error.description.contains("SecureEmployee"))
        #expect(error.description.contains("salary"))
        #expect(error.description.contains("ssn"))
    }

    @Test("Error is equatable")
    func errorIsEquatable() {
        let error1 = FieldSecurityError.writeNotAllowed(type: "SecureEmployee", fields: ["salary"])
        let error2 = FieldSecurityError.writeNotAllowed(type: "SecureEmployee", fields: ["salary"])
        let error3 = FieldSecurityError.writeNotAllowed(type: "SecureEmployee", fields: ["ssn"])

        #expect(error1 == error2)
        #expect(error1 != error3)
    }
}

// MARK: - RestrictedFieldMetadata Tests

@Suite("RestrictedFieldMetadata")
struct RestrictedFieldMetadataTests {

    @Test("Metadata struct is equatable")
    func metadataIsEquatable() {
        let meta1 = RestrictedFieldMetadata(
            fieldName: "salary",
            readAccess: .roles(["hr"]),
            writeAccess: .roles(["hr"])
        )
        let meta2 = RestrictedFieldMetadata(
            fieldName: "salary",
            readAccess: .roles(["hr"]),
            writeAccess: .roles(["hr"])
        )
        let meta3 = RestrictedFieldMetadata(
            fieldName: "ssn",
            readAccess: .roles(["hr"]),
            writeAccess: .roles(["hr"])
        )

        #expect(meta1 == meta2)
        #expect(meta1 != meta3)
    }

    @Test("Metadata is Sendable")
    func metadataIsSendable() {
        // Compile-time check - if this compiles, it's Sendable
        let _: any Sendable = RestrictedFieldMetadata(
            fieldName: "test",
            readAccess: .public,
            writeAccess: .public
        )
    }
}
