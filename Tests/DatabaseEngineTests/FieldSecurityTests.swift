#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Persistable
private struct SecureEmployee {
    var id: String = ""
    var name: String = ""

    @Restricted(read: .roles(["hr", "manager"]), write: .roles(["hr"]))
    var salary: Double = 0

    @Restricted(read: .roles(["hr"]), write: .roles(["hr"]))
    var socialSecurityNumber: String = ""

    @Restricted(write: .roles(["admin"]))
    var department: String = ""

    @Restricted(read: .authenticated)
    var internalNotes: String = ""
}

@Persistable
private struct PublicProfile {
    var id: String = ""
    var name: String = ""
    var biography: String = ""
}

private func authorization(
    identifier: String,
    roles: Set<String> = []
) -> AuthorizationContext {
    .authenticated(
        Principal(identifier: identifier, roles: roles)
    )
}

@Suite("Compiled field authorization metadata")
struct CompiledFieldAuthorizationMetadataTests {
    @Test("@Persistable emits exact field identities and access rules")
    func generatedRules() throws {
        let rules = SecureEmployee.fieldAccessRules

        #expect(rules.count == 4)
        #expect(
            rules.first {
                $0.field == SecureEmployee.fields.salary.identity
            }?.read == .roles(["hr", "manager"])
        )
        #expect(
            rules.first {
                $0.field
                    == SecureEmployee.fields.socialSecurityNumber.identity
            }?.write == .roles(["hr"])
        )
        #expect(
            rules.first {
                $0.field == SecureEmployee.fields.department.identity
            }?.read == .public
        )
        #expect(
            rules.first {
                $0.field == SecureEmployee.fields.internalNotes.identity
            }?.read == .authenticated
        )
    }

    @Test("Unrestricted entities emit no field rules")
    func unrestrictedEntityRules() {
        #expect(PublicProfile.fieldAccessRules.isEmpty)
    }

    @Test("Canonical storage round-trip preserves values and static rules")
    func canonicalStorageRoundTrip() throws {
        var employee = SecureEmployee(name: "Alice")
        employee.salary = 100_000
        employee.socialSecurityNumber = "123-45-6789"

        let frame = try PersistableStorageCodec.encode(employee)
        let decoded = try PersistableStorageCodec.decode(
            SecureEmployee.self,
            from: frame
        )

        #expect(decoded.name == employee.name)
        #expect(decoded.salary == employee.salary)
        #expect(
            FieldSecurityEvaluator.canRead(
                SecureEmployee.fields.salary,
                in: decoded,
                context: authorization(
                    identifier: "hr-1",
                    roles: ["hr"]
                )
            )
        )
        #expect(
            !FieldSecurityEvaluator.canRead(
                SecureEmployee.fields.salary,
                in: decoded,
                context: authorization(
                    identifier: "employee-1",
                    roles: ["employee"]
                )
            )
        )
    }
}

@Suite("Field access decisions")
struct FieldAccessDecisionTests {
    @Test("Access levels evaluate concrete authorization context")
    func accessLevels() {
        let employee = authorization(
            identifier: "employee-1",
            roles: ["employee"]
        )
        let humanResources = authorization(
            identifier: "hr-1",
            roles: ["hr"]
        )

        #expect(FieldAccessLevel.public.allows(.anonymous))
        #expect(!FieldAccessLevel.authenticated.allows(.anonymous))
        #expect(FieldAccessLevel.authenticated.allows(employee))
        #expect(
            !FieldAccessLevel.roles(["hr", "manager"]).allows(employee)
        )
        #expect(FieldAccessLevel.roles(["hr", "manager"]).allows(humanResources))
    }

    @Test("Evaluator accepts generated fields instead of string lookup")
    func generatedFieldEvaluation() {
        let employee = SecureEmployee(name: "Alice")
        let regularContext = authorization(
            identifier: "employee-1",
            roles: ["employee"]
        )
        let humanResourcesContext = authorization(
            identifier: "hr-1",
            roles: ["hr"]
        )

        #expect(
            FieldSecurityEvaluator.canRead(
                SecureEmployee.fields.name,
                in: employee,
                context: .anonymous
            )
        )
        #expect(
            !FieldSecurityEvaluator.canRead(
                SecureEmployee.fields.salary,
                in: employee,
                context: regularContext
            )
        )
        #expect(
            FieldSecurityEvaluator.canRead(
                SecureEmployee.fields.salary,
                in: employee,
                context: humanResourcesContext
            )
        )
        #expect(
            !FieldSecurityEvaluator.canWrite(
                SecureEmployee.fields.department,
                in: employee,
                context: regularContext
            )
        )
    }

    @Test("Unreadable field output retains canonical identities")
    func unreadableFieldIdentities() {
        let employee = SecureEmployee(name: "Alice")
        let unreadable = Set(
            FieldSecurityEvaluator.unreadableFields(
                in: employee,
                context: .anonymous
            )
        )

        #expect(unreadable.contains(SecureEmployee.fields.salary.identity))
        #expect(
            unreadable.contains(
                SecureEmployee.fields.socialSecurityNumber.identity
            )
        )
        #expect(
            unreadable.contains(
                SecureEmployee.fields.internalNotes.identity
            )
        )
        #expect(
            !unreadable.contains(
                SecureEmployee.fields.department.identity
            )
        )
    }
}

@Suite("Restricted field mutation validation")
struct RestrictedFieldMutationValidationTests {
    @Test("Insert rejects every field the principal may not write")
    func insertValidation() {
        let employee = SecureEmployee(name: "Alice")
        let context = authorization(
            identifier: "employee-1",
            roles: ["employee"]
        )

        #expect(
            throws: FieldSecurityError.writeNotAllowed(
                type: SecureEmployee.persistableType,
                fields: [
                    "department",
                    "salary",
                    "socialSecurityNumber",
                ]
            )
        ) {
            try FieldSecurityEvaluator.validateInsert(
                updated: employee,
                context: context
            )
        }
    }

    @Test("Update validates only canonical values that changed")
    func updateValidation() throws {
        let original = SecureEmployee(name: "Alice")
        var renamed = original
        renamed.name = "Alicia"
        let employeeContext = authorization(
            identifier: "employee-1",
            roles: ["employee"]
        )

        try FieldSecurityEvaluator.validateUpdate(
            original: original,
            updated: renamed,
            context: employeeContext
        )

        var salaryChanged = renamed
        salaryChanged.salary = 125_000
        #expect(
            throws: FieldSecurityError.writeNotAllowed(
                type: SecureEmployee.persistableType,
                fields: ["salary"]
            )
        ) {
            try FieldSecurityEvaluator.validateUpdate(
                original: renamed,
                updated: salaryChanged,
                context: employeeContext
            )
        }

        try FieldSecurityEvaluator.validateUpdate(
            original: renamed,
            updated: salaryChanged,
            context: authorization(
                identifier: "hr-1",
                roles: ["hr"]
            )
        )
    }
}
#endif
#endif
