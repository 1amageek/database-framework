#if !os(WASI)
import Foundation
import DatabaseRuntime
import StorageKit
import Testing
import TestHeartbeat
@testable import DatabaseKit
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct SecuredRecord: SecurityPolicy {
    var id: String = UUID().uuidString
    var ownerID: String
    var title: String
    var isPublic: Bool = false

    static func permitsRead(
        of resource: borrowing SecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.isPublic
            || resource.ownerID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.isAuthenticated && (query.limit ?? 0) <= 100
    }

    static func permitsCreate(
        _ newResource: borrowing SecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        newResource.ownerID == context.principal?.identifier
    }

    static func permitsUpdate(
        from resource: borrowing SecuredRecord,
        to newResource: borrowing SecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
            && newResource.ownerID == resource.ownerID
    }

    static func permitsDelete(
        _ resource: borrowing SecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }
}

@Persistable
private struct UnregisteredRecord {
    var id: String = UUID().uuidString
    var title: String
}

@Persistable
private struct FieldSecuredRecord: SecurityPolicy {
    var id: String = UUID().uuidString
    var title: String

    @Restricted(read: .roles(["security"]), write: .roles(["security"]))
    var secret: String = ""

    static func permitsRead(
        of resource: borrowing FieldSecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsCreate(
        _ newResource: borrowing FieldSecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing FieldSecuredRecord,
        to newResource: borrowing FieldSecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing FieldSecuredRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Persistable
private struct ExactQueryShapeRecord: SecurityPolicy {
    var id: String = UUID().uuidString
    var score: Int64 = 0
    var depth: Int64 = 0

    static func permitsRead(
        of resource: borrowing ExactQueryShapeRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        query.limit == 1
            && query.offset == nil
            && query.orderBy == ["score"]
    }

    static func permitsCreate(
        _ newResource: borrowing ExactQueryShapeRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing ExactQueryShapeRecord,
        to newResource: borrowing ExactQueryShapeRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing ExactQueryShapeRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Suite("Request authorization policy", .heartbeat)
struct RequestAuthorizationPolicyTests {
    private func delegate() throws -> RequestSecurityPolicyDelegate {
        let registry = try AuthorizationPolicyRegistry(
            handlers: [
                AuthorizationPolicyHandler(SecuredRecord.self),
                AuthorizationPolicyHandler(FieldSecuredRecord.self),
            ]
        )
        return RequestSecurityPolicyDelegate(
            policies: registry,
            schema: try Schema(
                entities: [
                    try Schema.Entity(from: SecuredRecord.self),
                    try Schema.Entity(from: FieldSecuredRecord.self),
                ],
                version: Schema.Version(1, 0, 0)
            )
        )
    }

    private func principal(
        _ identifier: String,
        roles: Set<String> = []
    ) -> AuthorizationContext {
        .authenticated(
            Principal(identifier: identifier, roles: roles)
        )
    }

    @Test("Registered policy permits owner and public reads")
    func registeredPolicyPermitsReads() throws {
        let security = try delegate()
        let owned = SecuredRecord(ownerID: "alice", title: "Owned")
        let published = SecuredRecord(
            ownerID: "bob",
            title: "Published",
            isPublic: true
        )

        try RequestAuthorization.$context.withValue(principal("alice")) {
            try security.evaluateGet(try PersistedModel(owned), fields: nil)
            try security.evaluateGet(try PersistedModel(published), fields: nil)
        }
    }

    @Test("Registered policy denial is a typed security failure")
    func registeredPolicyDeniesForeignPrivateRead() throws {
        let security = try delegate()
        let record = SecuredRecord(ownerID: "bob", title: "Private")

        do {
            try RequestAuthorization.$context.withValue(principal("alice")) {
                try security.evaluateGet(
                    try PersistedModel(record),
                    fields: nil
                )
            }
            Issue.record("Expected read denial")
        } catch let error as SecurityError {
            #expect(error.operation == .get)
            #expect(error.targetType == SecuredRecord.persistableType)
            #expect(error.resource?.id == .string(record.id))
            #expect(error.userID == "alice")
        }
    }

    @Test("Query policy validates bounds and authenticated context")
    func queryPolicyValidatesRequest() throws {
        let security = try delegate()

        try RequestAuthorization.$context.withValue(principal("alice")) {
            try security.evaluateList(
                entity: SecuredRecord.persistableType,
                limit: 100,
                offset: 0,
                orderBy: ["title"]
            )
        }

        #expect(throws: SecurityError.self) {
            try RequestAuthorization.$context.withValue(principal("alice")) {
                try security.evaluateList(
                    entity: SecuredRecord.persistableType,
                    limit: 101,
                    offset: 0,
                    orderBy: nil
                )
            }
        }
        #expect(throws: SecurityError.self) {
            try security.evaluateList(
                entity: SecuredRecord.persistableType,
                limit: 10,
                offset: 0,
                orderBy: nil
            )
        }
    }

    @Test("Logical LIST admission covers only the exact query shape")
    func logicalListAdmissionRequiresExactShape() async throws {
        let schema = try Schema(
            entities: [try ExactQueryShapeRecord.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "exact-list-admission-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        ExactQueryShapeRecord.self
                    )
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(ExactQueryShapeRecord.self)
                ]
            )
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "exact-query-reader")
            )
        )
        let allowed = IndexReadAuthorization(
            limit: 1,
            offset: nil,
            orderBy: ["score"]
        )
        try await context.withDataOperation {
            let admission = try context.admitLogicalRead(
                listAuthorization: allowed,
                fieldPlan: .fullEntity(try ExactQueryShapeRecord.schemaEntity)
            )

            try await context.withReadAuthorizationAdmission(admission) {
                #expect(throws: SecurityError.self) {
                    try context.indexQueryContext.authorizeListAccess(
                        entityName: ExactQueryShapeRecord.persistableType,
                        authorization: IndexReadAuthorization(
                            limit: nil,
                            offset: nil,
                            orderBy: ["score"]
                        )
                    )
                }
                #expect(throws: SecurityError.self) {
                    try context.indexQueryContext.authorizeListAccess(
                        entityName: ExactQueryShapeRecord.persistableType,
                        authorization: IndexReadAuthorization(
                            limit: 1,
                            offset: nil,
                            orderBy: ["depth"]
                        )
                    )
                }
            }
        }
    }

    @Test("Mutation policies preserve ownership")
    func mutationPoliciesPreserveOwnership() throws {
        let security = try delegate()
        let original = SecuredRecord(ownerID: "alice", title: "Original")
        var updated = original
        updated.title = "Updated"
        var transferred = updated
        transferred.ownerID = "bob"

        try RequestAuthorization.$context.withValue(principal("alice")) {
            try security.evaluateCreate(try PersistedModel(original))
            try security.evaluateUpdate(
                try PersistedModel(original),
                newResource: try PersistedModel(updated)
            )
            try security.evaluateDelete(try PersistedModel(updated))
        }

        #expect(throws: SecurityError.self) {
            try RequestAuthorization.$context.withValue(principal("alice")) {
                try security.evaluateUpdate(
                    try PersistedModel(original),
                    newResource: try PersistedModel(transferred)
                )
            }
        }
        #expect(throws: SecurityError.self) {
            try RequestAuthorization.$context.withValue(principal("bob")) {
                try security.evaluateDelete(try PersistedModel(original))
            }
        }
    }

    @Test("Missing policy denies instead of returning an empty result")
    func missingPolicyDenies() throws {
        let security = try delegate()

        #expect(throws: SecurityError.self) {
            try RequestAuthorization.$context.withValue(principal("alice")) {
                try security.evaluateList(
                    entity: UnregisteredRecord.persistableType,
                    limit: 10,
                    offset: 0,
                    orderBy: nil
                )
            }
        }
    }

    @Test("Administrator role does not bypass entity policy")
    func administratorDoesNotBypassPolicy() throws {
        let security = try delegate()
        let foreign = SecuredRecord(ownerID: "bob", title: "Private")

        #expect(throws: SecurityError.self) {
            try RequestAuthorization.$context.withValue(
                principal("operator", roles: ["admin"])
            ) {
                try security.evaluateGet(
                    try PersistedModel(foreign),
                    fields: nil
                )
            }
        }
    }

    @Test("Schema field rules distinguish projected and complete reads")
    func schemaFieldRulesAuthorizeProjectedReads() throws {
        let security = try delegate()
        var record = FieldSecuredRecord(title: "Visible")
        record.secret = "classified"
        let persisted = try PersistedModel(record)

        try RequestAuthorization.$context.withValue(principal("employee")) {
            try security.evaluateFieldRead(
                entity: FieldSecuredRecord.persistableType,
                fields: ["title"]
            )
            try security.evaluateGet(persisted, fields: ["title"])
        }
        #expect(throws: FieldSecurityError.self) {
            try RequestAuthorization.$context.withValue(
                principal("employee")
            ) {
                try security.evaluateGet(persisted, fields: nil)
            }
        }
        try RequestAuthorization.$context.withValue(
            principal("security", roles: ["security"])
        ) {
            try security.evaluateGet(persisted, fields: nil)
        }
    }

    @Test("Schema field rules validate canonical creates and updates")
    func schemaFieldRulesAuthorizeCanonicalWrites() throws {
        let security = try delegate()
        var original = FieldSecuredRecord(title: "Visible")
        original.secret = "classified"
        var renamed = original
        renamed.title = "Renamed"
        var changed = renamed
        changed.secret = "changed"

        try RequestAuthorization.$context.withValue(principal("employee")) {
            try security.evaluateUpdate(
                try PersistedModel(original),
                newResource: try PersistedModel(renamed)
            )
        }
        #expect(throws: FieldSecurityError.self) {
            try RequestAuthorization.$context.withValue(
                principal("employee")
            ) {
                try security.evaluateCreate(try PersistedModel(original))
            }
        }
        #expect(throws: FieldSecurityError.self) {
            try RequestAuthorization.$context.withValue(
                principal("employee")
            ) {
                try security.evaluateUpdate(
                    try PersistedModel(renamed),
                    newResource: try PersistedModel(changed)
                )
            }
        }
    }

    @Test("Test-only delegate bypasses entity policy evaluation")
    func testOnlyDelegateBypassesPolicies() throws {
        let security = DisabledSecurityDelegate()
        let foreign = SecuredRecord(ownerID: "bob", title: "Private")

        try security.evaluateGet(
            try PersistedModel(foreign),
            fields: nil
        )
        try security.evaluateList(
            entity: UnregisteredRecord.persistableType,
            limit: nil,
            offset: nil,
            orderBy: nil
        )
    }

    @Test("Handler rejects a model type mismatch")
    func handlerRejectsModelTypeMismatch() throws {
        let handler = AuthorizationPolicyHandler(SecuredRecord.self)
        let other = UnregisteredRecord(title: "Other")

        #expect(throws: AuthorizationPolicyHandlerError.self) {
            _ = try handler.permitsRead(
                try PersistedModel(other),
                context: principal("alice")
            )
        }
    }
}
#endif
