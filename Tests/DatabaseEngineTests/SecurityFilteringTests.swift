#if !os(WASI)
#if FOUNDATION_DB
import Foundation
import Testing
import TestHeartbeat
@testable import DatabaseKit
@testable import DatabaseEngine

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

@Suite("Request authorization policy", .heartbeat)
struct RequestAuthorizationPolicyTests {
    private func delegate(
        configuration: SecurityConfiguration = .enabled()
    ) throws -> RequestSecurityPolicyDelegate {
        let registry = try AuthorizationPolicyRegistry(
            handlers: [AuthorizationPolicyHandler(SecuredRecord.self)]
        )
        return RequestSecurityPolicyDelegate(
            configuration: configuration,
            policies: registry
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
            try security.evaluateGet(try PersistedModel(owned))
            try security.evaluateGet(try PersistedModel(published))
        }
    }

    @Test("Registered policy denial is a typed security failure")
    func registeredPolicyDeniesForeignPrivateRead() throws {
        let security = try delegate()
        let record = SecuredRecord(ownerID: "bob", title: "Private")

        do {
            try RequestAuthorization.$context.withValue(principal("alice")) {
                try security.evaluateGet(try PersistedModel(record))
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

    @Test("Administrator role bypasses entity policy")
    func administratorBypassesPolicy() throws {
        let security = try delegate()
        let foreign = SecuredRecord(ownerID: "bob", title: "Private")

        try RequestAuthorization.$context.withValue(
            principal("operator", roles: ["admin"])
        ) {
            try security.evaluateGet(try PersistedModel(foreign))
            try security.requireAdmin(
                operation: "rebuildIndex",
                targetType: SecuredRecord.persistableType
            )
        }
    }

    @Test("Non-administrator cannot perform admin operation")
    func nonAdministratorCannotPerformAdminOperation() throws {
        let security = try delegate()

        #expect(throws: SecurityError.self) {
            try RequestAuthorization.$context.withValue(principal("alice")) {
                try security.requireAdmin(
                    operation: "rebuildIndex",
                    targetType: SecuredRecord.persistableType
                )
            }
        }
    }

    @Test("Explicitly disabled security bypasses policy evaluation")
    func disabledSecurityBypassesPolicies() throws {
        let security = try delegate(configuration: .disabled)
        let foreign = SecuredRecord(ownerID: "bob", title: "Private")

        try security.evaluateGet(try PersistedModel(foreign))
        try security.evaluateList(
            entity: UnregisteredRecord.persistableType,
            limit: nil,
            offset: nil,
            orderBy: nil
        )
        try security.requireAdmin(
            operation: "rebuildIndex",
            targetType: SecuredRecord.persistableType
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
#endif
