import DatabaseKit
import StorageKit

/// A container-scoped executable adapter for one concrete model policy.
///
/// The generic initializer captures the concrete model type once. Runtime
/// operation paths retain no KeyPath, reflection metadata, or model
/// reconstruction closure.
public struct AuthorizationPolicyHandler: Sendable {
    public let entityName: String

    private let queryDecision: @Sendable (
        SecurityQuery,
        AuthorizationContext
    ) -> Bool
    private let readDecision: @Sendable (
        borrowing PersistedModel,
        AuthorizationContext
    ) throws -> AuthorizationResourceDecision
    private let createDecision: @Sendable (
        borrowing PersistedModel,
        AuthorizationContext
    ) throws -> AuthorizationResourceDecision
    private let updateDecision: @Sendable (
        borrowing PersistedModel,
        borrowing PersistedModel,
        AuthorizationContext
    ) throws -> AuthorizationResourceDecision
    private let deleteDecision: @Sendable (
        borrowing PersistedModel,
        AuthorizationContext
    ) throws -> AuthorizationResourceDecision

    public init<Model: SecurityPolicy>(_ modelType: Model.Type) {
        _ = modelType
        self.entityName = Model.persistableType
        self.queryDecision = { query, context in
            Model.permitsQuery(query, in: context)
        }
        self.readDecision = { resource, context in
            let resource = try resource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsRead(of: resource, in: context),
                resource: resource
            )
        }
        self.createDecision = { resource, context in
            let resource = try resource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsCreate(resource, in: context),
                resource: resource
            )
        }
        self.updateDecision = { oldResource, newResource, context in
            let oldResource = try oldResource.decode(as: Model.self)
            let newResource = try newResource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsUpdate(
                    from: oldResource,
                    to: newResource,
                    in: context
                ),
                resource: newResource
            )
        }
        self.deleteDecision = { resource, context in
            let resource = try resource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsDelete(resource, in: context),
                resource: resource
            )
        }
    }

    func permitsQuery(
        _ query: SecurityQuery,
        context: AuthorizationContext
    ) -> Bool {
        queryDecision(query, context)
    }

    func permitsRead(
        _ resource: borrowing PersistedModel,
        context: AuthorizationContext
    ) throws -> AuthorizationResourceDecision {
        try readDecision(resource, context)
    }

    func permitsCreate(
        _ resource: borrowing PersistedModel,
        context: AuthorizationContext
    ) throws -> AuthorizationResourceDecision {
        try createDecision(resource, context)
    }

    func permitsUpdate(
        from resource: borrowing PersistedModel,
        to newResource: borrowing PersistedModel,
        context: AuthorizationContext
    ) throws -> AuthorizationResourceDecision {
        try updateDecision(resource, newResource, context)
    }

    func permitsDelete(
        _ resource: borrowing PersistedModel,
        context: AuthorizationContext
    ) throws -> AuthorizationResourceDecision {
        try deleteDecision(resource, context)
    }
}

struct AuthorizationResourceDecision: Sendable {
    let isPermitted: Bool
    let resourceID: String

    init<Model: Persistable>(
        isPermitted: Bool,
        resource: borrowing Model
    ) throws {
        self.isPermitted = isPermitted
        self.resourceID = DatabaseTextFormatting.lowercaseHex(
            try PersistableIdentifierKeyCodec.tuple(
                for: resource.persistableIdentifierValue,
                expectedType: Model.persistableIdentifierType
            ).pack()
        )
    }
}
