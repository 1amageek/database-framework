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
            try Self.validateEntity(of: resource, expected: Model.persistableType)
            let resource = try resource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsRead(of: resource, in: context),
                resource: resource
            )
        }
        self.createDecision = { resource, context in
            try Self.validateEntity(of: resource, expected: Model.persistableType)
            let resource = try resource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsCreate(resource, in: context),
                resource: resource
            )
        }
        self.updateDecision = { oldResource, newResource, context in
            try Self.validateEntity(
                of: oldResource,
                expected: Model.persistableType
            )
            try Self.validateEntity(
                of: newResource,
                expected: Model.persistableType
            )
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
            try Self.validateEntity(of: resource, expected: Model.persistableType)
            let resource = try resource.decode(as: Model.self)
            return try AuthorizationResourceDecision(
                isPermitted: Model.permitsDelete(resource, in: context),
                resource: resource
            )
        }
    }

    /// Binds an operation policy to a schema entity without a compiled model.
    /// Field policies and Partition authorization remain separate checks.
    public init(
        schemaDriven entity: Schema.Entity,
        permits: @escaping @Sendable (SecurityError.Operation, AuthorizationContext) -> Bool
    ) {
        self.entityName = entity.name
        self.queryDecision = { _, context in permits(.list, context) }
        self.readDecision = { resource, context in
            try Self.schemaDecision(resource, entity: entity,
                                    isPermitted: permits(.get, context))
        }
        self.createDecision = { resource, context in
            try Self.schemaDecision(resource, entity: entity,
                                    isPermitted: permits(.create, context))
        }
        self.updateDecision = { original, updated, context in
            try Self.validateEntity(of: original, expected: entity.name)
            _ = try EntityRuntimeDefinition.identity(for: copy original, entity: entity)
            return try Self.schemaDecision(updated, entity: entity,
                                           isPermitted: permits(.update, context))
        }
        self.deleteDecision = { resource, context in
            try Self.schemaDecision(resource, entity: entity,
                                    isPermitted: permits(.delete, context))
        }
    }

    private static func schemaDecision(
        _ resource: borrowing PersistedModel,
        entity: Schema.Entity,
        isPermitted: Bool
    ) throws -> AuthorizationResourceDecision {
        try validateEntity(of: resource, expected: entity.name)
        return AuthorizationResourceDecision(
            isPermitted: isPermitted,
            identity: try EntityRuntimeDefinition.identity(for: copy resource, entity: entity)
        )
    }

    private static func validateEntity(
        of resource: borrowing PersistedModel,
        expected entity: String
    ) throws {
        guard resource.entity == entity else {
            throw AuthorizationPolicyHandlerError.modelTypeMismatch(
                expected: entity,
                actual: resource.entity
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
    let resource: EntityReference

    init(isPermitted: Bool, identity: EntityReference) {
        self.isPermitted = isPermitted
        self.resource = identity
    }

    init<Model: Persistable>(
        isPermitted: Bool,
        resource: borrowing Model
    ) throws {
        self.isPermitted = isPermitted
        self.resource = try EntityReferenceEncoder.encode(resource)
    }
}
