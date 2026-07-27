import DatabaseKit

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
        borrowing any Persistable,
        AuthorizationContext
    ) throws -> Bool
    private let createDecision: @Sendable (
        borrowing any Persistable,
        AuthorizationContext
    ) throws -> Bool
    private let updateDecision: @Sendable (
        borrowing any Persistable,
        borrowing any Persistable,
        AuthorizationContext
    ) throws -> Bool
    private let deleteDecision: @Sendable (
        borrowing any Persistable,
        AuthorizationContext
    ) throws -> Bool

    public init<Model: SecurityPolicy>(_ modelType: Model.Type) {
        _ = modelType
        self.entityName = Model.persistableType
        self.queryDecision = { query, context in
            Model.permitsQuery(query, in: context)
        }
        self.readDecision = { resource, context in
            guard let resource = resource as? Model else {
                throw AuthorizationPolicyHandlerError.modelTypeMismatch(
                    expected: Model.persistableType,
                    actual: type(of: resource).persistableType
                )
            }
            return Model.permitsRead(of: resource, in: context)
        }
        self.createDecision = { resource, context in
            guard let resource = resource as? Model else {
                throw AuthorizationPolicyHandlerError.modelTypeMismatch(
                    expected: Model.persistableType,
                    actual: type(of: resource).persistableType
                )
            }
            return Model.permitsCreate(resource, in: context)
        }
        self.updateDecision = { oldResource, newResource, context in
            guard let oldResource = oldResource as? Model,
                  let newResource = newResource as? Model else {
                throw AuthorizationPolicyHandlerError.modelTypeMismatch(
                    expected: Model.persistableType,
                    actual: type(of: newResource).persistableType
                )
            }
            return Model.permitsUpdate(
                from: oldResource,
                to: newResource,
                in: context
            )
        }
        self.deleteDecision = { resource, context in
            guard let resource = resource as? Model else {
                throw AuthorizationPolicyHandlerError.modelTypeMismatch(
                    expected: Model.persistableType,
                    actual: type(of: resource).persistableType
                )
            }
            return Model.permitsDelete(resource, in: context)
        }
    }

    func permitsQuery(
        _ query: SecurityQuery,
        context: AuthorizationContext
    ) -> Bool {
        queryDecision(query, context)
    }

    func permitsRead(
        _ resource: borrowing any Persistable,
        context: AuthorizationContext
    ) throws -> Bool {
        try readDecision(resource, context)
    }

    func permitsCreate(
        _ resource: borrowing any Persistable,
        context: AuthorizationContext
    ) throws -> Bool {
        try createDecision(resource, context)
    }

    func permitsUpdate(
        from resource: borrowing any Persistable,
        to newResource: borrowing any Persistable,
        context: AuthorizationContext
    ) throws -> Bool {
        try updateDecision(resource, newResource, context)
    }

    func permitsDelete(
        _ resource: borrowing any Persistable,
        context: AuthorizationContext
    ) throws -> Bool {
        try deleteDecision(resource, context)
    }
}
