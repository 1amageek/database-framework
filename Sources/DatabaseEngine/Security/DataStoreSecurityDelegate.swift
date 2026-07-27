import DatabaseKit

/// Authorizes database operations before their observable effects occur.
public protocol DataStoreSecurityDelegate: Sendable {
    func evaluateList<Model: Persistable>(
        type: Model.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws

    func evaluateGet(
        _ resource: borrowing any Persistable
    ) throws

    func evaluateCreate(
        _ resource: borrowing any Persistable
    ) throws

    func evaluateUpdate(
        _ resource: borrowing any Persistable,
        newResource: borrowing any Persistable
    ) throws

    func evaluateDelete(
        _ resource: borrowing any Persistable
    ) throws

    func requireAdmin(operation: String, targetType: String) throws
}

extension DataStoreSecurityDelegate {
    /// Authorizes every result without converting denial into an empty or
    /// partial successful response.
    func evaluateReadResults<Model: Persistable>(
        _ resources: borrowing [Model]
    ) throws {
        for index in resources.indices {
            try evaluateGet(resources[index])
        }
    }
}

/// Request-scoped authorization state supplied by the authenticated
/// application boundary.
public enum RequestAuthorization {
    @TaskLocal public static var context: AuthorizationContext = .anonymous
}

/// Evaluates the explicitly registered policy for each compiled entity.
public final class RequestSecurityPolicyDelegate:
    DataStoreSecurityDelegate,
    Sendable
{
    private let configuration: SecurityConfiguration
    private let policies: AuthorizationPolicyRegistry

    public init(
        configuration: SecurityConfiguration,
        policies: AuthorizationPolicyRegistry
    ) {
        self.configuration = configuration
        self.policies = policies
    }

    private var context: AuthorizationContext {
        RequestAuthorization.context
    }

    private var userID: String? {
        context.principal?.identifier
    }

    private var isAdmin: Bool {
        guard let principal = context.principal else {
            return false
        }
        return !principal.roles.isDisjoint(
            with: configuration.adminRoles
        )
    }

    private var shouldEvaluate: Bool {
        configuration.isEnabled && !isAdmin
    }

    public func evaluateList<Model: Persistable>(
        type: Model.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        guard shouldEvaluate else {
            return
        }
        guard limit.map({ $0 >= 0 }) ?? true,
              offset.map({ $0 >= 0 }) ?? true else {
            throw denial(
                operation: .list,
                entity: Model.persistableType,
                reason: "Query limit and offset must be nonnegative"
            )
        }
        let handler = try registeredHandler(
            operation: .list,
            entity: Model.persistableType
        )
        let query = SecurityQuery(
            limit: limit.map(UInt64.init),
            offset: offset.map(UInt64.init),
            orderBy: orderBy
        )
        guard handler.permitsQuery(query, context: context) else {
            throw denial(
                operation: .list,
                entity: Model.persistableType,
                reason: "The registered policy denied the query"
            )
        }
    }

    public func evaluateGet(
        _ resource: borrowing any Persistable
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = type(of: resource).persistableType
        let handler = try registeredHandler(
            operation: .get,
            entity: entity,
            resourceID: String(describing: resource.id)
        )
        guard try handler.permitsRead(resource, context: context) else {
            throw denial(
                operation: .get,
                entity: entity,
                reason: "The registered policy denied the read",
                resourceID: String(describing: resource.id)
            )
        }
    }

    public func evaluateCreate(
        _ resource: borrowing any Persistable
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = type(of: resource).persistableType
        let handler = try registeredHandler(
            operation: .create,
            entity: entity,
            resourceID: String(describing: resource.id)
        )
        guard try handler.permitsCreate(resource, context: context) else {
            throw denial(
                operation: .create,
                entity: entity,
                reason: "The registered policy denied the create",
                resourceID: String(describing: resource.id)
            )
        }
    }

    public func evaluateUpdate(
        _ resource: borrowing any Persistable,
        newResource: borrowing any Persistable
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = type(of: newResource).persistableType
        let handler = try registeredHandler(
            operation: .update,
            entity: entity,
            resourceID: String(describing: newResource.id)
        )
        guard try handler.permitsUpdate(
            from: resource,
            to: newResource,
            context: context
        ) else {
            throw denial(
                operation: .update,
                entity: entity,
                reason: "The registered policy denied the update",
                resourceID: String(describing: newResource.id)
            )
        }
    }

    public func evaluateDelete(
        _ resource: borrowing any Persistable
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = type(of: resource).persistableType
        let handler = try registeredHandler(
            operation: .delete,
            entity: entity,
            resourceID: String(describing: resource.id)
        )
        guard try handler.permitsDelete(resource, context: context) else {
            throw denial(
                operation: .delete,
                entity: entity,
                reason: "The registered policy denied the delete",
                resourceID: String(describing: resource.id)
            )
        }
    }

    public func requireAdmin(
        operation: String,
        targetType: String
    ) throws {
        guard !configuration.isEnabled || isAdmin else {
            throw denial(
                operation: .admin,
                entity: targetType,
                reason: "\(operation) requires an administrator role"
            )
        }
    }

    private func registeredHandler(
        operation: SecurityError.Operation,
        entity: String,
        resourceID: String? = nil
    ) throws -> AuthorizationPolicyHandler {
        guard let handler = policies.handler(for: entity) else {
            throw denial(
                operation: operation,
                entity: entity,
                reason: "No authorization policy is registered for the entity",
                resourceID: resourceID
            )
        }
        return handler
    }

    private func denial(
        operation: SecurityError.Operation,
        entity: String,
        reason: String,
        resourceID: String? = nil
    ) -> SecurityError {
        SecurityError(
            operation: operation,
            targetType: entity,
            reason: reason,
            resourceID: resourceID,
            userID: userID
        )
    }
}

/// Explicitly bypasses authorization in isolated test runtimes.
public final class DisabledSecurityDelegate:
    DataStoreSecurityDelegate,
    Sendable
{
    public init() {}

    public func evaluateList<Model: Persistable>(
        type: Model.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {}

    public func evaluateGet(
        _ resource: borrowing any Persistable
    ) throws {}

    public func evaluateCreate(
        _ resource: borrowing any Persistable
    ) throws {}

    public func evaluateUpdate(
        _ resource: borrowing any Persistable,
        newResource: borrowing any Persistable
    ) throws {}

    public func evaluateDelete(
        _ resource: borrowing any Persistable
    ) throws {}

    public func requireAdmin(
        operation: String,
        targetType: String
    ) throws {}
}
