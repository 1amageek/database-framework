import DatabaseKit
import StorageKit

/// Authorizes database operations before their observable effects occur.
public protocol DataStoreSecurityDelegate: Sendable {
    func evaluateList(
        entity: String,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws

    func evaluateGet(
        _ resource: borrowing PersistedModel
    ) throws

    func evaluateCreate(
        _ resource: borrowing PersistedModel
    ) throws

    func evaluateUpdate(
        _ resource: borrowing PersistedModel,
        newResource: borrowing PersistedModel
    ) throws

    func evaluateDelete(
        _ resource: borrowing PersistedModel
    ) throws

    func requireAdmin(operation: String, targetType: String) throws
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

    public func evaluateList(
        entity: String,
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
                entity: entity,
                reason: "Query limit and offset must be nonnegative"
            )
        }
        let handler = try registeredHandler(
            operation: .list,
            entity: entity
        )
        let query = SecurityQuery(
            limit: limit.map(UInt64.init),
            offset: offset.map(UInt64.init),
            orderBy: orderBy
        )
        guard handler.permitsQuery(query, context: context) else {
            throw denial(
                operation: .list,
                entity: entity,
                reason: "The registered policy denied the query"
            )
        }
    }

    public func evaluateGet(
        _ resource: borrowing PersistedModel
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = resource.entity
        let handler = try registeredHandler(
            operation: .get,
            entity: entity,
            resource: nil
        )
        let decision = try handler.permitsRead(resource, context: context)
        guard decision.isPermitted else {
            throw denial(
                operation: .get,
                entity: entity,
                reason: "The registered policy denied the read",
                resource: decision.resource
            )
        }
    }

    public func evaluateCreate(
        _ resource: borrowing PersistedModel
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = resource.entity
        let handler = try registeredHandler(
            operation: .create,
            entity: entity,
            resource: nil
        )
        let decision = try handler.permitsCreate(resource, context: context)
        guard decision.isPermitted else {
            throw denial(
                operation: .create,
                entity: entity,
                reason: "The registered policy denied the create",
                resource: decision.resource
            )
        }
    }

    public func evaluateUpdate(
        _ resource: borrowing PersistedModel,
        newResource: borrowing PersistedModel
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = newResource.entity
        let handler = try registeredHandler(
            operation: .update,
            entity: entity,
            resource: nil
        )
        let decision = try handler.permitsUpdate(
            from: resource,
            to: newResource,
            context: context
        )
        guard decision.isPermitted else {
            throw denial(
                operation: .update,
                entity: entity,
                reason: "The registered policy denied the update",
                resource: decision.resource
            )
        }
    }

    public func evaluateDelete(
        _ resource: borrowing PersistedModel
    ) throws {
        guard shouldEvaluate else {
            return
        }
        let entity = resource.entity
        let handler = try registeredHandler(
            operation: .delete,
            entity: entity,
            resource: nil
        )
        let decision = try handler.permitsDelete(resource, context: context)
        guard decision.isPermitted else {
            throw denial(
                operation: .delete,
                entity: entity,
                reason: "The registered policy denied the delete",
                resource: decision.resource
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
        resource: EntityReference? = nil
    ) throws -> AuthorizationPolicyHandler {
        guard let handler = policies.handler(for: entity) else {
            throw denial(
                operation: operation,
                entity: entity,
                reason: "No authorization policy is registered for the entity",
                resource: resource
            )
        }
        return handler
    }

    private func denial(
        operation: SecurityError.Operation,
        entity: String,
        reason: String,
        resource: EntityReference? = nil
    ) -> SecurityError {
        SecurityError(
            operation: operation,
            targetType: entity,
            reason: reason,
            resource: resource,
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

    public func evaluateList(
        entity: String,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {}

    public func evaluateGet(
        _ resource: borrowing PersistedModel
    ) throws {}

    public func evaluateCreate(
        _ resource: borrowing PersistedModel
    ) throws {}

    public func evaluateUpdate(
        _ resource: borrowing PersistedModel,
        newResource: borrowing PersistedModel
    ) throws {}

    public func evaluateDelete(
        _ resource: borrowing PersistedModel
    ) throws {}

    public func requireAdmin(
        operation: String,
        targetType: String
    ) throws {}
}
