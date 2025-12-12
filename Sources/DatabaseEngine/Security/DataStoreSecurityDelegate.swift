// DataStoreSecurityDelegate.swift
// DatabaseEngine - Security delegate protocol for DataStore

import Foundation
import Core

/// Security delegate protocol for DataStore
///
/// DataStore calls these methods before/after data operations to evaluate security.
/// The delegate is responsible for obtaining the current auth context (e.g., from TaskLocal)
/// and evaluating permissions based on SecurityPolicy.
///
/// **Design**:
/// - DataStore holds a reference to the delegate
/// - Auth context is obtained via TaskLocal (set per request)
/// - Delegate evaluates security and throws SecurityError if denied
public protocol DataStoreSecurityDelegate: Sendable {

    /// Evaluate LIST operation security
    ///
    /// Called before executing list/query operations.
    ///
    /// - Parameters:
    ///   - type: The Persistable type being queried
    ///   - limit: Query limit
    ///   - offset: Query offset
    ///   - orderBy: Query sort fields
    /// - Throws: SecurityError if LIST operation is not allowed
    func evaluateList<T: Persistable>(
        type: T.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws

    /// Evaluate GET operation security
    ///
    /// Called after fetching a resource to verify access is allowed.
    ///
    /// - Parameter resource: The fetched resource
    /// - Throws: SecurityError if GET operation is not allowed
    func evaluateGet(_ resource: any Persistable) throws

    /// Evaluate CREATE operation security
    ///
    /// Called before creating a new resource.
    ///
    /// - Parameter resource: The resource being created
    /// - Throws: SecurityError if CREATE operation is not allowed
    func evaluateCreate(_ resource: any Persistable) throws

    /// Evaluate UPDATE operation security
    ///
    /// Called before updating an existing resource.
    ///
    /// - Parameters:
    ///   - resource: The existing resource (before update)
    ///   - newResource: The updated resource
    /// - Throws: SecurityError if UPDATE operation is not allowed
    func evaluateUpdate(_ resource: any Persistable, newResource: any Persistable) throws

    /// Evaluate DELETE operation security
    ///
    /// Called before deleting a resource.
    ///
    /// - Parameter resource: The resource being deleted
    /// - Throws: SecurityError if DELETE operation is not allowed
    func evaluateDelete(_ resource: any Persistable) throws

    /// Require admin privileges
    ///
    /// Called for admin-only operations like clearAll.
    ///
    /// - Parameters:
    ///   - operation: The operation name (for error message)
    ///   - targetType: The target type name (for error message)
    /// - Throws: SecurityError if not admin
    func requireAdmin(operation: String, targetType: String) throws
}

// MARK: - TaskLocal Auth Context

/// TaskLocal storage for current auth context
///
/// Set this value at the beginning of each request to provide
/// authentication information to the security delegate.
///
/// **Usage**:
/// ```swift
/// // In request handler
/// try await AuthContextKey.$current.withValue(userAuth) {
///     let context = container.newContext()
///     // All operations in this scope use userAuth
///     try await context.save()
/// }
/// ```
public enum AuthContextKey {
    @TaskLocal public static var current: (any AuthContext)?
}

// MARK: - Default Security Delegate

/// Default security delegate implementation
///
/// Uses TaskLocal to obtain auth context and evaluates security
/// based on SecurityPolicy protocol conformance.
public final class DefaultSecurityDelegate: DataStoreSecurityDelegate, Sendable {

    /// Security configuration
    private let configuration: SecurityConfiguration

    public init(configuration: SecurityConfiguration) {
        self.configuration = configuration
    }

    /// Current auth context from TaskLocal
    private var auth: (any AuthContext)? {
        AuthContextKey.current
    }

    /// Whether security evaluation should be performed
    private var shouldEvaluate: Bool {
        guard configuration.isEnabled else { return false }
        guard let auth else { return true }  // Unauthenticated must be evaluated
        return auth.roles.isDisjoint(with: configuration.adminRoles)
    }

    /// Whether current auth has admin privileges
    private var isAdmin: Bool {
        guard let auth else { return false }
        return !auth.roles.isDisjoint(with: configuration.adminRoles)
    }

    // MARK: - DataStoreSecurityDelegate

    public func evaluateList<T: Persistable>(
        type: T.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        guard shouldEvaluate else { return }

        guard let secureType = T.self as? any SecurityPolicy.Type else {
            // strict モードでは SecurityPolicy 未実装を拒否
            if configuration.strict {
                throw SecurityError(
                    operation: .list,
                    targetType: T.persistableType,
                    reason: "Type does not implement SecurityPolicy. Implement SecurityPolicy or use strict: false."
                )
            }
            return
        }

        let allowed = secureType._evaluateList(
            limit: limit,
            offset: offset,
            orderBy: orderBy,
            auth: auth
        )

        guard allowed else {
            throw SecurityError(
                operation: .list,
                targetType: T.persistableType,
                reason: "Access denied: list operation not allowed"
            )
        }
    }

    public func evaluateGet(_ resource: any Persistable) throws {
        guard shouldEvaluate else { return }
        let modelType = type(of: resource)

        guard let secureType = modelType as? any SecurityPolicy.Type else {
            if configuration.strict {
                throw SecurityError(
                    operation: .get,
                    targetType: modelType.persistableType,
                    reason: "Type does not implement SecurityPolicy. Implement SecurityPolicy or use strict: false."
                )
            }
            return
        }

        let allowed = secureType._evaluateGet(resource: resource, auth: auth)

        guard allowed else {
            throw SecurityError(
                operation: .get,
                targetType: modelType.persistableType,
                reason: "Access denied: get operation not allowed"
            )
        }
    }

    public func evaluateCreate(_ resource: any Persistable) throws {
        guard shouldEvaluate else { return }
        let modelType = type(of: resource)

        guard let secureType = modelType as? any SecurityPolicy.Type else {
            if configuration.strict {
                throw SecurityError(
                    operation: .create,
                    targetType: modelType.persistableType,
                    reason: "Type does not implement SecurityPolicy. Implement SecurityPolicy or use strict: false."
                )
            }
            return
        }

        let allowed = secureType._evaluateCreate(newResource: resource, auth: auth)

        guard allowed else {
            throw SecurityError(
                operation: .create,
                targetType: modelType.persistableType,
                reason: "Access denied: create operation not allowed"
            )
        }
    }

    public func evaluateUpdate(_ resource: any Persistable, newResource: any Persistable) throws {
        guard shouldEvaluate else { return }
        let modelType = type(of: newResource)

        guard let secureType = modelType as? any SecurityPolicy.Type else {
            if configuration.strict {
                throw SecurityError(
                    operation: .update,
                    targetType: modelType.persistableType,
                    reason: "Type does not implement SecurityPolicy. Implement SecurityPolicy or use strict: false."
                )
            }
            return
        }

        let allowed = secureType._evaluateUpdate(
            resource: resource,
            newResource: newResource,
            auth: auth
        )

        guard allowed else {
            throw SecurityError(
                operation: .update,
                targetType: modelType.persistableType,
                reason: "Access denied: update operation not allowed"
            )
        }
    }

    public func evaluateDelete(_ resource: any Persistable) throws {
        guard shouldEvaluate else { return }
        let modelType = type(of: resource)

        guard let secureType = modelType as? any SecurityPolicy.Type else {
            if configuration.strict {
                throw SecurityError(
                    operation: .delete,
                    targetType: modelType.persistableType,
                    reason: "Type does not implement SecurityPolicy. Implement SecurityPolicy or use strict: false."
                )
            }
            return
        }

        let allowed = secureType._evaluateDelete(resource: resource, auth: auth)

        guard allowed else {
            throw SecurityError(
                operation: .delete,
                targetType: modelType.persistableType,
                reason: "Access denied: delete operation not allowed"
            )
        }
    }

    public func requireAdmin(operation: String, targetType: String) throws {
        guard isAdmin || !configuration.isEnabled else {
            throw SecurityError(
                operation: .admin,
                targetType: targetType,
                reason: "\(operation) requires admin privileges"
            )
        }
    }
}

// MARK: - Disabled Security Delegate

/// Security delegate that allows all operations (for testing)
///
/// **Warning**: Never use in production.
public final class DisabledSecurityDelegate: DataStoreSecurityDelegate, Sendable {

    public init() {}

    public func evaluateList<T: Persistable>(type: T.Type, limit: Int?, offset: Int?, orderBy: [String]?) throws {}
    public func evaluateGet(_ resource: any Persistable) throws {}
    public func evaluateCreate(_ resource: any Persistable) throws {}
    public func evaluateUpdate(_ resource: any Persistable, newResource: any Persistable) throws {}
    public func evaluateDelete(_ resource: any Persistable) throws {}
    public func requireAdmin(operation: String, targetType: String) throws {}
}
