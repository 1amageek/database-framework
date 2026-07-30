// SecurityError.swift
// DatabaseEngine - Security error type

import DatabaseTypes

/// Security error thrown when access is denied
///
/// **Usage**:
/// ```swift
/// do {
///     let post = try await context.fetch(Post.self, id: postID)
/// } catch let error as SecurityError {
///     print("Access denied: \(error.operation) on \(error.targetType)")
///     print("Reason: \(error.reason)")
/// }
/// ```
public struct SecurityError: Error, Sendable, CustomStringConvertible {
    /// The operation that was denied
    public enum Operation: String, Sendable {
        case get
        case list
        case create
        case update
        case delete
        case admin
    }

    /// The operation that was attempted
    public let operation: Operation

    /// The type name of the target resource
    public let targetType: String

    /// Human-readable reason for the denial
    public let reason: String

    /// The typed resource identity that was denied, when available.
    public let resource: EntityReference?

    /// The user ID that attempted the operation (if available)
    public let userID: String?

    public init(
        operation: Operation,
        targetType: String,
        reason: String,
        resource: EntityReference? = nil,
        userID: String? = nil
    ) {
        self.operation = operation
        self.targetType = targetType
        self.reason = reason
        self.resource = resource
        self.userID = userID
    }

    public var description: String {
        var desc = "SecurityError: \(operation.rawValue) on \(targetType)"
        if let resource {
            desc += " (resource entity: \(resource.entity))"
        }
        if let userID {
            desc += " by user \(userID)"
        }
        desc += " - \(reason)"
        return desc
    }
}

// MARK: - LocalizedError Conformance

extension SecurityError {
    public var errorDescription: String? {
        "Access denied for \(operation.rawValue) operation on \(targetType)"
    }

    public var failureReason: String? {
        reason
    }
}
