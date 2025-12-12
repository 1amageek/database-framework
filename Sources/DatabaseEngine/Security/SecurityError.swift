// SecurityError.swift
// DatabaseEngine - Security error type

import Foundation

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
    }

    /// The operation that was attempted
    public let operation: Operation

    /// The type name of the target resource
    public let targetType: String

    /// Human-readable reason for the denial
    public let reason: String

    public init(operation: Operation, targetType: String, reason: String) {
        self.operation = operation
        self.targetType = targetType
        self.reason = reason
    }

    public var description: String {
        "SecurityError: \(operation.rawValue) on \(targetType) - \(reason)"
    }
}

// MARK: - LocalizedError Conformance

extension SecurityError: LocalizedError {
    public var errorDescription: String? {
        "Access denied for \(operation.rawValue) operation on \(targetType)"
    }

    public var failureReason: String? {
        reason
    }
}
