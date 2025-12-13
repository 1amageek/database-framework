// FieldSecurityError.swift
// DatabaseEngine - Field-level security errors

import Foundation

/// Error thrown when field-level security check fails
///
/// **Usage**:
/// ```swift
/// do {
///     try await context.saveSecure()
/// } catch let error as FieldSecurityError {
///     switch error {
///     case .readNotAllowed(let type, let fields):
///         print("Cannot read \(fields) on \(type)")
///     case .writeNotAllowed(let type, let fields):
///         print("Cannot write \(fields) on \(type)")
///     }
/// }
/// ```
public enum FieldSecurityError: Error, Sendable, Equatable {
    /// Read access denied for specified fields
    case readNotAllowed(type: String, fields: [String])

    /// Write access denied for specified fields
    case writeNotAllowed(type: String, fields: [String])
}

// MARK: - CustomStringConvertible

extension FieldSecurityError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .readNotAllowed(let type, let fields):
            return "Read access denied for \(type).\(fields.joined(separator: ", "))"
        case .writeNotAllowed(let type, let fields):
            return "Write access denied for \(type).\(fields.joined(separator: ", "))"
        }
    }
}

// MARK: - LocalizedError

extension FieldSecurityError: LocalizedError {
    public var errorDescription: String? {
        description
    }

    public var failureReason: String? {
        switch self {
        case .readNotAllowed(_, let fields):
            return "Insufficient permissions to read fields: \(fields.joined(separator: ", "))"
        case .writeNotAllowed(_, let fields):
            return "Insufficient permissions to write fields: \(fields.joined(separator: ", "))"
        }
    }
}
