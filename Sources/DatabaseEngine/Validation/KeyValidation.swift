// KeyValidation.swift
// DatabaseEngine - Portable key and value size validation

import DatabaseTypes
import StorageKit

// MARK: - Portable Storage Limits

/// Canonical key limit chosen for portability across supported backends.
public let databaseMaximumKeySize: Int = 10_000

/// Canonical value limit chosen for portability across supported backends.
public let databaseMaximumValueSize: Int = 100_000

// MARK: - Validation Errors

/// Error thrown when the canonical physical storage limits are violated.
public enum DatabaseStorageLimitError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    /// Key exceeds the portable 10KB limit.
    case keyTooLarge(size: Int, limit: Int)

    /// Value exceeds the portable 100KB limit.
    case valueTooLarge(size: Int, limit: Int)

    public var description: String {
        switch self {
        case .keyTooLarge(let size, let limit):
            return "Key size \(size) bytes exceeds the portable storage limit of \(limit) bytes"
        case .valueTooLarge(let size, let limit):
            return "Value size \(size) bytes exceeds the portable storage limit of \(limit) bytes"
        }
    }
}

// MARK: - Validation Functions

/// Validate that a key does not exceed FDB's size limit
///
/// - Parameter key: The key bytes to validate
/// - Throws: `DatabaseStorageLimitError.keyTooLarge` if key exceeds 10KB
@inlinable
public func validateKeySize(_ key: ByteString) throws {
    if key.count > databaseMaximumKeySize {
        throw DatabaseStorageLimitError.keyTooLarge(
            size: key.count,
            limit: databaseMaximumKeySize
        )
    }
}

/// Validate that a value does not exceed FDB's size limit
///
/// - Parameter value: The value bytes to validate
/// - Throws: `DatabaseStorageLimitError.valueTooLarge` if value exceeds 100KB
@inlinable
public func validateValueSize(_ value: ByteString) throws {
    if value.count > databaseMaximumValueSize {
        throw DatabaseStorageLimitError.valueTooLarge(
            size: value.count,
            limit: databaseMaximumValueSize
        )
    }
}

/// Validate key and return it if valid
///
/// - Parameter key: The key bytes to validate
/// - Returns: The validated key
/// - Throws: `DatabaseStorageLimitError.keyTooLarge` if key exceeds 10KB
@inlinable
public func validatedKey(_ key: ByteString) throws -> ByteString {
    try validateKeySize(key)
    return key
}

/// Validate value and return it if valid
///
/// - Parameter value: The value bytes to validate
/// - Returns: The validated value
/// - Throws: `DatabaseStorageLimitError.valueTooLarge` if value exceeds 100KB
@inlinable
public func validatedValue(_ value: ByteString) throws -> ByteString {
    try validateValueSize(value)
    return value
}
