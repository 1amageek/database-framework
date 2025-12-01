// KeyValidation.swift
// FDBIndexing - Key and value size validation for FoundationDB limits

import FoundationDB

// MARK: - FDB Limits

/// FoundationDB key size limit (10KB)
public let fdbMaxKeySize: Int = 10_000

/// FoundationDB value size limit (100KB)
public let fdbMaxValueSize: Int = 100_000

// MARK: - Validation Errors

/// Error thrown when FDB limits are violated
public enum FDBLimitError: Error, CustomStringConvertible {
    /// Key exceeds FDB's 10KB limit
    case keyTooLarge(size: Int, limit: Int)

    /// Value exceeds FDB's 100KB limit
    case valueTooLarge(size: Int, limit: Int)

    public var description: String {
        switch self {
        case .keyTooLarge(let size, let limit):
            return "Key size \(size) bytes exceeds FDB limit of \(limit) bytes"
        case .valueTooLarge(let size, let limit):
            return "Value size \(size) bytes exceeds FDB limit of \(limit) bytes"
        }
    }
}

// MARK: - Validation Functions

/// Validate that a key does not exceed FDB's size limit
///
/// - Parameter key: The key bytes to validate
/// - Throws: `FDBLimitError.keyTooLarge` if key exceeds 10KB
@inlinable
public func validateKeySize(_ key: FDB.Bytes) throws {
    if key.count > fdbMaxKeySize {
        throw FDBLimitError.keyTooLarge(size: key.count, limit: fdbMaxKeySize)
    }
}

/// Validate that a value does not exceed FDB's size limit
///
/// - Parameter value: The value bytes to validate
/// - Throws: `FDBLimitError.valueTooLarge` if value exceeds 100KB
@inlinable
public func validateValueSize(_ value: FDB.Bytes) throws {
    if value.count > fdbMaxValueSize {
        throw FDBLimitError.valueTooLarge(size: value.count, limit: fdbMaxValueSize)
    }
}

/// Validate key and return it if valid
///
/// - Parameter key: The key bytes to validate
/// - Returns: The validated key
/// - Throws: `FDBLimitError.keyTooLarge` if key exceeds 10KB
@inlinable
public func validatedKey(_ key: FDB.Bytes) throws -> FDB.Bytes {
    try validateKeySize(key)
    return key
}

/// Validate value and return it if valid
///
/// - Parameter value: The value bytes to validate
/// - Returns: The validated value
/// - Throws: `FDBLimitError.valueTooLarge` if value exceeds 100KB
@inlinable
public func validatedValue(_ value: FDB.Bytes) throws -> FDB.Bytes {
    try validateValueSize(value)
    return value
}
