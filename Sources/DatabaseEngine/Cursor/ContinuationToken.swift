// ContinuationToken.swift
// DatabaseEngine - Opaque token for resuming paginated queries
//
// Reference: FDB Record Layer RecordCursorContinuation
// Enables stateless pagination across transactions.

import Foundation
import FoundationDB

// MARK: - ContinuationToken

/// Opaque continuation token for resuming queries
///
/// Encapsulates all state needed to resume a query from where it left off.
/// Tokens are serialized to bytes and can be stored/transmitted as base64 strings.
///
/// **Key Properties**:
/// - Opaque to clients (internal format may change)
/// - Versioned for forward compatibility
/// - Contains plan fingerprint for validation
///
/// **Usage**:
/// ```swift
/// // First page
/// let result = try await context.cursor(User.self)
///     .where(\.isActive == true)
///     .limit(20)
///     .next()
///
/// // Store continuation for next request
/// let tokenString = result.continuation?.base64String
///
/// // Resume from token
/// if let tokenString = savedToken {
///     let token = try ContinuationToken.fromBase64(tokenString)
///     let nextResult = try await context.cursor(User.self, continuation: token).next()
/// }
/// ```
///
/// **Reference**: FDB Record Layer RecordCursorContinuation
public struct ContinuationToken: Sendable, Hashable {

    // MARK: - Constants

    /// Current token format version
    ///
    /// Increment when making breaking changes to token format.
    public static let currentVersion: UInt8 = 1

    // MARK: - Properties

    /// Raw serialized data (Tuple-encoded)
    public let data: [UInt8]

    // MARK: - Initialization

    /// Create from raw bytes
    public init(data: [UInt8]) {
        self.data = data
    }

    // MARK: - Serialization

    /// Serialize to base64 string for API transport
    ///
    /// Base64 encoding is URL-safe and suitable for query parameters.
    public var base64String: String {
        Data(data).base64EncodedString()
    }

    /// Parse from base64 string
    ///
    /// - Parameter string: Base64-encoded token
    /// - Returns: Parsed continuation token
    /// - Throws: `ContinuationError.invalidTokenFormat` if parsing fails
    public static func fromBase64(_ string: String) throws -> ContinuationToken {
        guard let data = Data(base64Encoded: string) else {
            throw ContinuationError.invalidTokenFormat
        }
        return ContinuationToken(data: Array(data))
    }

    // MARK: - Special Tokens

    /// Special token indicating end of results
    ///
    /// When a cursor reaches the end of data, it returns this token
    /// to indicate no more results are available.
    public static let endOfResults = ContinuationToken(data: [])

    /// Check if this is end of results
    public var isEndOfResults: Bool {
        data.isEmpty
    }

    // MARK: - Debugging

    /// Token size in bytes (for monitoring)
    public var byteCount: Int {
        data.count
    }
}

// MARK: - CustomStringConvertible

extension ContinuationToken: CustomStringConvertible {
    public var description: String {
        if isEndOfResults {
            return "ContinuationToken(endOfResults)"
        }
        return "ContinuationToken(\(byteCount) bytes)"
    }
}

// MARK: - NoNextReason

/// Reason why cursor stopped (following FDB Record Layer pattern)
///
/// When a cursor returns results without a continuation token,
/// this enum explains why iteration ended.
public enum NoNextReason: Sendable, Hashable, CustomStringConvertible {
    /// All data has been exhausted
    ///
    /// The query has returned all matching records.
    case sourceExhausted

    /// Return limit was reached
    ///
    /// The requested batch size or query limit was reached.
    /// More data may be available.
    case returnLimitReached

    /// Time limit exceeded (for long-running queries)
    ///
    /// The query took too long and was interrupted.
    /// Can be resumed from continuation.
    case timeLimitReached

    /// Transaction size limit approached
    ///
    /// Approaching FDB's 10MB transaction limit.
    /// Must commit and continue in a new transaction.
    case transactionLimitReached

    /// Scan count limit reached
    ///
    /// Maximum number of records scanned (for cost control).
    case scanLimitReached

    public var description: String {
        switch self {
        case .sourceExhausted:
            return "Source exhausted"
        case .returnLimitReached:
            return "Return limit reached"
        case .timeLimitReached:
            return "Time limit reached"
        case .transactionLimitReached:
            return "Transaction limit reached"
        case .scanLimitReached:
            return "Scan limit reached"
        }
    }
}

// MARK: - ContinuationError

/// Errors related to continuation tokens
public enum ContinuationError: Error, CustomStringConvertible, Sendable {
    /// Token data is not valid base64 or has invalid structure
    case invalidTokenFormat

    /// Token version doesn't match current version
    case versionMismatch(expected: UInt8, actual: UInt8)

    /// Token data is corrupted (checksum failed, incomplete, etc.)
    case corruptedToken

    /// Token has expired (if expiration is implemented)
    case tokenExpired

    /// Token was created for a different query
    ///
    /// Plan fingerprint doesn't match the current query.
    case planMismatch(String)

    /// Token scan type doesn't match current operation
    case scanTypeMismatch(expected: String, actual: String)

    public var description: String {
        switch self {
        case .invalidTokenFormat:
            return "Invalid continuation token format"
        case .versionMismatch(let expected, let actual):
            return "Token version mismatch: expected \(expected), got \(actual)"
        case .corruptedToken:
            return "Continuation token is corrupted"
        case .tokenExpired:
            return "Continuation token has expired"
        case .planMismatch(let reason):
            return "Plan mismatch: \(reason)"
        case .scanTypeMismatch(let expected, let actual):
            return "Scan type mismatch: expected \(expected), got \(actual)"
        }
    }
}
