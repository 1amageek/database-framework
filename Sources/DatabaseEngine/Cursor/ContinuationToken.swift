// ContinuationToken.swift
// DatabaseEngine - Opaque token for resuming paginated queries
//
// Reference: FDB Record Layer RecordCursorContinuation
// Enables stateless pagination across transactions.

import DatabaseTypes

// MARK: - ContinuationToken

/// Opaque, non-empty continuation token for resuming queries.
///
/// Encapsulates all state needed to resume a query from where it left off.
/// Tokens are serialized to bytes and can be stored/transmitted as base64 strings.
///
/// **Key Properties**:
/// - Opaque to clients (internal format may change)
/// - Uses one strict protocol version
/// - Contains plan fingerprint for validation
///
/// **Usage**:
/// ```swift
/// // First page
/// let result = try await context.cursor(User.self)
///     .where(User.fields.isActive == true)
///     .limit(20)
///     .next()
///
/// // Store continuation for next request
/// let tokenString = result.continuation?.base64URLString
///
/// // Resume from token
/// if let tokenString = savedToken {
///     let token = try ContinuationToken(base64URLString: tokenString)
///     let nextResult = try await context.cursor(User.self, continuation: token).next()
/// }
/// ```
///
/// **Reference**: FDB Record Layer RecordCursorContinuation
public struct ContinuationToken: Sendable, Hashable {

    // MARK: - Constants

    /// Current token format version
    ///
    /// The runtime accepts exactly this version.
    internal static let currentVersion: UInt8 = 1

    // MARK: - Properties

    /// Raw canonical token bytes.
    internal let data: ByteString

    // MARK: - Initialization

    /// Creates a token from canonical runtime bytes.
    internal init(data: ByteString) throws {
        guard !data.isEmpty else {
            throw ContinuationError.invalidTokenFormat
        }
        guard data.count <= ContinuationStateFormat.maximumByteCount else {
            throw ContinuationError.tokenTooLarge(
                actual: data.count,
                maximum: ContinuationStateFormat.maximumByteCount
            )
        }
        self.data = data
    }

    // MARK: - Serialization

    /// Serializes the token for URL-safe API transport.
    ///
    /// Base64 encoding is URL-safe and suitable for query parameters.
    public var base64URLString: String {
        Base64URLFormat.encode(data)
    }

    /// Creates a token from its unpadded RFC 4648 base64url representation.
    ///
    /// - Parameter base64URLString: URL-safe encoded token.
    /// - Throws: `ContinuationError.invalidTokenFormat` if parsing fails
    public init(base64URLString: String) throws {
        let data: ByteString
        do {
            data = try Base64URLFormat.decode(
                base64URLString,
                maximumDecodedByteCount:
                    ContinuationStateFormat.maximumByteCount
            )
        } catch {
            throw ContinuationError.invalidTokenFormat
        }
        try self.init(data: data)
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
    /// The query has returned all matching entities.
    case sourceExhausted

    /// Return limit was reached
    ///
    /// The query's total result limit was reached.
    case returnLimitReached

    public var description: String {
        switch self {
        case .sourceExhausted:
            return "Source exhausted"
        case .returnLimitReached:
            return "Return limit reached"
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

    /// Token data is structurally invalid or incomplete.
    case corruptedToken

    /// Token exceeds the bounded continuation frame size.
    case tokenTooLarge(actual: Int, maximum: Int)

    /// Token was created for a different query
    ///
    /// Plan fingerprint doesn't match the current query.
    case planMismatch(String)

    public var description: String {
        switch self {
        case .invalidTokenFormat:
            return "Invalid continuation token format"
        case .versionMismatch(let expected, let actual):
            return "Token version mismatch: expected \(expected), got \(actual)"
        case .corruptedToken:
            return "Continuation token is corrupted"
        case .tokenTooLarge(let actual, let maximum):
            return "Continuation token has \(actual) bytes; maximum is \(maximum)"
        case .planMismatch(let reason):
            return "Plan mismatch: \(reason)"
        }
    }
}
