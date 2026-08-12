import DatabaseKit

/// Opaque authority-owned reference retained by a persistent job.
///
/// The value identifies an authentication record, not a principal snapshot.
/// Roles and claims are resolved again before every productive job slice.
public struct DatabaseJobAuthorizationReference: Sendable, Hashable {
    public static let maximumUTF8ByteCount = 512

    public let value: String

    public init(_ value: String) throws {
        guard !value.isEmpty,
              value.utf8.count <= Self.maximumUTF8ByteCount else {
            throw DatabaseJobAuthorizationError.invalidReference
        }
        self.value = value
    }
}

public enum DatabaseJobAuthorizationError: Error, Sendable, Equatable {
    case invalidReference
    case referenceRequired
    case validatorUnavailable
    case principalChanged
    case revalidationFailed
}

/// Host authority consulted before every productive persistent-job slice.
public protocol DatabaseJobAuthorizationValidating: Sendable {
    func revalidate(
        _ reference: DatabaseJobAuthorizationReference
    ) async throws -> AuthorizationContext
}

public final class AnyDatabaseJobAuthorizationValidator: Sendable {
    private let revalidateReference: @Sendable (
        DatabaseJobAuthorizationReference
    ) async throws -> AuthorizationContext

    public init<Validator: DatabaseJobAuthorizationValidating>(
        _ validator: Validator
    ) {
        self.revalidateReference = validator.revalidate
    }

    public func revalidate(
        _ reference: DatabaseJobAuthorizationReference
    ) async throws -> AuthorizationContext {
        try await revalidateReference(reference)
    }
}
