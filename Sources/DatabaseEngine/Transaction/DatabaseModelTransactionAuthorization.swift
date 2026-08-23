import DatabaseKit

/// Identifies who authorizes a model transaction before framework capabilities
/// are minted.
package enum DatabaseModelTransactionAuthorization: Sendable {
    /// The current request must hold the specified persisted Grant.
    case request(Security.Access)

    /// A trusted execution host validates durable operation ownership inside
    /// the same control-metadata transaction before receiving data authority.
    case durableOperationOwner

    package var grantedAccess: Security.Access {
        switch self {
        case .request(let requiredAccess):
            requiredAccess.union([.read, .write])
        case .durableOperationOwner:
            [.read, .write]
        }
    }

    package var requiresPersistedGrant: Bool {
        switch self {
        case .request:
            true
        case .durableOperationOwner:
            false
        }
    }
}
