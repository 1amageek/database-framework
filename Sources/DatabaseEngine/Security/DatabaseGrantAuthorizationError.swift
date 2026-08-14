#if DATABASE_MULTIPLE_BASES
import DatabaseKit

/// Persisted-Grant authorization and mutation failures.
public enum DatabaseGrantAuthorizationError: Error, Sendable, Equatable {
    case unauthenticated
    case denied(resource: Security.Resource, required: Security.Access)
    case resourceMismatch(
        expected: Security.Resource,
        actual: Security.Resource
    )
    case invalidSubject
    case invalidAccessBits(UInt8)
    case lastAdministrator
    case revisionConflict(expected: UInt64, actual: UInt64)
    case revisionOverflow
    case corruptedGrant
}
#endif
