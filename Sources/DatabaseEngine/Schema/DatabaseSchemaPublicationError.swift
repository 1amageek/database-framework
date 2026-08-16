import DatabaseKit

public enum DatabaseSchemaPublicationError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case fingerprintConflict(
        expected: SchemaFingerprint,
        actual: SchemaFingerprint
    )
    case idempotencyKeyReused(String)
    case transitionInProgress
    case invalidIdempotencyKey
    case persistentIndexBuildJobRequired
    case generationOverflow
    case corruptedState(String)

    public var description: String {
        switch self {
        case .fingerprintConflict:
            return "The expected schema fingerprint does not match the active schema"
        case .idempotencyKeyReused(let key):
            return "Schema idempotency key '\(key)' was already used for a different schema"
        case .transitionInProgress:
            return "A schema transition is already in progress"
        case .invalidIdempotencyKey:
            return "Schema idempotency key must not be empty"
        case .persistentIndexBuildJobRequired:
            return "Schema publication requires an atomic persistent index-build job"
        case .generationOverflow:
            return "Schema generation exhausted UInt64"
        case .corruptedState(let reason):
            return "Persisted schema publication state is invalid: \(reason)"
        }
    }
}
