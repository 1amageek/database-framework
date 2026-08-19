public enum DatabaseMigrationAdmissionError: Error, Sendable, Equatable,
    CustomStringConvertible
{
    case migrationRequired
    case migrationInProgress
    case staleSchemaGeneration(required: UInt64, actual: UInt64)
    case operationLimitExceeded

    public var description: String {
        switch self {
        case .migrationRequired:
            return "Database migration must complete before data operations are admitted"
        case .migrationInProgress:
            return "Database migration is already in progress"
        case .staleSchemaGeneration(let required, let actual):
            return
                "Database operation uses stale schema generation \(actual); generation \(required) or newer is required"
        case .operationLimitExceeded:
            return "Database migration admission operation count overflowed"
        }
    }
}
