@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseSchemaExecutionError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case migrationRequired([SchemaExecuteOperation.CompatibilityIssue])
    case runtimeUnavailable
    case storageCapabilityUnavailable(
        indexName: String,
        kindIdentifier: String,
        capability: String
    )
    case persistentJobServiceUnavailable

    public var description: String {
        switch self {
        case .migrationRequired(let issues):
            return issues.map { $0.message }.joined(separator: " | ")
        case .runtimeUnavailable:
            return "Schema runtime is unavailable"
        case .storageCapabilityUnavailable(
            let indexName,
            let kindIdentifier,
            let capability
        ):
            return "Index '\(indexName)' of kind '\(kindIdentifier)' requires unavailable storage capability '\(capability)'"
        case .persistentJobServiceUnavailable:
            return "Schema index backfill requires the persistent job service"
        }
    }
}
