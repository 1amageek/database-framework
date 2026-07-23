public enum DatabaseMaintenanceRuntimeError: Error, Sendable, Equatable {
    case invalidInvocation(String)
    case invalidContinuation
    case invalidBatchSize(UInt32)
    case exactPartitionRequired(entity: String)
    case entityRequiredForPartitionFilter
    case migrationsNotResumable
    case compactionUnavailable
    case compactionRequiresJob
}
