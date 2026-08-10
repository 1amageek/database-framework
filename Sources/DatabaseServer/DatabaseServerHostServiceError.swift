public enum DatabaseServerHostServiceError: Error, Sendable, Equatable {
    case missingJobScheduler
    case missingSchemaApplyJobOperation
}
