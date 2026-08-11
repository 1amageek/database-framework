public enum DatabaseHostServiceError: Error, Sendable, Equatable {
    case missingJobScheduler
    case missingJobAuthorizationValidator
    case missingSchemaApplyJobOperation
}
