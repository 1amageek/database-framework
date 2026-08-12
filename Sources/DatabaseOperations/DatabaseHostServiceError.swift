import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
public enum DatabaseHostServiceError: Error, Sendable, Equatable {
    case missingJobScheduler
    case missingJobAuthorizationValidator
    case missingSchemaApplyJobOperation
}
