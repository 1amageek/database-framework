import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
/// Defines where a resumable operation establishes its durable checkpoint.
public enum DatabaseResumableOperationCommitModel: Sendable, Equatable {
    /// Operation effects and job state commit in one transaction.
    case atomicWithJobState

    /// The operation commits an idempotent durable checkpoint before the job
    /// runner persists progress. A retry must derive its next step from that
    /// operation-owned checkpoint. At a cancellation race, a completed
    /// checkpoint wins; an incomplete checkpoint is recorded before the job
    /// transitions to its cancelled unsuccessful outcome.
    case operationCheckpointed
}
