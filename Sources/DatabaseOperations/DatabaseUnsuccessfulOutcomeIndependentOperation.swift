import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
public protocol DatabaseUnsuccessfulOutcomeIndependentOperation:
    DatabaseResumableOperation {}

public extension DatabaseUnsuccessfulOutcomeIndependentOperation {
    func applyUnsuccessfulOutcome(
        plan: Plan,
        state: State,
        outcome: DatabaseJobUnsuccessfulOutcome,
        context: DatabaseResumableOperationContext
    ) async throws {
        _ = plan
        _ = state
        _ = outcome
        _ = context
    }
}
