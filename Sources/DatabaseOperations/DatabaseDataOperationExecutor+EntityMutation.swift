import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseOperationCore

package extension DatabaseDataOperationExecutor {
    func makeEntityMutationExecutor(
        runtimeLimits: DatabaseOperationLimits
    ) -> DatabaseEntityMutationExecutor {
        DatabaseEntityMutationExecutor(
            container: container,
            runtimeLimits: runtimeLimits
        )
    }
}
