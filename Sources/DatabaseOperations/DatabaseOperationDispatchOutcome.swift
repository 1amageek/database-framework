import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

/// The semantic outcome of one target-bound operation. The operation layer
/// never creates a transport frame; the Wire adapter consumes this value.
package enum DatabaseOperationDispatchOutcome: Sendable {
    case success(
        DatabaseOperationResult,
        context: DatabaseOperationContext
    )
    case failure(DatabaseOperationDispatchFailure)
}

package struct DatabaseOperationDispatchFailure: Sendable {
    package let error: any Error
    package let context: DatabaseOperationContext

    package init(
        error: any Error,
        context: DatabaseOperationContext
    ) {
        self.error = error
        self.context = context
    }
}
