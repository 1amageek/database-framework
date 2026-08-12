import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

/// Type-erased database error mapper.
public final class AnyDatabaseErrorMapper: DatabaseErrorMapper, Sendable {
    private let mapError: @Sendable (
        any Error,
        DatabaseOperationContext,
        DatabaseWireLimits
    ) -> RemoteOperationError

    public init<Mapper: DatabaseErrorMapper>(_ mapper: Mapper) {
        self.mapError = { error, context, limits in
            mapper.remoteError(
                for: error,
                context: context,
                limits: limits
            )
        }
    }

    public func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> RemoteOperationError {
        mapError(error, context, limits)
    }
}
