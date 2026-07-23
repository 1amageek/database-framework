import DatabaseWire

/// Type-erased database error mapper.
public final class AnyDatabaseErrorMapper: DatabaseErrorMapper, Sendable {
    private let mapError: @Sendable (
        any Error,
        DatabaseOperationContext,
        DatabaseWireLimits
    ) -> DatabaseRemoteError

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
    ) -> DatabaseRemoteError {
        mapError(error, context, limits)
    }
}
