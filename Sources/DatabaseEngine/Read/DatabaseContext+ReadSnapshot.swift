import DatabaseKit
import StorageKit

extension DatabaseContext {
    /// Executes one read operation on a container-owned transaction snapshot.
    ///
    /// Historical-position configuration and verification happen before the
    /// transaction is attenuated. The operation receives only a scoped read
    /// session and immutable snapshot metadata.
    public func withReadSnapshot<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable @escaping (DatabaseReadSnapshot) async throws -> T
    ) async throws -> T {
        try await withReadSnapshot(
            restoring: nil,
            configuration: configuration,
            workMeter: workMeter,
            operation
        )
    }

    /// Restores a server-validated continuation position before attenuation.
    /// The SPI keeps read-version selection out of application-facing APIs.
    @_spi(DatabaseExecution)
    public func withReadSnapshot<T: Sendable>(
        restoring requestedPosition: DatabaseReadPosition?,
        configuration: TransactionConfiguration = .default,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable @escaping (DatabaseReadSnapshot) async throws -> T
    ) async throws -> T {
        try await withStorageAccess(
            requiredAccess: .read,
            configuration: configuration,
            restoringReadPosition: requestedPosition
        ) { transaction in
            guard let admittedTransaction = transaction
                as? ReadAuthorizedTransactionAccess else {
                throw DatabaseTransactionError.invalidOperationContext
            }
            let position = try await admittedTransaction
                .captureReadPosition()
            if let requestedPosition, position != requestedPosition {
                throw DatabaseReadPositionError.restoredPositionChanged(
                    expected: requestedPosition,
                    actual: position
                )
            }
            let supportsPositionRestoration: Bool = switch position {
            case .version:
                admittedTransaction.capabilities.historicalReadVersion
            case .opaque:
                false
            }
            return try await DatabaseReadSession.withSession(
                context: self,
                workMeter: workMeter
            ) { session in
                try await operation(
                    DatabaseReadSnapshot(
                        session: session,
                        position: position,
                        supportsPositionRestoration:
                            supportsPositionRestoration
                    )
                )
            }
        }
    }
}
