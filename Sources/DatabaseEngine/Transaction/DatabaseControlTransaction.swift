import DatabaseKit
import StorageKit

extension DBContainer {
    /// Executes one transaction against the control domain for framework-owned
    /// metadata. Callers at a user request boundary must perform the target
    /// Grant check before invoking this method; this method is intentionally
    /// package-scoped and never creates an authorization bypass on its own.
    @_spi(DatabaseExecution)
    public func withControlMetadataTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        #if DATABASE_MULTI_BASE
        let selectedTransactionExecutor = controlTransactionExecutor
        #else
        let selectedTransactionExecutor = transactionExecutor
        #endif
        return try await selectedTransactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
            try await self.withRootScopedDatabaseTransaction(
                storageAccess: storageAccess,
                dataRoot: self.controlStorage().root,
                accessMode: .readWrite,
                exposesPhysicalMaintenance: false,
                operation
            )
        }
    }

    #if DATABASE_MULTI_BASE
    /// Executes a control-domain transaction after evaluating the persisted
    /// database Grant in the same transaction attempt.
    @_spi(DatabaseExecution)
    public func withControlTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        let selectedTransactionExecutor = controlTransactionExecutor
        return try await selectedTransactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
            try await self.databaseGrantStore.require(
                requiredAccess,
                authorization: authorization,
                transaction: storageAccess
            )
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await self.withRootScopedDatabaseTransaction(
                    storageAccess: storageAccess,
                    dataRoot: self.controlStorage().root,
                    accessMode: requiredAccess == .read
                        ? .readOnly
                        : .readWrite,
                    exposesPhysicalMaintenance:
                        requiredAccess.contains(.administer),
                    operation
                )
            }
        }
    }
    #else
    /// Executes authorized control work against the ordinary database.
    /// The calling composition admits the operation while the framework binds
    /// its authorization context for entity and field policies.
    @_spi(DatabaseExecution)
    public func withControlTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await transactionExecutor.withTransaction(
            configuration: configuration,
            clock: monotonicClock,
            executionDeadline: executionDeadline
        ) { storageAccess in
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await self.withRootScopedDatabaseTransaction(
                    storageAccess: storageAccess,
                    dataRoot: self.databaseRoot,
                    accessMode: requiredAccess == .read
                        ? .readOnly
                        : .readWrite,
                    exposesPhysicalMaintenance:
                        requiredAccess.contains(.administer),
                    operation
                )
            }
        }
    }
    #endif

    package func withRootScopedDatabaseTransaction<Result: Sendable>(
        storageAccess: any TransactionAccess,
        dataRoot: Subspace,
        accessMode: DatabaseTransactionAccessMode,
        exposesPhysicalMaintenance: Bool = false,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        let operationScope = DatabaseReadScopeGate()
        let validationScope = DatabaseReadScopeGate()
        let admittedStorageAccess = DataRootTransactionAccess.admitted(
            storageAccess,
            dataRoot: dataRoot,
            accessMode: accessMode,
            readScope: operationScope
        )
        let validationStorageAccess = DataRootTransactionAccess.admitted(
            storageAccess,
            dataRoot: dataRoot,
            accessMode: .readOnly,
            readScope: validationScope
        )
        let transaction = DatabaseTransaction(
            storageAccess: admittedStorageAccess,
            validationStorageAccess: validationStorageAccess,
            container: self,
            maintenanceAccess: exposesPhysicalMaintenance
                ? DatabaseMaintenanceAccess(
                    compaction: storageAccess.compaction,
                    operationScope: operationScope
                )
                : nil
        )
        do {
            let result = try await operation(transaction)
            try await operationScope.closeAndWait()
            admittedStorageAccess.revoke()
            try await transaction.prepareForCommit()
            try await validationScope.closeAndWait()
            validationStorageAccess.revoke()
            return result
        } catch {
            var terminalError: any Error = error
            for scope in [operationScope, validationScope] {
                do {
                    try await scope.closeAndWait()
                } catch let cleanupError as DatabaseReadScopeCleanupError {
                    terminalError = cleanupError.preserving(
                        operationError: terminalError
                    )
                }
            }
            admittedStorageAccess.revoke()
            validationStorageAccess.revoke()
            await transaction.invalidate()
            throw terminalError
        }
    }
}
