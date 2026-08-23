import StorageKit

/// A database-administration capability for transaction-scoped physical
/// maintenance. It is separate from data-root mutation access because a
/// physical backend operation is not scoped to one Base key range.
@_spi(DatabaseExecution)
public struct DatabaseMaintenanceAccess: Sendable {
    public let compaction: DatabaseCompactionAccess?

    package init(
        compaction: StorageCompactionAccess?,
        operationScope: DatabaseReadScopeGate
    ) {
        self.compaction = compaction.map {
            DatabaseCompactionAccess(
                storageAccess: $0,
                operationScope: operationScope
            )
        }
    }
}

/// Operation-scoped access to physical storage compaction.
@_spi(DatabaseExecution)
public struct DatabaseCompactionAccess: Sendable {
    public let limits: StorageCompactionLimits

    private let storageAccess: StorageCompactionAccess
    private let operationScope: DatabaseReadScopeGate

    package init(
        storageAccess: StorageCompactionAccess,
        operationScope: DatabaseReadScopeGate
    ) {
        self.limits = storageAccess.limits
        self.storageAccess = storageAccess
        self.operationScope = operationScope
    }

    public func stageSlice(
        maximumWorkUnits: UInt64,
        continuation: StorageCompactionContinuation?
    ) async throws -> StorageCompactionResult {
        let lease: DatabaseReadScopeLease
        do {
            lease = try operationScope.beginRead()
        } catch DatabaseReadTransactionError.snapshotClosed {
            throw DatabaseMaintenanceAccessError.operationClosed
        }
        defer { lease.finish() }
        return try await storageAccess.stageSlice(
            maximumWorkUnits: maximumWorkUnits,
            continuation: continuation
        )
    }
}

@_spi(DatabaseExecution)
public enum DatabaseMaintenanceAccessError: Error, Sendable, Equatable {
    case operationClosed
}
