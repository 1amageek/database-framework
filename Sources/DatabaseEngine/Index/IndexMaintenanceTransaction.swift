import DatabaseTypes
import StorageKit

/// Failures raised while constructing an index-maintenance capability.
public enum IndexMaintenanceAccessError: Error, Sendable, Equatable {
    case invalidIndexSubspace
}

/// Storage capability confined to one physical index.
///
/// Transaction lifecycle and database-root authority are deliberately absent
/// from the semantic contract even though StorageKit's compatibility protocol
/// supplies rejecting control methods.
public protocol IndexMaintenanceTransactionAccess: TransactionAccess {}

private struct ScopedIndexMaintenanceTransactionAccess:
    IndexMaintenanceTransactionAccess
{
    let transaction: DataRootTransactionAccess

    var capabilities: TransactionCapabilities { transaction.capabilities }
    var compaction: StorageCompactionAccess? { nil }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await transaction.getValue(for: key, snapshot: snapshot)
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await transaction.getValue(for: key)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await transaction.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        transaction.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }

    func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        try await transaction.getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
    }

    func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        try await transaction.getRangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize
        )
    }

    func setValue(_ value: ByteString, for key: ByteString) throws {
        try transaction.setValue(value, for: key)
    }

    func clear(key: ByteString) throws {
        try transaction.clear(key: key)
    }

    func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try transaction.clearRange(beginKey: beginKey, endKey: endKey)
    }

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        try transaction.atomicOp(
            key: key,
            param: param,
            mutationType: mutationType
        )
    }

    func setReadVersion(_ version: Int64) throws {
        try transaction.setReadVersion(version)
    }

    func getReadVersion() async throws -> Int64 {
        try await transaction.getReadVersion()
    }

    func setOption(forOption option: TransactionOption) throws {
        try transaction.setOption(forOption: option)
    }

    func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws {
        try transaction.setOption(to: value, forOption: option)
    }

    func setOption(to value: Int, forOption option: TransactionOption) throws {
        try transaction.setOption(to: value, forOption: option)
    }

    func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        try transaction.addConflictRange(
            beginKey: beginKey,
            endKey: endKey,
            type: type
        )
    }

    func requestVersionstamp() -> any PendingTransactionVersionstamp {
        transaction.requestVersionstamp()
    }
}

/// Runs one maintainer callback with storage authority confined to its index.
///
/// The facade supports the storage operations needed by physical index
/// implementations, but every point, range, selector, metric, atomic, and
/// conflict operation remains inside `indexSubspace`. It is revoked after the
/// callback and all cursors opened by the callback have been drained.
package func withIndexMaintenanceTransaction<Result: Sendable>(
    transaction: any TransactionAccess,
    indexSubspace: Subspace,
    _ operation: @Sendable (
        any IndexMaintenanceTransactionAccess
    ) async throws -> Result
) async throws -> Result {
    guard !indexSubspace.prefix.isEmpty else {
        throw IndexMaintenanceAccessError.invalidIndexSubspace
    }
    let operationScope = DatabaseReadScopeGate()
    let admittedTransaction = DataRootTransactionAccess.admitted(
        transaction,
        dataRoot: indexSubspace,
        accessMode: .readWrite,
        readScope: operationScope
    )
    let result: Result
    do {
        result = try await operation(
            ScopedIndexMaintenanceTransactionAccess(
                transaction: admittedTransaction
            )
        )
    } catch {
        let operationError = error
        do {
            try await operationScope.closeAndWait()
        } catch let cleanupError as DatabaseReadScopeCleanupError {
            admittedTransaction.revoke()
            throw cleanupError.preserving(operationError: operationError)
        }
        admittedTransaction.revoke()
        throw operationError
    }
    do {
        try await operationScope.closeAndWait()
        admittedTransaction.revoke()
        return result
    } catch {
        admittedTransaction.revoke()
        throw error
    }
}
