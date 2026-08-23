import DatabaseTypes
import StorageKit

/// Transaction access whose range cursors retain the container operation that
/// admitted their underlying transaction.
final class ContainerTransactionAccess: TransactionAccess, Sendable {
    private let transaction: any TransactionAccess
    private let operationLease: DatabaseStorageOperationLease

    init(
        transaction: any TransactionAccess,
        operationLease: DatabaseStorageOperationLease
    ) {
        self.transaction = transaction
        self.operationLease = operationLease
    }

    /// The backend-owned access used only when the container delegates a
    /// backend-specific namespace operation. The wrapper remains retained by
    /// the caller, so its operation lease stays active for the entire borrow.
    func namespaceWriteTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerNamespaceWriteTransactionBorrow {
        guard operationLease.belongs(to: lifecycle) else {
            throw StorageError.invalidOperation(
                "Namespace operations require a transaction admitted by the same database container"
            )
        }
        return ContainerNamespaceWriteTransactionBorrow(
            transaction: transaction,
            operationLease: operationLease
        )
    }

    func namespaceReadTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerNamespaceReadTransactionBorrow {
        try namespaceWriteTransactionBorrow(for: lifecycle).readOnly()
    }

    var capabilities: TransactionCapabilities {
        transaction.capabilities
    }

    var compaction: StorageCompactionAccess? {
        transaction.compaction
    }

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
        try await transaction.getKey(
            selector: selector,
            snapshot: snapshot
        )
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        do {
            let cursorLease = try operationLease.beginChildOperation()
            return transaction.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            ).retainingLifetime(of: cursorLease)
        } catch {
            return KeyValueCursor(
                consuming: FailedContainerRangeResult(error: error)
            )
        }
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

    func setOption(
        to value: Int,
        forOption option: TransactionOption
    ) throws {
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

    func requestVersionstamp() -> any PendingTransactionVersionstamp {
        transaction.requestVersionstamp()
    }
}

extension ContainerTransactionAccess:
    ContainerNamespaceReadTransactionBorrowing {}
