import DatabaseTypes
import StorageKit

/// A storage transaction that keeps its database container lifecycle active
/// until the owned transaction is released.
final class ContainerTransaction: Transaction, Sendable {
    private let transaction: any Transaction
    private let operationLease: DatabaseStorageOperationLease

    init(
        transaction: any Transaction,
        operationLease: DatabaseStorageOperationLease
    ) {
        self.transaction = transaction
        self.operationLease = operationLease
    }

    /// The backend-owned access used only when the container delegates a
    /// backend-specific namespace operation. The wrapper remains retained by
    /// the caller, so its operation lease stays active for the entire borrow.
    func namespaceTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerNamespaceTransactionBorrow {
        guard operationLease.belongs(to: lifecycle) else {
            throw StorageError.invalidOperation(
                "Namespace operations require a transaction admitted by the same database container"
            )
        }
        return ContainerNamespaceTransactionBorrow(
            transaction: transaction,
            operationLease: operationLease
        )
    }

    var capabilities: TransactionCapabilities {
        transaction.capabilities
    }

    var compaction: StorageCompactionAccess? {
        transaction.compaction
    }

    var transactionDomain: StorageTransactionDomain {
        transaction.transactionDomain
    }

    var storageFailure: StorageError? {
        transaction.storageFailure
    }

    var mutationByteLimit: Int? {
        transaction.mutationByteLimit
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
        transaction.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        ).retainingLifetime(of: operationLease)
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

    func configureMutationByteLimit(maximumBytes: Int?) throws {
        try transaction.configureMutationByteLimit(
            maximumBytes: maximumBytes
        )
    }

    func commit() async throws {
        try await transaction.commit()
    }

    func cancel() async throws {
        try await transaction.cancel()
    }

    func getCommittedVersion() throws -> Int64 {
        try transaction.getCommittedVersion()
    }
}
