import DatabaseTypes
import StorageKit

/// Storage access admitted for reads while retaining the underlying snapshot
/// and container-operation lifetime.
///
/// `TransactionAccess` combines reads and mutations in StorageKit. Database
/// authorization is narrower: a caller admitted with only Base read access
/// must not inherit the mutation methods of that storage protocol. This
/// adapter preserves read behavior and rejects every persistent mutation.
final class ReadAuthorizedTransactionAccess:
    TransactionAccess,
    Sendable
{
    private let transaction: any TransactionAccess

    private init(transaction: any TransactionAccess) {
        self.transaction = transaction
    }

    static func admitted(
        _ transaction: any TransactionAccess
    ) -> any TransactionAccess {
        if transaction is ReadAuthorizedTransactionAccess {
            return transaction
        }
        return ReadAuthorizedTransactionAccess(transaction: transaction)
    }

    func namespaceTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerNamespaceTransactionBorrow {
        if let transaction = transaction as? ContainerTransactionAccess {
            return try transaction.namespaceTransactionBorrow(for: lifecycle)
        }
        if let transaction = transaction as? ContainerTransaction {
            return try transaction.namespaceTransactionBorrow(for: lifecycle)
        }
        throw StorageError.invalidOperation(
            "Namespace reads require a transaction admitted by the same database container"
        )
    }

    var capabilities: TransactionCapabilities {
        transaction.capabilities
    }

    var transactionDomain: StorageTransactionDomain {
        transaction.transactionDomain
    }

    /// Physical compaction is a mutation capability and is not exposed through
    /// read-authorized access.
    var compaction: StorageCompactionAccess? {
        nil
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
        )
    }

    func setValue(_ value: ByteString, for key: ByteString) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func clear(key: ByteString) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
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
