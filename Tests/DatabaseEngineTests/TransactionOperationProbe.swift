import DatabaseTypes
import StorageKit
import Synchronization

/// Observes compound admission while delegating actual storage behavior.
final class TransactionOperationProbe: Transaction, Sendable {
    private struct State: Sendable {
        var depth = 0
        var operationCount = 0
        var minimumReadDepth: Int?
    }

    private let transaction: any Transaction
    private let state = Mutex(State())

    init(transaction: any Transaction) {
        self.transaction = transaction
    }

    var depth: Int { state.withLock { $0.depth } }
    var operationCount: Int { state.withLock { $0.operationCount } }
    var minimumReadDepth: Int? { state.withLock { $0.minimumReadDepth } }
    var transactionDomain: StorageTransactionDomain { transaction.transactionDomain }
    var capabilities: TransactionCapabilities { transaction.capabilities }
    var checkpoints: (any TransactionCheckpointAccess)? { transaction.checkpoints }
    var storageFailure: StorageError? { transaction.storageFailure }

    func resetReadDepth() {
        state.withLock { $0.minimumReadDepth = nil }
    }

    func performOperation(
        _ operation: @escaping @Sendable () async throws -> Void
    ) async throws {
        state.withLock {
            $0.depth += 1
            $0.operationCount += 1
        }
        defer { state.withLock { $0.depth -= 1 } }
        try await transaction.performOperation(operation)
    }

    private func recordRead() {
        state.withLock {
            $0.minimumReadDepth = min($0.minimumReadDepth ?? $0.depth, $0.depth)
        }
    }

    func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
        recordRead()
        return try await transaction.getValue(for: key, snapshot: snapshot)
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        recordRead()
        return try await transaction.getValue(for: key)
    }

    func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? {
        recordRead()
        return try await transaction.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        recordRead()
        return transaction.rangeCursor(from: begin, to: end, limit: limit,
            reverse: reverse, snapshot: snapshot, streamingMode: streamingMode)
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

    func atomicOp(key: ByteString, param: ByteString, mutationType: MutationType) throws {
        try transaction.atomicOp(key: key, param: param, mutationType: mutationType)
    }

    func configureCheckpoints(limits: TransactionCheckpointLimits) throws {
        try transaction.configureCheckpoints(limits: limits)
    }

    func commit() async throws { try await transaction.commit() }
    func cancel() async throws { try await transaction.cancel() }
    func getCommittedVersion() throws -> Int64 { try transaction.getCommittedVersion() }
}
