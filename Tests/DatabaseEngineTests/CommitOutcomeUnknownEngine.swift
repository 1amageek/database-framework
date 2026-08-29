import DatabaseTypes
import StorageKit
import Synchronization

final class CommitOutcomeUnknownEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        init() {}
    }

    typealias TransactionType = CommitOutcomeUnknownTransaction

    fileprivate final class OutcomeState: Sendable {
        private let reportsUnknownOutcome = Mutex(false)

        func arm() {
            reportsUnknownOutcome.withLock { $0 = true }
        }

        func consume() -> Bool {
            reportsUnknownOutcome.withLock { reportsUnknownOutcome in
                guard reportsUnknownOutcome else {
                    return false
                }
                reportsUnknownOutcome = false
                return true
            }
        }
    }

    final class CommitOutcomeUnknownTransaction: Transaction, Sendable {
        typealias RangeResult = KeyValueRangeResult

        private let underlying: InMemoryTransaction
        private let outcomeState: OutcomeState

        var capabilities: TransactionCapabilities {
            underlying.capabilities
        }

        var mutationByteLimit: Int? {
            underlying.mutationByteLimit
        }
        var transactionDomain: StorageTransactionDomain {
            underlying.transactionDomain
        }
        var storageFailure: StorageError? {
            underlying.storageFailure
        }

        fileprivate init(
            underlying: InMemoryTransaction,
            outcomeState: OutcomeState
        ) {
            self.underlying = underlying
            self.outcomeState = outcomeState
        }

        func configureMutationByteLimit(maximumBytes: Int?) throws {
            try underlying.configureMutationByteLimit(
                maximumBytes: maximumBytes
            )
        }

        func getValue(
            for key: ByteString,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getValue(for key: ByteString) async throws -> ByteString? {
            try await underlying.getValue(for: key)
        }

        func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await underlying.getKey(
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
            underlying.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }

        func setValue(_ value: ByteString, for key: ByteString) throws {
            try underlying.setValue(value, for: key)
        }

        func clear(key: ByteString) throws {
            try underlying.clear(key: key)
        }

        func clearRange(beginKey: ByteString, endKey: ByteString) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        func atomicOp(
            key: ByteString,
            param: ByteString,
            mutationType: MutationType
        ) throws {
            try underlying.atomicOp(
                key: key,
                param: param,
                mutationType: mutationType
            )
        }

        func commit() async throws {
            try await underlying.commit()
            guard outcomeState.consume() else {
                return
            }
            throw StorageError(
                code: .commitUnknownResult,
                message: "The commit completed but its outcome is ambiguous"
            )
        }

        func cancel() async throws {
            try await underlying.cancel()
        }

        func setReadVersion(_ version: Int64) throws {
            try underlying.setReadVersion(version)
        }

        func getReadVersion() async throws -> Int64 {
            try await underlying.getReadVersion()
        }

        func getCommittedVersion() throws -> Int64 {
            try underlying.getCommittedVersion()
        }

        func setOption(forOption option: TransactionOption) throws {
            try underlying.setOption(forOption: option)
        }

        func setOption(
            to value: ByteString?,
            forOption option: TransactionOption
        ) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func setOption(
            to value: Int,
            forOption option: TransactionOption
        ) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func addConflictRange(
            beginKey: ByteString,
            endKey: ByteString,
            type: ConflictRangeType
        ) throws {
            try underlying.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        func getEstimatedRangeSizeBytes(
            beginKey: ByteString,
            endKey: ByteString
        ) async throws -> Int {
            try await underlying.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        func getRangeSplitPoints(
            beginKey: ByteString,
            endKey: ByteString,
            chunkSize: Int
        ) async throws -> [ByteString] {
            try await underlying.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }

        func requestVersionstamp() -> any PendingTransactionVersionstamp {
            underlying.requestVersionstamp()
        }
    }

    private let underlying = InMemoryEngine()
    private let outcomeState = OutcomeState()

    init() {}

    init(configuration: Configuration) async throws {}

    func reportNextCommitAsUnknown() {
        outcomeState.arm()
    }

    func createTransaction() throws -> CommitOutcomeUnknownTransaction {
        CommitOutcomeUnknownTransaction(
            underlying: try underlying.createTransaction(),
            outcomeState: outcomeState
        )
    }

    var transactionDomain: StorageTransactionDomain {
        underlying.transactionDomain
    }

    var directoryAccess: any DirectoryAccess {
        underlying.directoryAccess
    }

    func requestShutdown() {
        underlying.requestShutdown()
    }

    func waitUntilShutdown() async {
        await underlying.waitUntilShutdown()
    }
}
