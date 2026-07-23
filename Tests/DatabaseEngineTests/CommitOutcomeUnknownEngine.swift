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
            for key: Bytes,
            snapshot: Bool
        ) async throws -> Bytes? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> Bytes? {
            try await underlying.getKey(
                selector: selector,
                snapshot: snapshot
            )
        }

        func getRange(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueRangeResult {
            underlying.getRange(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }

        func setValue(_ value: Bytes, for key: Bytes) throws {
            try underlying.setValue(value, for: key)
        }

        func clear(key: Bytes) throws {
            try underlying.clear(key: key)
        }

        func clearRange(beginKey: Bytes, endKey: Bytes) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        func atomicOp(
            key: Bytes,
            param: Bytes,
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
            to value: Bytes?,
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
            beginKey: Bytes,
            endKey: Bytes,
            type: ConflictRangeType
        ) throws {
            try underlying.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        func getEstimatedRangeSizeBytes(
            beginKey: Bytes,
            endKey: Bytes
        ) async throws -> Int {
            try await underlying.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        func getRangeSplitPoints(
            beginKey: Bytes,
            endKey: Bytes,
            chunkSize: Int
        ) async throws -> [Bytes] {
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

    var directoryService: any DirectoryService {
        underlying.directoryService
    }

    func shutdown() {
        underlying.shutdown()
    }
}
