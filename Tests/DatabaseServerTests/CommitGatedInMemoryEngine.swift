import DatabaseTypes
import StorageKit
import Synchronization

final class CommitGatedInMemoryEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        init() {}
    }

    typealias TransactionType = CommitGatedTransaction

    actor CommitGate {
        private var didStart = false
        private var didRelease = false
        private var startWaiters: [CheckedContinuation<Void, Never>] = []
        private var releaseWaiter: CheckedContinuation<Void, Never>?

        func waitUntilStarted() async {
            guard !didStart else { return }
            await withCheckedContinuation { continuation in
                startWaiters.append(continuation)
            }
        }

        fileprivate func enterAndWait() async {
            didStart = true
            let waiters = startWaiters
            startWaiters.removeAll(keepingCapacity: false)
            for waiter in waiters {
                waiter.resume()
            }
            guard !didRelease else { return }
            await withCheckedContinuation { continuation in
                if didRelease {
                    continuation.resume()
                } else {
                    releaseWaiter = continuation
                }
            }
        }

        func release() {
            didRelease = true
            releaseWaiter?.resume()
            releaseWaiter = nil
        }
    }

    fileprivate final class CommitGateState: Sendable {
        private let gate = Mutex<CommitGate?>(nil)

        func arm() -> CommitGate {
            let newGate = CommitGate()
            gate.withLock { $0 = newGate }
            return newGate
        }

        func take() -> CommitGate? {
            gate.withLock { gate in
                let current = gate
                gate = nil
                return current
            }
        }
    }

    final class CommitGatedTransaction: Transaction, Sendable {
        typealias RangeResult = KeyValueRangeResult

        private let underlying: InMemoryTransaction
        private let commitGateState: CommitGateState
        private let containsMutation = Mutex(false)

        var capabilities: TransactionCapabilities { underlying.capabilities }
        var mutationByteLimit: Int? { underlying.mutationByteLimit }
        var transactionDomain: StorageTransactionDomain {
            underlying.transactionDomain
        }

        fileprivate init(
            underlying: InMemoryTransaction,
            commitGateState: CommitGateState
        ) {
            self.underlying = underlying
            self.commitGateState = commitGateState
        }

        func configureMutationByteLimit(maximumBytes: Int?) throws {
            try underlying.configureMutationByteLimit(maximumBytes: maximumBytes)
        }

        func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await underlying.getKey(selector: selector, snapshot: snapshot)
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

        func setValue(_ value: ByteString, for key: ByteString) throws {
            try underlying.setValue(value, for: key)
            containsMutation.withLock { $0 = true }
        }

        func clear(key: ByteString) throws {
            try underlying.clear(key: key)
            containsMutation.withLock { $0 = true }
        }

        func clearRange(beginKey: ByteString, endKey: ByteString) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
            containsMutation.withLock { $0 = true }
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
            containsMutation.withLock { $0 = true }
        }

        func commit() async throws {
            if containsMutation.withLock({ $0 }),
               let gate = commitGateState.take() {
                // A dispatched commit has an authoritative backend outcome.
                // The gate models commit work that cannot be aborted.
                await gate.enterAndWait()
            }
            try await underlying.commit()
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
    private let commitGateState = CommitGateState()

    init() {}

    init(configuration: Configuration) async throws {
        _ = configuration
    }

    func suspendNextMutatingCommit() -> CommitGate {
        commitGateState.arm()
    }

    func createTransaction() throws -> CommitGatedTransaction {
        CommitGatedTransaction(
            underlying: try underlying.createTransaction(),
            commitGateState: commitGateState
        )
    }

    var namespaceResolver: any NamespaceResolver {
        underlying.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        underlying.namespaceCatalog
    }

    func shutdown() {
        underlying.shutdown()
    }
}
