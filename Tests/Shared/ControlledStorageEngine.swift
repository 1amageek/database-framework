import DatabaseTypes
import StorageKit
import Synchronization

/// Decorates a real storage engine with deterministic test-only boundaries.
public final class ControlledStorageEngine<Base: StorageEngine>:
    StorageEngine,
    Sendable {
    public struct Configuration: Sendable {
        let base: Base.Configuration

        public init(base: Base.Configuration) {
            self.base = base
        }
    }

    public typealias TransactionType = ControlledTransaction

    private let base: Base
    public let control: StorageTransactionControl
    private let directoryAccessAdapter: DirectoryAccessAdapter

    public init(
        base: Base,
        control: StorageTransactionControl = StorageTransactionControl()
    ) {
        self.base = base
        self.control = control
        self.directoryAccessAdapter = DirectoryAccessAdapter(
            base: base.directoryAccess
        )
    }

    public init(configuration: Configuration) async throws {
        let base = try await Base(configuration: configuration.base)
        self.base = base
        self.control = StorageTransactionControl()
        self.directoryAccessAdapter = DirectoryAccessAdapter(
            base: base.directoryAccess
        )
    }

    public func createTransaction() throws -> ControlledTransaction {
        ControlledTransaction(
            base: try base.createTransaction(),
            control: control
        )
    }

    public var transactionDomain: StorageTransactionDomain {
        base.transactionDomain
    }

    /// Directory work runs on the transaction the base engine created.
    ///
    /// A decorator that wraps transactions owns the translation back at every
    /// boundary that consumes one: a backend catalog such as FoundationDB
    /// drives its native Directory Layer and accepts only its own transaction
    /// type. Mutating Directory operations still mark the controlled
    /// transaction, so a scenario that intercepts a commit observes them.
    public var directoryAccess: any DirectoryAccess {
        directoryAccessAdapter
    }

    public func requestShutdown() {
        base.requestShutdown()
    }

    public func waitUntilShutdown() async {
        await base.waitUntilShutdown()
    }

    public final class ControlledTransaction: Transaction, Sendable {
        public typealias RangeResult = KeyValueRangeResult

        fileprivate let base: Base.TransactionType
        private let control: StorageTransactionControl
        private struct MutationState: Sendable {
            var hasMutations = false
            var keys: [ByteString] = []
        }

        private let mutationState = Mutex(MutationState())
        private let injectedFailure = Mutex<StorageError?>(nil)

        init(
            base: Base.TransactionType,
            control: StorageTransactionControl
        ) {
            self.base = base
            self.control = control
        }

        public var capabilities: TransactionCapabilities {
            base.capabilities
        }

        public var compaction: StorageCompactionAccess? {
            base.compaction
        }

        public var storageFailure: StorageError? {
            injectedFailure.withLock { $0 } ?? base.storageFailure
        }

        public var mutationByteLimit: Int? {
            base.mutationByteLimit
        }

        public var transactionDomain: StorageTransactionDomain {
            base.transactionDomain
        }

        public func configureMutationByteLimit(
            maximumBytes: Int?
        ) throws {
            try base.configureMutationByteLimit(maximumBytes: maximumBytes)
        }

        public func getValue(
            for key: ByteString,
            snapshot: Bool
        ) async throws -> ByteString? {
            control.recordValueRead()
            let value = try await base.getValue(
                for: key,
                snapshot: snapshot
            )
            await control.suspendValueReadIfRequested(for: key)
            return value
        }

        public func getValue(
            for key: ByteString,
            snapshot: Bool,
            maximumByteCount: Int
        ) async throws -> ByteString? {
            control.recordBoundedValueRead(
                maximumByteCount: maximumByteCount,
                snapshot: snapshot
            )
            await control.suspendBoundedValueReadIfRequested(for: key)
            let value = try await base.getValue(
                for: key,
                snapshot: snapshot,
                maximumByteCount: maximumByteCount
            )
            await control.suspendValueReadIfRequested(for: key)
            return value
        }

        public func getValue(for key: ByteString) async throws -> ByteString? {
            control.recordValueRead()
            let value = try await base.getValue(for: key)
            await control.suspendValueReadIfRequested(for: key)
            return value
        }

        public func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> ByteString? {
            control.recordKeyRead()
            return try await base.getKey(
                selector: selector,
                snapshot: snapshot
            )
        }

        public func rangeCursor(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueCursor {
            control.recordRangeCursorOpened(limit: limit)
            let cursor = base.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
            return KeyValueCursor(
                consuming: ControlledRangeResult(
                    base: cursor,
                    control: control
                )
            )
        }

        public func setValue(
            _ value: ByteString,
            for key: ByteString
        ) throws {
            try base.setValue(value, for: key)
            recordMutation(key: key)
        }

        public func clear(key: ByteString) throws {
            try base.clear(key: key)
            recordMutation(key: key)
        }

        public func clearRange(
            beginKey: ByteString,
            endKey: ByteString
        ) throws {
            try base.clearRange(beginKey: beginKey, endKey: endKey)
            mutationState.withLock {
                $0.hasMutations = true
                $0.keys.append(beginKey)
                $0.keys.append(endKey)
            }
        }

        public func atomicOp(
            key: ByteString,
            param: ByteString,
            mutationType: MutationType
        ) throws {
            try base.atomicOp(
                key: key,
                param: param,
                mutationType: mutationType
            )
            recordMutation(key: key)
        }

        public func setReadVersion(_ version: Int64) throws {
            try base.setReadVersion(version)
        }

        public func getReadVersion() async throws -> Int64 {
            control.recordReadVersion()
            return try await base.getReadVersion()
        }

        public func setOption(forOption option: TransactionOption) throws {
            try base.setOption(forOption: option)
        }

        public func setOption(
            to value: ByteString?,
            forOption option: TransactionOption
        ) throws {
            try base.setOption(to: value, forOption: option)
        }

        public func setOption(
            to value: Int,
            forOption option: TransactionOption
        ) throws {
            try base.setOption(to: value, forOption: option)
        }

        public func addConflictRange(
            beginKey: ByteString,
            endKey: ByteString,
            type: ConflictRangeType
        ) throws {
            try base.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        public func getEstimatedRangeSizeBytes(
            beginKey: ByteString,
            endKey: ByteString
        ) async throws -> Int {
            control.recordRangeMetadataRead()
            return try await base.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        public func getRangeSplitPoints(
            beginKey: ByteString,
            endKey: ByteString,
            chunkSize: Int
        ) async throws -> [ByteString] {
            control.recordRangeMetadataRead()
            return try await base.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }

        public func requestVersionstamp() -> any PendingTransactionVersionstamp {
            base.requestVersionstamp()
        }

        public func commit() async throws {
            let acceptedMutations = mutationState.withLock { $0 }
            switch control.commitDirective(
                hasMutations: acceptedMutations.hasMutations,
                mutationKeys: acceptedMutations.keys
            ) {
            case .proceed:
                break
            case .fail(let error):
                injectedFailure.withLock { $0 = error }
                throw error
            case .suspend(let barrier):
                await barrier.enterAndWait()
            }
            try await base.commit()
        }

        public func cancel() async throws {
            try await base.cancel()
        }

        public func getCommittedVersion() throws -> Int64 {
            try base.getCommittedVersion()
        }

        fileprivate func recordDirectoryMutation() {
            mutationState.withLock { $0.hasMutations = true }
        }

        private func recordMutation(key: ByteString) {
            mutationState.withLock {
                $0.hasMutations = true
                $0.keys.append(key)
            }
        }
    }

    /// Forwards Directory operations to the base catalog on the base engine's
    /// own transaction.
    private final class DirectoryAccessAdapter: DirectoryAccess {
        private let base: any DirectoryAccess

        init(base: any DirectoryAccess) {
            self.base = base
        }

        var transactionDomain: StorageTransactionDomain {
            base.transactionDomain
        }

        var backend: StorageBackend {
            base.backend
        }

        func openRoot(
            transaction: any TransactionReadAccess
        ) async throws -> Directory? {
            try await base.openRoot(transaction: read(transaction))
        }

        func openOrInitializeRoot(
            transaction: any TransactionAccess
        ) async throws -> Directory {
            let root = try await base.openOrInitializeRoot(
                transaction: write(transaction)
            )
            markMutation(transaction)
            return root
        }

        func open(
            _ name: String,
            expecting expected: LayerTag?,
            in parent: Directory,
            transaction: any TransactionReadAccess
        ) async throws -> Directory? {
            try await base.open(
                name,
                expecting: expected,
                in: parent,
                transaction: read(transaction)
            )
        }

        func openOrCreate(
            _ name: String,
            layer: LayerTag,
            in parent: Directory,
            transaction: any TransactionAccess
        ) async throws -> Directory {
            let directory = try await base.openOrCreate(
                name,
                layer: layer,
                in: parent,
                transaction: write(transaction)
            )
            markMutation(transaction)
            return directory
        }

        func listChildren(
            in parent: Directory,
            after: String?,
            limit: Int,
            transaction: any TransactionReadAccess
        ) async throws -> [DirectoryEntry] {
            try await base.listChildren(
                in: parent,
                after: after,
                limit: limit,
                transaction: read(transaction)
            )
        }

        func move(
            _ name: String,
            in source: Directory,
            to newName: String,
            in destination: Directory,
            transaction: any TransactionAccess
        ) async throws -> Directory {
            let directory = try await base.move(
                name,
                in: source,
                to: newName,
                in: destination,
                transaction: write(transaction)
            )
            markMutation(transaction)
            return directory
        }

        func remove(
            _ name: String,
            in parent: Directory,
            transaction: any TransactionAccess
        ) async throws {
            try await base.remove(
                name,
                in: parent,
                transaction: write(transaction)
            )
            markMutation(transaction)
        }

        /// A transaction this engine did not create is forwarded unchanged, so
        /// the base catalog reports the domain mismatch it owns.
        private func read(
            _ transaction: any TransactionReadAccess
        ) -> any TransactionReadAccess {
            guard let controlled = transaction as? ControlledTransaction else {
                return transaction
            }
            return controlled.base
        }

        private func write(
            _ transaction: any TransactionAccess
        ) -> any TransactionAccess {
            guard let controlled = transaction as? ControlledTransaction else {
                return transaction
            }
            return controlled.base
        }

        private func markMutation(_ transaction: any TransactionAccess) {
            (transaction as? ControlledTransaction)?.recordDirectoryMutation()
        }
    }

    private struct ControlledRangeResult: TransactionRangeResult {
        typealias Element = (ByteString, ByteString)

        let base: KeyValueCursor
        let control: StorageTransactionControl

        func makeCursor() -> Cursor {
            Cursor(base: base, control: control)
        }

        struct Cursor: TransactionRangeCursor {
            var base: KeyValueCursor
            let control: StorageTransactionControl
            var isAdmitted = false
            var returnedElementCount = 0
            var isFinished = false

            mutating func next() async throws -> Element? {
                if !isAdmitted {
                    isAdmitted = true
                    await control.suspendRangeAdvanceIfRequested()
                }
                if returnedElementCount > 0 {
                    await control.suspendRangeContinuationIfRequested()
                }
                let element = try await base.next()
                if element != nil {
                    returnedElementCount += 1
                }
                return element
            }

            mutating func finish(
                isolation actor: isolated (any Actor)?
            ) async throws {
                guard !isFinished else { return }
                isFinished = true
                defer { control.recordRangeCursorFinished() }
                try await base.finish()
            }
        }
    }
}
