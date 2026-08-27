import DatabaseTypes
import StorageKit
import Synchronization

/// Decorates a real storage engine with deterministic test-only boundaries.
public final class ControlledStorageEngine<Base: StorageEngine>:
    StorageEngine,
    NamespaceResolver,
    NamespaceCatalog,
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

    public init(
        base: Base,
        control: StorageTransactionControl = StorageTransactionControl()
    ) {
        self.base = base
        self.control = control
    }

    public init(configuration: Configuration) async throws {
        self.base = try await Base(configuration: configuration.base)
        self.control = StorageTransactionControl()
    }

    public func createTransaction() throws -> ControlledTransaction {
        ControlledTransaction(
            base: try base.createTransaction(),
            control: control
        )
    }

    public var namespaceResolver: any NamespaceResolver {
        self
    }

    public var namespaceCatalog: (any NamespaceCatalog)? {
        base.namespaceCatalog == nil ? nil : self
    }

    public func resolveOrCreate(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        let controlled = try controlledTransaction(from: transaction)
        control.recordNamespaceRead()
        let subspace = try await base.namespaceResolver.resolveOrCreate(
            path: path,
            transaction: controlled.base
        )
        controlled.recordNamespaceMutation()
        return subspace
    }

    public func resolveExisting(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        control.recordNamespaceRead()
        return try await base.namespaceResolver.resolveExisting(
            path: path,
            transaction: try controlledTransaction(from: transaction).base
        )
    }

    public func namespaceExists(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Bool {
        control.recordNamespaceRead()
        return try await base.namespaceResolver.namespaceExists(
            path: path,
            transaction: try controlledTransaction(from: transaction).base
        )
    }

    public func listNamespaces(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> [String] {
        guard let namespaceCatalog = base.namespaceCatalog else {
            throw StorageError.invalidOperation(
                "The controlled storage backend has no namespace catalog"
            )
        }
        control.recordNamespaceRead()
        return try await namespaceCatalog.listNamespaces(
            path: path,
            transaction: try controlledTransaction(from: transaction).base
        )
    }

    public func removeNamespace(
        path: [String],
        transaction: any TransactionAccess
    ) async throws {
        guard let namespaceCatalog = base.namespaceCatalog else {
            throw StorageError.invalidOperation(
                "The controlled storage backend has no namespace catalog"
            )
        }
        let controlled = try controlledTransaction(from: transaction)
        try await namespaceCatalog.removeNamespace(
            path: path,
            transaction: controlled.base
        )
        controlled.recordNamespaceMutation()
    }

    public func requestShutdown() {
        base.requestShutdown()
    }

    public func waitUntilShutdown() async {
        await base.waitUntilShutdown()
    }

    private func controlledTransaction(
        from transaction: any TransactionAccess
    ) throws -> ControlledTransaction {
        guard let controlled = transaction as? ControlledTransaction else {
            throw StorageError.invalidOperation(
                "Namespace operations require this controlled engine's transaction"
            )
        }
        return controlled
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

        fileprivate func recordNamespaceMutation() {
            mutationState.withLock { $0.hasMutations = true }
        }

        private func recordMutation(key: ByteString) {
            mutationState.withLock {
                $0.hasMutations = true
                $0.keys.append(key)
            }
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
            var isFinished = false

            mutating func next() async throws -> Element? {
                if !isAdmitted {
                    isAdmitted = true
                    await control.suspendRangeAdvanceIfRequested()
                }
                return try await base.next()
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
