// ScenarioStorageEngine.swift
// Makes every owner a serialized scenario created terminal at its boundary.

import DatabaseTypes
import StorageKit
import Synchronization

/// Binds a real storage engine to the lifetime of one serialized scenario.
///
/// `createTransaction()` is the seam a container and a raw transaction holder
/// both reach, because `createOwnedTransaction` defaults through it.
/// Decorating it therefore needs no per-test discipline.
///
/// `executeTransaction` is forwarded rather than decorated. A base engine may
/// own that lifecycle itself, as `FDBStorageEngine` does to convert a native
/// backend error the operation threw, and supplying the protocol default here
/// would silently replace that behavior with a different one. A forwarded call
/// owns its transaction from creation through commit or cancellation, so
/// admitting the call as a whole bounds it more tightly than counting its
/// individual operations would: it cannot outlive the scenario that started it.
///
/// Sealing belongs to `endScenario()`, which only the scenario resource owner
/// calls, after every container and raw transaction holder the scenario body
/// created has been shut down. The `StorageEngine` shutdown methods only
/// forward, because a container storage lifecycle reaches both of them for the
/// single container it owns: `requestShutdown()` from `deinit`, and
/// `waitUntilShutdown()` from `shutdown()`. Sealing in either one would refuse
/// the work of a scenario that shut one container down and went on using the
/// engine, turning a running scenario into a false failure at a boundary that
/// is not the scenario's own.
public final class ScenarioStorageEngine<Base: StorageEngine>:
    StorageEngine,
    Sendable {
    public struct Configuration: Sendable {
        let base: Base.Configuration
        let recorder: ScenarioAdmissionRecorder?

        public init(
            base: Base.Configuration,
            recorder: ScenarioAdmissionRecorder? = nil
        ) {
            self.base = base
            self.recorder = recorder
        }
    }

    public typealias TransactionType = ScenarioTransaction

    private let base: Base
    private let ledger: ScenarioAdmissionLedger
    private let recorder: ScenarioAdmissionRecorder?
    private let hasReported = Mutex(false)
    private let directoryAccessAdapter: DirectoryAccessAdapter

    public init(base: Base, recorder: ScenarioAdmissionRecorder? = nil) {
        let ledger = ScenarioAdmissionLedger(
            backend: base.directoryAccess.backend
        )
        self.base = base
        self.ledger = ledger
        self.recorder = recorder
        self.directoryAccessAdapter = DirectoryAccessAdapter(
            base: base.directoryAccess,
            ledger: ledger
        )
    }

    public init(configuration: Configuration) async throws {
        let base = try await Base(configuration: configuration.base)
        let ledger = ScenarioAdmissionLedger(
            backend: base.directoryAccess.backend
        )
        self.base = base
        self.ledger = ledger
        self.recorder = configuration.recorder
        self.directoryAccessAdapter = DirectoryAccessAdapter(
            base: base.directoryAccess,
            ledger: ledger
        )
    }

    public var admissionLedger: ScenarioAdmissionLedger { ledger }

    /// Creating a transaction is refused after the scenario seals, but it is
    /// not counted as outstanding work. Creation is local to the base engine
    /// and returns before the caller issues anything, so counting it would
    /// have reported quiescence for a transaction that had not yet begun to
    /// read or write. What the returned transaction goes on to do is admitted
    /// operation by operation instead.
    public func createTransaction() throws -> ScenarioTransaction {
        try ledger.requireOpen(.beginTransaction)
        return ScenarioTransaction(
            base: try base.createTransaction(),
            ledger: ledger
        )
    }

    public var transactionDomain: StorageTransactionDomain {
        base.transactionDomain
    }

    /// Admits one owned-transaction lifecycle and lets the base engine run it.
    /// `withTransaction` arrives here through `StorageTransactionExecutor`, so
    /// both entry points share that admission.
    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        try await ledger.withAdmission(.execute) {
            try await base.executeTransaction(operation)
        }
    }

    /// Directory work runs on the transaction the base engine created, so the
    /// adapter translates back at that boundary and counts the operation
    /// itself. A backend catalog such as FoundationDB drives its native
    /// Directory Layer and accepts only its own transaction type.
    public var directoryAccess: any DirectoryAccess {
        directoryAccessAdapter
    }

    public func requestShutdown() {
        base.requestShutdown()
    }

    public func waitUntilShutdown() async {
        await base.waitUntilShutdown()
    }

    /// Ends the scenario that owns this engine: closes admission, waits for the
    /// operations it admitted to end, reports what it observed, and only then
    /// completes the base engine shutdown.
    ///
    /// The scenario resource owner is the sole caller, so the seal lands at the
    /// scenario boundary rather than wherever a container happens to shut down.
    /// Reporting happens once, because the owner and a later container shutdown
    /// can both reach the base engine.
    public func endScenario() async {
        ledger.close()
        let report = await ledger.waitUntilQuiescent()
        let shouldReport = hasReported.withLock { hasReported in
            guard !hasReported else { return false }
            hasReported = true
            return true
        }
        if shouldReport {
            recorder?.record(report)
        }
        await base.waitUntilShutdown()
    }

    public final class ScenarioTransaction: Transaction, Sendable {
        public typealias RangeResult = KeyValueRangeResult

        fileprivate let base: Base.TransactionType
        fileprivate let ledger: ScenarioAdmissionLedger

        init(base: Base.TransactionType, ledger: ScenarioAdmissionLedger) {
            self.base = base
            self.ledger = ledger
        }

        // MARK: - Local transaction state

        public var capabilities: TransactionCapabilities {
            base.capabilities
        }

        public var compaction: StorageCompactionAccess? {
            base.compaction
        }

        public var storageFailure: StorageError? {
            base.storageFailure
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

        public func setReadVersion(_ version: Int64) throws {
            try base.setReadVersion(version)
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

        /// Requesting the versionstamp is local bookkeeping on the base
        /// transaction, so the request itself is forwarded. Resolving the
        /// value waits for the commit the backend is still running, which is
        /// backend work, so the returned pending value carries the ledger and
        /// admits that wait.
        public func requestVersionstamp() -> any PendingTransactionVersionstamp {
            ScenarioPendingVersionstamp(
                base: base.requestVersionstamp(),
                ledger: ledger
            )
        }

        public func getCommittedVersion() throws -> Int64 {
            try base.getCommittedVersion()
        }

        // MARK: - Admitted read

        public func getValue(
            for key: ByteString,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await ledger.withAdmission(.read) {
                try await base.getValue(for: key, snapshot: snapshot)
            }
        }

        public func getValue(
            for key: ByteString,
            snapshot: Bool,
            maximumByteCount: Int
        ) async throws -> ByteString? {
            try await ledger.withAdmission(.read) {
                try await base.getValue(
                    for: key,
                    snapshot: snapshot,
                    maximumByteCount: maximumByteCount
                )
            }
        }

        public func getValue(for key: ByteString) async throws -> ByteString? {
            try await ledger.withAdmission(.read) {
                try await base.getValue(for: key)
            }
        }

        public func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await ledger.withAdmission(.read) {
                try await base.getKey(selector: selector, snapshot: snapshot)
            }
        }

        public func getReadVersion() async throws -> Int64 {
            try await ledger.withAdmission(.read) {
                try await base.getReadVersion()
            }
        }

        public func getEstimatedRangeSizeBytes(
            beginKey: ByteString,
            endKey: ByteString
        ) async throws -> Int {
            try await ledger.withAdmission(.rangeRead) {
                try await base.getEstimatedRangeSizeBytes(
                    beginKey: beginKey,
                    endKey: endKey
                )
            }
        }

        public func getRangeSplitPoints(
            beginKey: ByteString,
            endKey: ByteString,
            chunkSize: Int
        ) async throws -> [ByteString] {
            try await ledger.withAdmission(.rangeRead) {
                try await base.getRangeSplitPoints(
                    beginKey: beginKey,
                    endKey: endKey,
                    chunkSize: chunkSize
                )
            }
        }

        /// The backend range result is lazy, so the factory only has to refuse
        /// to build one. Each advance and the terminal cleanup are admitted
        /// where they run.
        public func rangeCursor(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueCursor {
            do {
                try ledger.requireOpen(.rangeRead)
            } catch {
                let ledger = ledger
                return KeyValueCursor(validatingScope: {
                    try ledger.requireOpen(.rangeRead)
                })
            }
            let cursor = base.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
            return KeyValueCursor(
                consuming: ScenarioRangeResult(base: cursor, ledger: ledger)
            )
        }

        // MARK: - Admitted mutation

        public func setValue(
            _ value: ByteString,
            for key: ByteString
        ) throws {
            try ledger.begin(.write)
            defer { ledger.end(.write) }
            try base.setValue(value, for: key)
        }

        public func clear(key: ByteString) throws {
            try ledger.begin(.delete)
            defer { ledger.end(.delete) }
            try base.clear(key: key)
        }

        public func clearRange(
            beginKey: ByteString,
            endKey: ByteString
        ) throws {
            try ledger.begin(.deleteRange)
            defer { ledger.end(.deleteRange) }
            try base.clearRange(beginKey: beginKey, endKey: endKey)
        }

        public func atomicOp(
            key: ByteString,
            param: ByteString,
            mutationType: MutationType
        ) throws {
            try ledger.begin(.write)
            defer { ledger.end(.write) }
            try base.atomicOp(
                key: key,
                param: param,
                mutationType: mutationType
            )
        }

        public func commit() async throws {
            try await ledger.withAdmission(.commit) {
                try await base.commit()
            }
        }

        /// Cancellation is forwarded past a closed admission and is not
        /// counted. It is the work that makes a leaked transaction terminal,
        /// and it completes inside the transaction without reaching the
        /// service.
        public func cancel() async throws {
            try await base.cancel()
        }
    }

    /// Forwards Directory operations to the base catalog on the base engine's
    /// own transaction, and counts the operation here because the unwrapped
    /// transaction is invisible to the ledger.
    private final class DirectoryAccessAdapter: DirectoryAccess {
        private let base: any DirectoryAccess
        private let ledger: ScenarioAdmissionLedger

        init(base: any DirectoryAccess, ledger: ScenarioAdmissionLedger) {
            self.base = base
            self.ledger = ledger
        }

        var transactionDomain: StorageTransactionDomain {
            base.transactionDomain
        }

        var backend: StorageBackend {
            base.backend
        }

        func admit(_ operation: StorageOperation) throws {
            try ledger.requireOpen(operation)
            try base.admit(operation)
        }

        func openRoot(
            transaction: any TransactionReadAccess
        ) async throws -> Directory? {
            try await ledger.withAdmission(.read) {
                try await base.openRoot(transaction: read(transaction))
            }
        }

        func openOrInitializeRoot(
            transaction: any TransactionAccess
        ) async throws -> Directory {
            try await ledger.withAdmission(.write) {
                try await base.openOrInitializeRoot(
                    transaction: write(transaction)
                )
            }
        }

        func open(
            _ name: String,
            expecting expected: LayerTag?,
            in parent: Directory,
            transaction: any TransactionReadAccess
        ) async throws -> Directory? {
            try await ledger.withAdmission(.read) {
                try await base.open(
                    name,
                    expecting: expected,
                    in: parent,
                    transaction: read(transaction)
                )
            }
        }

        func openOrCreate(
            _ name: String,
            layer: LayerTag,
            in parent: Directory,
            transaction: any TransactionAccess
        ) async throws -> Directory {
            try await ledger.withAdmission(.write) {
                try await base.openOrCreate(
                    name,
                    layer: layer,
                    in: parent,
                    transaction: write(transaction)
                )
            }
        }

        func listChildren(
            in parent: Directory,
            after: String?,
            limit: Int,
            transaction: any TransactionReadAccess
        ) async throws -> [DirectoryEntry] {
            try await ledger.withAdmission(.rangeRead) {
                try await base.listChildren(
                    in: parent,
                    after: after,
                    limit: limit,
                    transaction: read(transaction)
                )
            }
        }

        func move(
            _ name: String,
            in source: Directory,
            to newName: String,
            in destination: Directory,
            transaction: any TransactionAccess
        ) async throws -> Directory {
            try await ledger.withAdmission(.write) {
                try await base.move(
                    name,
                    in: source,
                    to: newName,
                    in: destination,
                    transaction: write(transaction)
                )
            }
        }

        func remove(
            _ name: String,
            in parent: Directory,
            transaction: any TransactionAccess
        ) async throws {
            try await ledger.withAdmission(.write) {
                try await base.remove(
                    name,
                    in: parent,
                    transaction: write(transaction)
                )
            }
        }

        /// A transaction this engine did not create is forwarded unchanged, so
        /// the base catalog reports the domain mismatch it owns.
        private func read(
            _ transaction: any TransactionReadAccess
        ) -> any TransactionReadAccess {
            guard let scenario = transaction as? ScenarioTransaction else {
                return transaction
            }
            return scenario.base
        }

        private func write(
            _ transaction: any TransactionAccess
        ) -> any TransactionAccess {
            guard let scenario = transaction as? ScenarioTransaction else {
                return transaction
            }
            return scenario.base
        }
    }

    private struct ScenarioRangeResult: TransactionRangeResult {
        typealias Element = (ByteString, ByteString)

        let base: KeyValueCursor
        let ledger: ScenarioAdmissionLedger

        func makeCursor() -> Cursor {
            Cursor(base: base, ledger: ledger)
        }

        struct Cursor: TransactionRangeCursor {
            var base: KeyValueCursor
            let ledger: ScenarioAdmissionLedger
            var isFinished = false

            mutating func next() async throws -> Element? {
                try ledger.begin(.rangeRead)
                defer { ledger.end(.rangeRead) }
                return try await base.next()
            }

            /// Terminal cursor cleanup is forwarded past a closed admission
            /// for the same reason as cancellation: it releases the backend
            /// iterator instead of reaching the service, and refusing it would
            /// leave a leaked cursor open. Any advance it waits on is already
            /// counted.
            mutating func finish(
                isolation actor: isolated (any Actor)?
            ) async throws {
                guard !isFinished else { return }
                isFinished = true
                try await base.finish()
            }
        }
    }
}

/// Admits the wait a pending versionstamp performs.
///
/// A versionstamp request is answered only once the commit that produces it
/// has been applied, so awaiting the value is service work that can still be
/// running when the scenario tries to seal. Forwarding the base pending value
/// unwrapped would leave that one wait outside the gate.
struct ScenarioPendingVersionstamp: PendingTransactionVersionstamp {
    let base: any PendingTransactionVersionstamp
    let ledger: ScenarioAdmissionLedger

    var value: TransactionVersionstamp {
        get async throws {
            try await ledger.withAdmission(.commit) {
                try await base.value
            }
        }
    }
}
