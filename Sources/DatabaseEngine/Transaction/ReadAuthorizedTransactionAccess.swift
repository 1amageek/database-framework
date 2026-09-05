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
    private final class SnapshotIdentity: Sendable {}

    private let transactionDomainValue: StorageTransactionDomain
    private let capabilitiesValue: TransactionCapabilities
    /// Stable identity issued when DatabaseEngine first admits a storage
    /// transaction for reads. Every attenuation wrapper inherits the token;
    /// the StorageKit transaction itself need not have reference identity.
    private let snapshotIdentity: SnapshotIdentity
    private let directTransaction: (any TransactionAccess)?
    private let resolveTransaction:
        @Sendable () throws -> any TransactionAccess
    private let validateScope: @Sendable () throws -> Void
    private let beginScopeOperation:
        (@Sendable () throws -> DatabaseReadScopeOperationLease)?
    private let registerScopeCursor:
        (@Sendable (
            @Sendable (DatabaseReadScopeOperationLease) -> KeyValueCursor,
            DatabaseReadScopeOperationLease
        )
            -> KeyValueCursor)?

    private init(transaction: any TransactionAccess) {
        self.transactionDomainValue = transaction.transactionDomain
        self.capabilitiesValue = transaction.capabilities
        self.snapshotIdentity = SnapshotIdentity()
        self.directTransaction = transaction
        self.resolveTransaction = { transaction }
        self.validateScope = {}
        self.beginScopeOperation = nil
        self.registerScopeCursor = nil
    }

    private init(
        admitted transaction: ReadAuthorizedTransactionAccess,
        resolveTransaction: @escaping @Sendable () throws
            -> any TransactionAccess,
        validateScope: @escaping @Sendable () throws -> Void,
        beginScopeOperation: @escaping @Sendable () throws
            -> DatabaseReadScopeOperationLease,
        registerScopeCursor: @escaping @Sendable (
            @Sendable (DatabaseReadScopeOperationLease) -> KeyValueCursor,
            DatabaseReadScopeOperationLease
        ) -> KeyValueCursor
    ) {
        self.transactionDomainValue = transaction.transactionDomain
        self.capabilitiesValue = transaction.capabilities
        self.snapshotIdentity = transaction.snapshotIdentity
        self.directTransaction = nil
        self.resolveTransaction = resolveTransaction
        self.validateScope = validateScope
        self.beginScopeOperation = beginScopeOperation
        self.registerScopeCursor = registerScopeCursor
    }

    static func admitted(
        _ transaction: any TransactionAccess
    ) -> any TransactionAccess {
        admittedReadAccess(transaction)
    }

    static func admittedReadAccess(
        _ transaction: any TransactionAccess
    ) -> ReadAuthorizedTransactionAccess {
        if let admitted = transaction as? ReadAuthorizedTransactionAccess {
            return admitted
        }
        return ReadAuthorizedTransactionAccess(transaction: transaction)
    }

    static func scoped(
        admitted transaction: ReadAuthorizedTransactionAccess,
        resolveTransaction: @escaping @Sendable () throws
            -> ReadAuthorizedTransactionAccess,
        validateScope: @escaping @Sendable () throws -> Void,
        beginScopeOperation: @escaping @Sendable () throws
            -> DatabaseReadScopeOperationLease,
        registerScopeCursor: @escaping @Sendable (
            @Sendable (DatabaseReadScopeOperationLease) -> KeyValueCursor,
            DatabaseReadScopeOperationLease
        ) -> KeyValueCursor
    ) -> ReadAuthorizedTransactionAccess {
        ReadAuthorizedTransactionAccess(
            admitted: transaction,
            resolveTransaction: resolveTransaction,
            validateScope: validateScope,
            beginScopeOperation: beginScopeOperation,
            registerScopeCursor: registerScopeCursor
        )
    }

    func matches(_ transaction: any TransactionAccess) -> Bool {
        guard let admitted = transaction as? ReadAuthorizedTransactionAccess
        else { return false }
        return admitted.snapshotIdentity === snapshotIdentity
    }

    func restoreReadPosition(_ position: DatabaseReadPosition) throws {
        try validate(operation: nil)
        try Self.restoreReadPosition(
            position,
            on: resolveTransaction()
        )
    }

    static func restoreReadPosition(
        _ position: DatabaseReadPosition,
        on transaction: any TransactionAccess
    ) throws {
        guard case .version(let version) = position,
              transaction.capabilities.historicalReadVersion else {
            throw DatabaseReadPositionError.positionIsNotRestorable
        }
        guard let signedVersion = Int64(exactly: version) else {
            throw DatabaseReadPositionError.versionExceedsStorageRange(
                version
            )
        }
        if let admitted = transaction as? ReadAuthorizedTransactionAccess {
            try admitted.restoreReadPosition(position)
        } else {
            try transaction.setReadVersion(signedVersion)
        }
    }

    func selectReadPosition(
        restoring requestedPosition: DatabaseReadPosition?
    ) async throws -> DatabaseReadPosition {
        if let requestedPosition {
            try restoreReadPosition(requestedPosition)
        }
        let position = try await captureReadPosition()
        if let requestedPosition, position != requestedPosition {
            throw DatabaseReadPositionError.restoredPositionChanged(
                expected: requestedPosition,
                actual: position
            )
        }
        return position
    }

    func captureReadPosition() async throws -> DatabaseReadPosition {
        guard capabilitiesValue.readVersion else {
            return .opaque(Self.makeOpaqueReadPosition())
        }
        let signedVersion = try await getReadVersion()
        guard let version = UInt64(exactly: signedVersion) else {
            throw DatabaseReadPositionError.invalidStorageVersion(
                signedVersion
            )
        }
        return .version(version)
    }

    private static func makeOpaqueReadPosition() -> ByteString {
        var generator = SystemRandomNumberGenerator()
        return ByteString((0..<32).map { _ in
            UInt8.random(in: .min ... .max, using: &generator)
        })
    }

    private func validate(
        operation: DatabaseReadScopeOperationLease?
    ) throws {
        if let operation {
            try operation.validate()
        } else {
            try validateScope()
        }
    }

    /// Borrows the admitted backend transaction for a Directory read.
    ///
    /// An attenuated scope resolves to the scope that admitted it, so the
    /// resolved value is itself a read-authorized access whenever a child
    /// session narrows a parent snapshot. Resolution therefore recurses and
    /// each level contributes its own scope operation to the borrow.
    /// Recursion terminates because `admittedReadAccess` never re-wraps an
    /// admitted access and `scoped` always resolves to a distinct parent.
    func directoryTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerDirectoryTransactionBorrow {
        let operation = try beginScopeOperation?()
        do {
            try validate(operation: operation)
            let transaction = try resolveTransaction()
            if let admitted = transaction as? ReadAuthorizedTransactionAccess {
                precondition(
                    admitted !== self,
                    "A read scope must resolve to the scope that admitted it"
                )
                return try admitted.directoryTransactionBorrow(for: lifecycle)
                    .retainingReadScope(operation)
            }
            if let admitted = transaction as? ContainerTransactionAccess {
                return try admitted.directoryTransactionBorrow(for: lifecycle)
                    .retainingReadScope(operation)
            }
            if let admitted = transaction as? ContainerTransaction {
                return try admitted.directoryTransactionBorrow(for: lifecycle)
                    .retainingReadScope(operation)
            }
            // The rejected transaction type is not reported: `type(of:)` on
            // an existential requires runtime type metadata, which Embedded
            // Swift does not provide. Naming it here breaks the Embedded
            // WASM runtime.
            throw StorageError.invalidOperation(
                """
                Directory reads require a transaction admitted by the same \
                database container
                """
            )
        } catch {
            operation?.end()
            throw error
        }
    }

    var capabilities: TransactionCapabilities {
        capabilitiesValue
    }

    var transactionDomain: StorageTransactionDomain {
        transactionDomainValue
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
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let value = try await resolveTransaction().getValue(
            for: key,
            snapshot: snapshot
        )
        try validate(operation: operation)
        return value
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let value = try await resolveTransaction().getValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
        try validate(operation: operation)
        return value
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let value = try await resolveTransaction().getValue(for: key)
        try validate(operation: operation)
        return value
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let key = try await resolveTransaction().getKey(
            selector: selector,
            snapshot: snapshot
        )
        try validate(operation: operation)
        return key
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        guard let beginScopeOperation, let registerScopeCursor else {
            guard let directTransaction else {
                preconditionFailure(
                    "Direct read access must retain its transaction"
                )
            }
            return directTransaction.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }
        var operation: DatabaseReadScopeOperationLease?
        let transaction: any TransactionAccess
        do {
            operation = try beginScopeOperation()
            try operation?.validate()
            transaction = try resolveTransaction()
        } catch {
            let admissionError = error
            operation?.end()
            return KeyValueCursor(validatingScope: {
                throw admissionError
            })
        }
        guard let operation else {
            preconditionFailure("Scoped read operation was not acquired")
        }
        return registerScopeCursor(
            { cursorOperation in
                transaction.rangeCursor(
                    from: begin,
                    to: end,
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot,
                    streamingMode: streamingMode
                ).validatingScope {
                    try cursorOperation.validate()
                }
            },
            operation
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
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func getReadVersion() async throws -> Int64 {
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let version = try await resolveTransaction().getReadVersion()
        try validate(operation: operation)
        return version
    }

    func setOption(forOption option: TransactionOption) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func setOption(
        to value: Int,
        forOption option: TransactionOption
    ) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        throw DatabaseReadTransactionError.mutationRequiresWriteAccess
    }

    func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let size = try await resolveTransaction().getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
        try validate(operation: operation)
        return size
    }

    func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        let operation = try beginScopeOperation?()
        defer { operation?.end() }
        try validate(operation: operation)
        let points = try await resolveTransaction().getRangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize
        )
        try validate(operation: operation)
        return points
    }

    func requestVersionstamp() -> any PendingTransactionVersionstamp {
        RejectedReadVersionstamp()
    }
}

private struct RejectedReadVersionstamp: PendingTransactionVersionstamp {
    var value: TransactionVersionstamp {
        get async throws {
            throw DatabaseReadTransactionError.mutationRequiresWriteAccess
        }
    }
}
