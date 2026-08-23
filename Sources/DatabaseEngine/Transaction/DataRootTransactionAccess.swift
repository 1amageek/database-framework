import DatabaseTypes
import StorageKit
import Synchronization

private final class RevocableDataRootTransactionOwner: Sendable {
    private let transaction: Mutex<(any TransactionAccess)?>

    init(transaction: any TransactionAccess) {
        self.transaction = Mutex(transaction)
    }

    func borrow() throws -> any TransactionAccess {
        try transaction.withLock { transaction in
            guard let transaction else {
                throw DatabaseReadTransactionError.snapshotClosed
            }
            return transaction
        }
    }

    func revoke() {
        transaction.withLock { $0 = nil }
    }
}

/// Read or mutation access confined to one selected database data root while
/// retaining the underlying snapshot and container-operation lifetime.
///
/// `TransactionAccess` combines reads and mutations in StorageKit. Database
/// authorization is narrower: a caller admitted with only Base read access
/// must not inherit the mutation methods of that storage protocol. This
/// adapter preserves physical reads while rejecting persistent mutation and
/// transaction-control capabilities.
final class DataRootTransactionAccess:
    TransactionAccess,
    Sendable
{
    private let transactionOwner: RevocableDataRootTransactionOwner
    private let physicalCapabilitiesSnapshot: TransactionCapabilities
    private let dataRoot: Subspace
    private let accessMode: DatabaseTransactionAccessMode
    private let readScope: DatabaseReadScopeGate?

    private init(
        transaction: any TransactionAccess,
        dataRoot: Subspace,
        accessMode: DatabaseTransactionAccessMode,
        readScope: DatabaseReadScopeGate?
    ) {
        self.transactionOwner = RevocableDataRootTransactionOwner(
            transaction: transaction
        )
        self.physicalCapabilitiesSnapshot =
            (transaction as? DataRootTransactionAccess)?
                .physicalCapabilitiesSnapshot
            ?? transaction.capabilities
        self.dataRoot = dataRoot
        self.accessMode = accessMode
        self.readScope = readScope
    }

    static func admitted(
        _ transaction: any TransactionAccess,
        dataRoot: Subspace,
        accessMode: DatabaseTransactionAccessMode = .readOnly,
        readScope: DatabaseReadScopeGate? = nil
    ) -> DataRootTransactionAccess {
        if let admitted = transaction as? DataRootTransactionAccess,
           admitted.dataRoot == dataRoot,
           admitted.accessMode == accessMode,
           admitted.readScope === readScope {
            return admitted
        }
        return DataRootTransactionAccess(
            transaction: transaction,
            dataRoot: dataRoot,
            accessMode: accessMode,
            readScope: readScope
        )
    }

    func readProjection() -> any TransactionReadAccess {
        DatabaseReadAccessProjection(transaction: self)
    }

    func revoke() {
        transactionOwner.revoke()
    }

    var readProjectionCapabilities: TransactionCapabilities {
        let capabilities = physicalCapabilities
        return TransactionCapabilities(
            historicalReadVersion: capabilities.historicalReadVersion,
            readVersion: capabilities.readVersion
        )
    }

    func captureReadVersion() async throws -> UInt64? {
        let transaction = try transactionOwner.borrow()
        if let rooted = transaction as? DataRootTransactionAccess {
            return try await rooted.captureReadVersion()
        }
        guard transaction.capabilities.readVersion else { return nil }
        let signedVersion = try await transaction.getReadVersion()
        guard let version = UInt64(exactly: signedVersion) else {
            throw DatabaseExecutionReadPointError
                .backendReturnedInvalidVersion(signedVersion)
        }
        return version
    }

    private var physicalCapabilities: TransactionCapabilities {
        physicalCapabilitiesSnapshot
    }

    /// Executes the one get-only control operation used by runtime
    /// diagnostics without exposing transaction control to a read caller.
    func currentReadVersionForDiagnostics() async throws -> Int64 {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        let transaction = try transactionOwner.borrow()
        return try await transaction.getReadVersion()
    }

    var capabilities: TransactionCapabilities {
        // TransactionRunner applies configuration before this facade is
        // installed. Data-plane callers never receive transaction-control
        // capability, irrespective of read/write admission.
        .none
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
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try validateKey(key)
        let transaction = try transactionOwner.borrow()
        return try await transaction.getValue(for: key, snapshot: snapshot)
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        return try await getValue(for: key, snapshot: false)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        let transaction = try transactionOwner.borrow()
        guard !dataRoot.prefix.isEmpty else {
            return try await transaction.getKey(
                selector: selector,
                snapshot: snapshot
            )
        }
        let upperBound = try dataRootUpperBound()
        guard selector.key >= dataRoot.prefix,
              selector.key <= upperBound else {
            throw DatabaseReadTransactionError.keyOutsideDataRoot
        }
        if selector.offset == 1, selector.key == upperBound {
            return nil
        }

        let bounds: (begin: KeySelector, end: KeySelector, reverse: Bool)
        switch (selector.orEqual, selector.offset) {
        case (false, 1), (true, 1):
            bounds = (
                begin: selector,
                end: .firstGreaterOrEqual(upperBound),
                reverse: false
            )
        case (false, 0):
            bounds = (
                begin: .firstGreaterOrEqual(dataRoot.prefix),
                end: .firstGreaterOrEqual(min(selector.key, upperBound)),
                reverse: true
            )
        case (true, 0):
            bounds = (
                begin: .firstGreaterOrEqual(dataRoot.prefix),
                end: selector.key >= upperBound
                    ? .firstGreaterOrEqual(upperBound)
                    : .firstGreaterThan(selector.key),
                reverse: true
            )
        default:
            throw DatabaseReadTransactionError.unsupportedKeySelector
        }

        var cursor = transaction.rangeCursor(
            from: bounds.begin,
            to: bounds.end,
            limit: 1,
            reverse: bounds.reverse,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        let key: ByteString?
        do {
            key = try await cursor.next()?.0
        } catch let cleanupError as StorageRangeCleanupError {
            throw cleanupError
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
        guard let key else { return nil }
        guard dataRoot.contains(key), key < upperBound else {
            throw DatabaseReadTransactionError
                .backendReturnedKeyOutsideDataRoot
        }
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
        do {
            let scopeLease = try readScope?.beginRead()
            defer { scopeLease?.finish() }
            try validateRange(begin: begin, end: end)
            let transaction = try transactionOwner.borrow()
            let cursor = transaction.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
            let scopedCursor: KeyValueCursor
            if dataRoot.prefix.isEmpty {
                scopedCursor = cursor
            } else {
                scopedCursor = KeyValueCursor(
                    consuming: DatabaseRootRangeResult(
                        cursor: cursor,
                        dataRoot: dataRoot,
                        upperBound: try dataRootUpperBound()
                    )
                )
            }
            guard let readScope else { return scopedCursor }
            guard let scopeLease else {
                return failedCursor(
                    DatabaseReadTransactionError.snapshotClosed
                )
            }
            return try readScope.scope(
                scopedCursor,
                admittedBy: scopeLease
            )
        } catch let error as DatabaseReadTransactionError {
            return failedCursor(error)
        } catch {
            return failedCursor(error)
        }
    }

    func setValue(_ value: ByteString, for key: ByteString) throws {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try requireMutationAccess()
        try validateKey(key)
        let transaction = try transactionOwner.borrow()
        try transaction.setValue(value, for: key)
    }

    func clear(key: ByteString) throws {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try requireMutationAccess()
        try validateKey(key)
        let transaction = try transactionOwner.borrow()
        try transaction.clear(key: key)
    }

    func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try requireMutationAccess()
        try validateRange(beginKey: beginKey, endKey: endKey)
        let transaction = try transactionOwner.borrow()
        try transaction.clearRange(beginKey: beginKey, endKey: endKey)
    }

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try requireMutationAccess()
        try validateKey(key)
        if mutationType == .setVersionstampedKey {
            let operand = try VersionstampedMutationOperand(key)
            try operand.validateReplacement(
                afterProtectedPrefixByteCount: dataRoot.prefix.count
            )
        }
        let transaction = try transactionOwner.borrow()
        try transaction.atomicOp(
            key: key,
            param: param,
            mutationType: mutationType
        )
    }

    func setReadVersion(_ version: Int64) throws {
        throw DatabaseReadTransactionError
            .transactionControlUnavailable
    }

    func getReadVersion() async throws -> Int64 {
        throw DatabaseReadTransactionError
            .transactionControlUnavailable
    }

    func setOption(forOption option: TransactionOption) throws {
        throw DatabaseReadTransactionError
            .transactionControlUnavailable
    }

    func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws {
        throw DatabaseReadTransactionError
            .transactionControlUnavailable
    }

    func setOption(
        to value: Int,
        forOption option: TransactionOption
    ) throws {
        throw DatabaseReadTransactionError
            .transactionControlUnavailable
    }

    func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try requireMutationAccess()
        try validateRange(beginKey: beginKey, endKey: endKey)
        let transaction = try transactionOwner.borrow()
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
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try validateRange(beginKey: beginKey, endKey: endKey)
        let transaction = try transactionOwner.borrow()
        return try await transaction.getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
    }

    func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        let scopeLease = try readScope?.beginRead()
        defer { scopeLease?.finish() }
        try validateRange(beginKey: beginKey, endKey: endKey)
        let upperBound = dataRoot.prefix.isEmpty
            ? nil
            : try dataRootUpperBound()
        let transaction = try transactionOwner.borrow()
        let points = try await transaction.getRangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize
        )
        guard points.allSatisfy({ point in
            dataRoot.prefix.isEmpty
                || dataRoot.contains(point)
                || point == upperBound
        }) else {
            throw DatabaseReadTransactionError
                .backendReturnedKeyOutsideDataRoot
        }
        return points
    }

    func requestVersionstamp() -> any PendingTransactionVersionstamp {
        RejectedDataRootTransactionVersionstamp()
    }

    private func validateKey(_ key: ByteString) throws {
        guard dataRoot.prefix.isEmpty || dataRoot.contains(key) else {
            throw DatabaseReadTransactionError.keyOutsideDataRoot
        }
    }

    private func requireMutationAccess() throws {
        guard accessMode.allowsMutation else {
            throw DatabaseReadTransactionError.mutationRequiresWriteAccess
        }
    }

    private func validateRange(
        begin: KeySelector,
        end: KeySelector
    ) throws {
        guard begin.offset == 1,
              !end.orEqual,
              end.offset == 1 else {
            throw DatabaseReadTransactionError.unsupportedKeySelector
        }
        try validateRange(beginKey: begin.key, endKey: end.key)
        if !dataRoot.prefix.isEmpty, begin.orEqual {
            guard begin.key < (try dataRootUpperBound()) else {
                throw DatabaseReadTransactionError.rangeOutsideDataRoot
            }
        }
    }

    private func validateRange(
        beginKey: ByteString,
        endKey: ByteString
    ) throws {
        guard beginKey <= endKey else {
            throw DatabaseReadTransactionError.rangeOutsideDataRoot
        }
        guard !dataRoot.prefix.isEmpty else { return }
        let upperBound = try dataRootUpperBound()
        guard beginKey >= dataRoot.prefix, endKey <= upperBound else {
            throw DatabaseReadTransactionError.rangeOutsideDataRoot
        }
    }

    private func dataRootUpperBound() throws -> ByteString {
        do {
            return try strinc(dataRoot.prefix)
        } catch {
            throw DatabaseReadTransactionError.invalidDataRoot
        }
    }

    private func failedCursor(
        _ error: any Error
    ) -> KeyValueCursor {
        KeyValueCursor(
            consuming: FailedDatabaseRootRangeResult(error: error)
        )
    }
}

/// Erases mutation and transaction-control capabilities at read API
/// boundaries. The wrapped access is already confined to one database root.
package struct DatabaseReadAccessProjection: TransactionReadAccess {
    let transaction: any TransactionReadAccess
    private let advertisedCapabilities: TransactionCapabilities

    package init(transaction: any TransactionReadAccess) {
        self.transaction = transaction
        self.advertisedCapabilities =
            (transaction as? DataRootTransactionAccess)?
                .readProjectionCapabilities
            ?? transaction.capabilities
    }

    package var capabilities: TransactionCapabilities {
        advertisedCapabilities
    }

    package func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await transaction.getValue(for: key, snapshot: snapshot)
    }

    package func getValue(for key: ByteString) async throws -> ByteString? {
        try await transaction.getValue(for: key)
    }

    package func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await transaction.getKey(selector: selector, snapshot: snapshot)
    }

    package func rangeCursor(
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

    package func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        try await transaction.getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
    }

    package func getRangeSplitPoints(
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

}

private struct DatabaseRootRangeResult: TransactionRangeResult {
    let cursor: KeyValueCursor
    let dataRoot: Subspace
    let upperBound: ByteString

    func makeCursor() -> DatabaseRootRangeCursor {
        DatabaseRootRangeCursor(
            cursor: cursor,
            dataRoot: dataRoot,
            upperBound: upperBound
        )
    }
}

private struct DatabaseRootRangeCursor: TransactionRangeCursor {
    private var cursor: KeyValueCursor
    private let dataRoot: Subspace
    private let upperBound: ByteString

    init(
        cursor: KeyValueCursor,
        dataRoot: Subspace,
        upperBound: ByteString
    ) {
        self.cursor = cursor
        self.dataRoot = dataRoot
        self.upperBound = upperBound
    }

    mutating func next() async throws -> (ByteString, ByteString)? {
        guard let element = try await cursor.next() else { return nil }
        guard dataRoot.contains(element.0), element.0 < upperBound else {
            let boundaryError = DatabaseReadTransactionError
                .backendReturnedKeyOutsideDataRoot
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: boundaryError,
                    cleanupError: error
                )
            }
            throw boundaryError
        }
        return element
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {
        try await cursor.finish()
    }
}

private struct FailedDatabaseRootRangeResult: TransactionRangeResult {
    let error: any Error

    func makeCursor() -> FailedDatabaseRootRangeCursor {
        FailedDatabaseRootRangeCursor(error: error)
    }
}

private struct FailedDatabaseRootRangeCursor: TransactionRangeCursor {
    let error: any Error

    mutating func next() async throws -> (ByteString, ByteString)? {
        throw error
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {}
}

private struct RejectedDataRootTransactionVersionstamp:
    PendingTransactionVersionstamp
{
    var value: TransactionVersionstamp {
        get async throws {
            throw DatabaseReadTransactionError
                .versionstampUnavailable
        }
    }
}
