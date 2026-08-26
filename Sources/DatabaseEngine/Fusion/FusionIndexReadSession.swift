import DatabaseTypes
import StorageKit
import Synchronization

/// One revocable physical-read authority. Every operation is serial because
/// the caller-owned transaction does not permit overlapping operations.
final class FusionIndexReadSession: FusionIndexReadAccess, Sendable {
    private struct State: Sendable {
        var isActive = true
        var isOperationInFlight = false
        var invalidationWaiters: [CheckedContinuation<Void, Never>] = []
        var cursors: [ObjectIdentifier: FusionIndexReadCursorState] = [:]
        var accountedCursorCapacity = 0
        var transaction: (any TransactionReadAccess)?
    }

    private struct CursorRegistrySlot: Sendable {
        let identifier: ObjectIdentifier
        let cursor: FusionIndexReadCursorState
    }

    let index: ReadableIndex

    private let workMeter: DatabaseWorkMeter
    private let snapshot: Bool
    private let cursorRegistryLayout: DatabaseRetainedHashTableLayout
    private let cursorRegistryReservation: DatabaseIntermediateReservation
    private let lowerBound: ByteString
    private let upperBound: ByteString
    private let state: Mutex<State>
    private let invalidation = FusionIndexReadSessionInvalidation()

    init(
        index: ReadableIndex,
        admission: FusionIndexReadAdmission
    ) throws {
        let cursorRegistryLayout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(
                MemoryLayout<[
                    ObjectIdentifier: FusionIndexReadCursorState
                ]>.stride
            ),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<CursorRegistrySlot>.stride)
            )
        )
        self.index = index
        self.workMeter = admission.workMeter
        self.snapshot = admission.snapshot
        self.cursorRegistryLayout = cursorRegistryLayout
        self.cursorRegistryReservation = try admission.workMeter
            .reserveIntermediate(
            bytes: cursorRegistryLayout.containerByteCount,
            at: .indexScan
        )
        let range = try index.subspace.prefixRange()
        self.lowerBound = range.begin
        self.upperBound = range.end
        self.state = Mutex(State(transaction: admission.transaction))
    }

    #if DEBUG
    static func testing(
        index: ReadableIndex,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionIndexReadSession {
        try FusionIndexReadSession(
            index: index,
            admission: FusionIndexReadAdmission.testing(
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
        )
    }
    #endif

    func getValue(key: ByteString) async throws -> FusionIndexReadValue? {
        let transaction = try beginOperation()
        defer { endOperation() }
        try workMeter.consume(at: .indexScan)
        try DatabaseByteProcessingMeter.consume(
            byteCount: key.count,
            workMeter: workMeter,
            stage: .indexScan
        )
        try validatePointKey(key)
        let value = try await transaction.readPointValue(
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            at: .indexScan
        )
        guard let value else { return nil }
        try validateValueSize(value)
        try DatabaseByteProcessingMeter.consume(
            byteCount: value.count,
            workMeter: workMeter,
            stage: .indexScan
        )
        return FusionIndexReadValue(
            bytes: value
        )
    }

    func rangeCursor(
        from beginKey: ByteString,
        to endKey: ByteString,
        reverse: Bool
    ) throws -> FusionIndexReadCursor {
        let transaction = try beginOperation()
        defer { endOperation() }
        guard beginKey >= lowerBound,
              endKey <= upperBound,
              beginKey < endKey else {
            throw FusionExecutionContractError.invalidIndexReadRange(
                index: index.descriptor.name
            )
        }
        let cursorReservation = try workMeter.reserveIntermediate(
            rows: 1,
            bytes: UInt64(MemoryLayout<KeyValueCursor>.stride) + 64,
            at: .indexScan
        )
        try admitCursorRegistration()
        let cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        let cursorState = FusionIndexReadCursorState(
            session: self,
            cursor: cursor,
            reservation: cursorReservation
        )
        registerDuringOperation(cursorState)
        return FusionIndexReadCursor(state: cursorState)
    }

    /// Revokes this capability and authoritatively closes all feature-owned
    /// cursors before the shared transaction advances to another input.
    func invalidate() async throws {
        try await invalidation.run { [self] in
            try await invalidateOnce()
        }
    }

    private func invalidateOnce() async throws {
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                state.isActive = false
                guard state.isOperationInFlight else { return true }
                state.invalidationWaiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }

        var firstCleanupError: (any Error)?
        do {
            let drained = state.withLock { state in
                let cursors = state.cursors
                state.cursors = [:]
                state.accountedCursorCapacity = 0
                return cursors
            }
            for cursor in drained.values {
                do {
                    try await cursor.finishForInvalidation()
                } catch where firstCleanupError == nil {
                    firstCleanupError = error
                } catch {
                    // Every cursor is closed; the first failure remains
                    // authoritative.
                }
            }
        }
        // Invalidation is terminal, so the registry container and every
        // capacity claim end here rather than following the escaped handle.
        cursorRegistryReservation.release()
        state.withLock { state in
            state.transaction = nil
        }
        if let firstCleanupError { throw firstCleanupError }
        try workMeter.checkpoint(at: .indexScan)
    }

    internal func beginCursorOperation() throws {
        _ = try beginOperation()
        try workMeter.checkpoint(at: .indexScan)
    }

    internal func checkpointCursorOperation() throws {
        try workMeter.checkpoint(at: .indexScan)
    }

    internal func endCursorOperation() {
        endOperation()
    }

    internal func validateCursorKey(_ key: ByteString) throws {
        guard key >= lowerBound, key < upperBound else {
            throw FusionExecutionContractError.indexCursorEscapedAdmittedSubspace(
                index: index.descriptor.name
            )
        }
    }

    internal func removeCursor(_ cursor: FusionIndexReadCursorState) {
        _ = state.withLock { state in
            state.cursors.removeValue(forKey: ObjectIdentifier(cursor))
        }
    }

    private func beginOperation() throws -> any TransactionReadAccess {
        let result: Result<any TransactionReadAccess, FusionExecutionContractError> =
            state.withLock { state in
            guard state.isActive else {
                return .failure(
                    .indexReadSessionInvalidated(
                        index: index.descriptor.name
                    )
                )
            }
            guard !state.isOperationInFlight else {
                return .failure(
                    .concurrentIndexReadSessionOperation(
                        index: index.descriptor.name
                    )
                )
            }
            guard let transaction = state.transaction else {
                return .failure(
                    .indexReadSessionInvalidated(
                        index: index.descriptor.name
                    )
                )
            }
            state.isOperationInFlight = true
            return .success(transaction)
        }
        return try result.get()
    }

    private func endOperation() {
        let waiters = state.withLock { state in
            state.isOperationInFlight = false
            guard !state.isActive else {
                return [CheckedContinuation<Void, Never>]()
            }
            let waiters = state.invalidationWaiters
            state.invalidationWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters { waiter.resume() }
    }

    private func registerDuringOperation(
        _ cursor: FusionIndexReadCursorState
    ) {
        state.withLock { state in
            precondition(state.isOperationInFlight)
            state.cursors[ObjectIdentifier(cursor)] = cursor
        }
    }

    private func admitCursorRegistration() throws {
        try state.withLock { state in
            precondition(state.isOperationInFlight)
            let (requiredCount, overflow) = state.cursors.count
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseRetainedHashTableLayoutError.capacityOverflow(
                    currentCapacity: state.accountedCursorCapacity
                )
            }
            let growth = try cursorRegistryLayout.growth(
                from: state.accountedCursorCapacity,
                toFit: requiredCount
            )
            try cursorRegistryReservation.reserveAdditional(
                bytes: growth.additionalByteCount,
                at: .indexScan
            )
            if growth.capacity != state.accountedCursorCapacity {
                state.cursors.reserveCapacity(growth.capacity)
                state.accountedCursorCapacity = growth.capacity
            }
        }
    }

    private func validatePointKey(_ key: ByteString) throws {
        guard key >= lowerBound, key < upperBound else {
            throw FusionExecutionContractError.indexReadOutsideAdmittedSubspace(
                index: index.descriptor.name
            )
        }
    }

    internal func retainRow(
        key: ByteString,
        value: ByteString
    ) throws -> FusionIndexReadRow {
        try validateKeySize(key)
        try validateValueSize(value)
        try DatabaseByteProcessingMeter.consume(
            byteCount: UInt64(key.count + value.count),
            workMeter: workMeter,
            stage: .indexScan
        )
        let retainedBytes = UInt64(key.count + value.count)
        let reservation = try workMeter.reserveIntermediate(
            rows: 1,
            bytes: retainedBytes,
            at: .indexScan
        )
        return FusionIndexReadRow(
            key: try DatabaseRetainedByteString.make(
                key,
                reservation: reservation,
                at: .indexScan
            ),
            value: try DatabaseRetainedByteString.make(
                value,
                reservation: reservation,
                at: .indexScan
            )
        )
    }

    internal func admitCursorRead() throws {
        try workMeter.consume(at: .indexScan)
    }
}
