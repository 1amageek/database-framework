import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Canonical full-scan cleanup")
struct CanonicalFullScanCleanupTests {
    @Test("successful retained scan finishes the cursor exactly once")
    func successfulScanFinishesExactlyOnce() async throws {
        let first = try makeEnvelope(payload: [0x01])
        let second = try makeEnvelope(payload: [0x02, 0x03])
        let probe = CursorProbe()
        let transaction = ScriptedTransaction(
            rows: [
                ScriptedRow(key: [0x01], value: first),
                ScriptedRow(key: [0x02], value: second),
            ],
            probe: probe
        )
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: Subspace(prefix: Tuple("canonical", "blobs").pack()),
            configuration: .v1
        )
        let meter = makeMeter()
        var receivedCount = 0
        var receivedByteCount = 0

        try await storage.consumeRetainedScan(
            begin: [0x00],
            end: [0xFF],
            workMeter: meter,
            stage: .storageRow
        ) { _, data in
            receivedCount += 1
            receivedByteCount += data.count
        }

        #expect(receivedCount == 2)
        #expect(receivedByteCount == 3)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("envelope decode failure finishes the cursor before escaping")
    func decodeFailureFinishesCursor() async throws {
        let probe = CursorProbe()
        let transaction = ScriptedTransaction(
            rows: [ScriptedRow(key: [0x01], value: [0x00])],
            probe: probe
        )
        let storage = makeStorage(transaction: transaction)
        let meter = makeMeter()
        var callbackInvocations = 0
        var failure: (any Error)?

        do {
            try await storage.consumeRetainedScan(
                begin: [0x00],
                end: [0xFF],
                workMeter: meter,
                stage: .storageRow
            ) { _, _ in
                callbackInvocations += 1
            }
        } catch {
            failure = error
        }

        #expect(failure as? ItemStorageError == .notEnvelopeFormat)
        #expect(callbackInvocations == 0)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("authorization failure finishes the cursor before escaping")
    func authorizationFailureFinishesCursor() async throws {
        let probe = CursorProbe()
        let transaction = ScriptedTransaction(
            rows: [
                ScriptedRow(
                    key: [0x01],
                    value: try makeEnvelope(payload: [0x01])
                )
            ],
            probe: probe
        )
        let storage = makeStorage(transaction: transaction)
        let meter = makeMeter()
        var failure: (any Error)?

        do {
            try await storage.consumeRetainedScan(
                begin: [0x00],
                end: [0xFF],
                workMeter: meter,
                stage: .storageRow
            ) { _, _ in
                throw CanonicalFullScanTestError.authorizationDenied
            }
        } catch {
            failure = error
        }

        #expect(
            failure as? CanonicalFullScanTestError
                == .authorizationDenied
        )
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("work-limit failure finishes the cursor before escaping")
    func workLimitFailureFinishesCursor() async throws {
        let probe = CursorProbe()
        let transaction = ScriptedTransaction(
            rows: [
                ScriptedRow(
                    key: [0x01],
                    value: try makeEnvelope(payload: [0x01])
                )
            ],
            probe: probe
        )
        let storage = makeStorage(transaction: transaction)
        let meter = makeMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: 0
            )
        )
        var failure: (any Error)?

        do {
            try await storage.consumeRetainedScan(
                begin: [0x00],
                end: [0xFF],
                workMeter: meter,
                stage: .storageRow
            ) { _, _ in
                Issue.record("The work limit should reject before the callback")
            }
        } catch {
            failure = error
        }

        #expect(failure is DatabaseWorkLimitError)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("cancellation after a decoded row finishes the cursor")
    func cancellationFinishesCursor() async throws {
        let probe = CursorProbe()
        let transaction = ScriptedTransaction(
            rows: [
                ScriptedRow(
                    key: [0x01],
                    value: try makeEnvelope(payload: [0x01])
                )
            ],
            probe: probe
        )
        let storage = makeStorage(transaction: transaction)
        let meter = makeMeter()
        let callbackBarrier = StorageOperationBarrier()
        let operation = Task {
            try await storage.consumeRetainedScan(
                begin: [0x00],
                end: [0xFF],
                workMeter: meter,
                stage: .storageRow
            ) { _, _ in
                await callbackBarrier.enterAndWait()
                try Task.checkCancellation()
            }
        }

        await callbackBarrier.waitUntilEntered()
        operation.cancel()
        callbackBarrier.release()

        switch await operation.result {
        case .success:
            Issue.record("Cancelled scan completed successfully")
        case .failure(let error):
            #expect(error is CancellationError)
        }
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("body and cursor cleanup failures remain distinguishable")
    func bodyAndCleanupFailuresAreCombined() async throws {
        let probe = CursorProbe()
        let transaction = ScriptedTransaction(
            rows: [
                ScriptedRow(
                    key: [0x01],
                    value: try makeEnvelope(payload: [0x01])
                )
            ],
            probe: probe,
            finishError: .cleanupFailed
        )
        let storage = makeStorage(transaction: transaction)
        let meter = makeMeter()
        var failure: (any Error)?

        do {
            try await storage.consumeRetainedScan(
                begin: [0x00],
                end: [0xFF],
                workMeter: meter,
                stage: .storageRow
            ) { _, _ in
                throw CanonicalFullScanTestError.authorizationDenied
            }
        } catch {
            failure = error
        }

        guard let cleanup = failure as? StorageRangeCleanupError else {
            Issue.record("Expected StorageRangeCleanupError, got \(String(describing: failure))")
            return
        }
        #expect(
            cleanup.iterationError as? CanonicalFullScanTestError
                == .authorizationDenied
        )
        #expect(cleanup.cleanupError as? CanonicalFullScanCursorError == .cleanupFailed)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeStorage(transaction: ScriptedTransaction) -> ItemStorage {
        ItemStorage(
            transaction: transaction,
            blobsSubspace: Subspace(prefix: Tuple("canonical", "blobs").pack()),
            configuration: .v1
        )
    }

    private func makeMeter(
        budget: ExecutionBudget = ExecutionBudget()
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: budget,
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeEnvelope(payload: ByteString) throws -> ByteString {
        try ItemEnvelope.inline(
            payload: payload,
            encoding: .identity,
            plainByteCount: UInt64(payload.count),
            checksum: ItemChecksum.crc32c(payload)
        ).serialize()
    }
}

private struct ScriptedRow: Sendable {
    let key: ByteString
    let value: ByteString
}

private final class CursorProbe: Sendable {
    private let state = Mutex(0)

    var finishCount: Int {
        state.withLock { $0 }
    }

    func recordFinish() {
        state.withLock { $0 += 1 }
    }
}

private struct ScriptedTransaction: TransactionAccess {
    let transactionDomain = StorageTransactionDomain()
    let rows: [ScriptedRow]
    let probe: CursorProbe
    let finishError: CanonicalFullScanCursorError?

    init(
        rows: [ScriptedRow],
        probe: CursorProbe,
        finishError: CanonicalFullScanCursorError? = nil
    ) {
        self.rows = rows
        self.probe = probe
        self.finishError = finishError
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        _ = key
        _ = snapshot
        return nil
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        _ = key
        return nil
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        _ = selector
        _ = snapshot
        return nil
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        _ = begin
        _ = end
        _ = limit
        _ = reverse
        _ = snapshot
        _ = streamingMode
        return KeyValueCursor(
            consuming: ScriptedRangeResult(
                rows: rows,
                probe: probe,
                finishError: finishError
            )
        )
    }

    func setValue(_ value: ByteString, for key: ByteString) throws {
        _ = value
        _ = key
        throw CanonicalFullScanTransactionError.mutation
    }

    func clear(key: ByteString) throws {
        _ = key
        throw CanonicalFullScanTransactionError.mutation
    }

    func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        _ = beginKey
        _ = endKey
        throw CanonicalFullScanTransactionError.mutation
    }

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        _ = key
        _ = param
        _ = mutationType
        throw CanonicalFullScanTransactionError.mutation
    }
}

private struct ScriptedRangeResult: TransactionRangeResult {
    let rows: [ScriptedRow]
    let probe: CursorProbe
    let finishError: CanonicalFullScanCursorError?

    func makeCursor() -> Cursor {
        Cursor(rows: rows, probe: probe, finishError: finishError)
    }

    struct Cursor: TransactionRangeCursor {
        let rows: [ScriptedRow]
        let probe: CursorProbe
        let finishError: CanonicalFullScanCursorError?
        var offset = 0
        var isFinished = false

        mutating func next() async throws -> (ByteString, ByteString)? {
            guard !isFinished, offset < rows.count else {
                return nil
            }
            let row = rows[offset]
            offset += 1
            return (row.key, row.value)
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            _ = actor
            guard !isFinished else { return }
            isFinished = true
            probe.recordFinish()
            if let finishError {
                throw finishError
            }
        }
    }
}

private enum CanonicalFullScanTestError: Error, Equatable, Sendable {
    case authorizationDenied
}

private enum CanonicalFullScanCursorError: Error, Equatable, Sendable {
    case cleanupFailed
}

private enum CanonicalFullScanTransactionError: Error, Equatable, Sendable {
    case mutation
}
