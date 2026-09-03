import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing

@testable import DatabaseEngine

@Persistable
private struct FusionIndexReadSessionItem {
    #Index(
        .ordered(
            name: "fusion_session_value",
            keys: [.ascending(\FusionIndexReadSessionItem.value)],
            unique: false
        )
    )

    var id: String
    var value: Int64
}

@Suite("Fusion index read session")
struct FusionIndexReadSessionTests {
    @Test("Index point and range reads preserve the resolved consistency")
    func readsPreserveResolvedConsistency() async throws {
        for snapshot in [false, true] {
            let entity = try FusionIndexReadSessionItem.schemaEntity
            let descriptor = try #require(entity.indexes.first)
            let root = Subspace("fusion-session", snapshot ? "snapshot" : "serializable")
            let entries = root.subspace("entries")
            let key = entries.pack(Tuple("one"))
            let engine = InMemoryEngine()
            defer { engine.requestShutdown() }
            try await engine.withTransaction { transaction in
                try transaction.setValue([1], for: key)
            }
            let meter = makeMeter()

            try await engine.withTransaction { transaction in
                let recording = SnapshotRecordingTransaction(base: transaction)
                do {
                    let session = try FusionIndexReadSession.testing(
                        index: ReadableIndex(
                            descriptor: descriptor,
                            physicalLayout: try IndexPhysicalLayout(
                                name: "test.fusion-session",
                                revision: 1
                            ),
                            subspace: root
                        ),
                        transaction: recording,
                        snapshot: snapshot,
                        workMeter: meter
                    )
                    let expectedPointMaximum = Int(
                        min(
                            meter.budget.maximumIntermediateBytes
                                - meter.retainedIntermediateBytes,
                            UInt64(Int.max)
                        )
                    )
                    #expect(try await session.getValue(key: key)?.bytes == [1])
                    #expect(recording.pointMaximums == [expectedPointMaximum])
                    let cursor = try openFusionSubspaceCursor(
                        using: session,
                        in: entries,
                        reverse: false
                    )
                    #expect(try await cursor.next()?.key == key)
                    try await cursor.finish()
                    try await session.invalidate()
                }
                #expect(recording.pointSnapshots == [snapshot])
                #expect(recording.pointMaximums.count == 1)
                #expect(recording.rangeSnapshots == [snapshot])
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
            await engine.waitUntilShutdown()
        }
    }

    @Test("Cursor admission fails before an unaccounted backend cursor opens")
    func cursorAdmissionIsResourceBounded() async throws {
        let entity = try FusionIndexReadSessionItem.schemaEntity
        let descriptor = try #require(entity.indexes.first)
        let root = Subspace("fusion-session", "bounded")
        let entries = root.subspace("entries")
        let dictionaryBytes = UInt64(
            MemoryLayout<[
                ObjectIdentifier: FusionIndexReadCursorState
            ]>.stride
        )
        let slotBytes = UInt64(
            MemoryLayout<(ObjectIdentifier, FusionIndexReadCursorState)>.stride
        )
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 100,
                maximumIntermediateRows: 1,
                maximumIntermediateBytes: dictionaryBytes + slotBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let engine = InMemoryEngine()
        defer { engine.requestShutdown() }

        try await engine.withTransaction { transaction in
            let recording = SnapshotRecordingTransaction(base: transaction)
            do {
                let session = try FusionIndexReadSession.testing(
                    index: ReadableIndex(
                        descriptor: descriptor,
                        physicalLayout: try IndexPhysicalLayout(
                            name: "test.fusion-session",
                            revision: 1
                        ),
                        subspace: root
                    ),
                    transaction: recording,
                    snapshot: false,
                    workMeter: meter
                )
                #expect {
                    _ = try openFusionSubspaceCursor(
                        using: session,
                        in: entries,
                        reverse: false
                    )
                } throws: { error in
                    error is DatabaseWorkLimitError
                }
                #expect(recording.rangeSnapshots.isEmpty)
                try await session.invalidate()
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
        await engine.waitUntilShutdown()
    }

    @Test("Cursor failures close the backend exactly once")
    func cursorFailuresCloseBackendExactlyOnce() async throws {
        for outcome in [
            ScriptedCursorOutcome.iterationFailure,
            .cancellationFailure,
        ] {
            let probe = ScriptedCursorProbe()
            let meter = makeMeter()
            do {
                let session = try makeScriptedSession(
                    outcome: outcome,
                    cleanupFails: false,
                    probe: probe,
                    workMeter: meter
                )
                do {
                    let cursor = try openFusionSubspaceCursor(
                        using: session,
                        in: session.index.subspace.subspace("entries"),
                        reverse: false
                    )
                    do {
                        _ = try await cursor.next()
                        Issue.record("The scripted cursor must fail")
                    } catch {
                        switch outcome {
                        case .iterationFailure:
                            #expect(
                                error as? ScriptedCursorError == .iteration
                            )
                        case .cancellationFailure:
                            #expect(error is CancellationError)
                        case .endOfStream, .row, .suspendedRow:
                            Issue.record("Unexpected scripted cursor outcome")
                        }
                    }
                }
                try await session.invalidate()
            }
            #expect(probe.nextCount == 1)
            #expect(probe.finishCount == 1)
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Iteration and cleanup failures keep one authoritative wrapper")
    func cursorPreservesIterationAndCleanupFailure() async throws {
        let probe = ScriptedCursorProbe()
        let meter = makeMeter()
        do {
            let session = try makeScriptedSession(
                outcome: .iterationFailure,
                cleanupFails: true,
                probe: probe,
                workMeter: meter
            )
            do {
                let cursor = try openFusionSubspaceCursor(
                    using: session,
                    in: session.index.subspace.subspace("entries"),
                    reverse: false
                )
                do {
                    _ = try await cursor.next()
                    Issue.record("The scripted cursor must fail")
                } catch let cleanup as StorageRangeCleanupError {
                    #expect(
                        cleanup.iterationError as? ScriptedCursorError
                            == .iteration
                    )
                    #expect(
                        cleanup.cleanupError as? ScriptedCursorError
                            == .cleanup
                    )
                    #expect(
                        !(cleanup.iterationError is StorageRangeCleanupError)
                    )
                }
            }
            try await session.invalidate()
        }
        #expect(probe.nextCount == 1)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Terminal cleanup failures remain cleanup-only")
    func cursorPreservesTerminalCleanupFailure() async throws {
        let probe = ScriptedCursorProbe()
        let meter = makeMeter()
        do {
            let session = try makeScriptedSession(
                outcome: .endOfStream,
                cleanupFails: true,
                probe: probe,
                workMeter: meter
            )
            do {
                let cursor = try openFusionSubspaceCursor(
                    using: session,
                    in: session.index.subspace.subspace("entries"),
                    reverse: false
                )
                do {
                    _ = try await cursor.next()
                    Issue.record("Terminal cleanup must fail")
                } catch let cleanup as StorageRangeTerminalCleanupError {
                    #expect(
                        cleanup.cleanupError as? ScriptedCursorError
                            == .cleanup
                    )
                } catch {
                    Issue.record(
                        "Expected StorageRangeTerminalCleanupError, got \(error)"
                    )
                }
            }
            try await session.invalidate()
        }
        #expect(probe.nextCount == 1)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation after backend I/O closes the cursor exactly once")
    func postAwaitCancellationClosesBackendExactlyOnce() async throws {
        let probe = ScriptedCursorProbe()
        let gate = ScriptedAdvanceGate()
        let meter = makeMeter()
        let session = try makeScriptedSession(
            outcome: .suspendedRow(gate),
            cleanupFails: false,
            probe: probe,
            workMeter: meter
        )
        let cursor = try openFusionSubspaceCursor(
            using: session,
            in: session.index.subspace.subspace("entries"),
            reverse: false
        )
        let advance = Task {
            try await cursor.next()
        }
        await gate.waitUntilAdvanceStarts()
        advance.cancel()
        gate.resume(
            with: (
                session.index.subspace.subspace("entries").pack(Tuple("one")),
                [0x01]
            )
        )

        do {
            _ = try await advance.value
            Issue.record("The post-I/O checkpoint must observe cancellation")
        } catch is CancellationError {
        } catch {
            Issue.record("Expected CancellationError, got \(error)")
        }
        try await session.invalidate()

        #expect(probe.nextCount == 1)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Invalidation closes every sibling after a cleanup failure")
    func invalidationClosesEverySiblingAfterCleanupFailure() async throws {
        let probe = ScriptedCursorProbe()
        let meter = makeMeter()
        do {
            let entries = Subspace("fusion-session", "scripted")
                .subspace("entries")
            let session = try makeScriptedSession(
                outcome: .row(entries.pack(Tuple("one")), [0x01]),
                cleanupFails: true,
                probe: probe,
                workMeter: meter
            )
            let first = try openFusionSubspaceCursor(
                using: session,
                in: entries,
                reverse: false
            )
            let second = try openFusionSubspaceCursor(
                using: session,
                in: entries,
                reverse: false
            )
            #expect(try await first.next() != nil)
            #expect(try await second.next() != nil)

            await #expect {
                try await session.invalidate()
            } throws: { error in
                error as? ScriptedCursorError == .cleanup
            }
        }
        #expect(probe.nextCount == 2)
        #expect(probe.finishCount == 2)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Concurrent invalidations share one authoritative cleanup")
    func concurrentInvalidationsShareCleanup() async throws {
        let probe = ScriptedCursorProbe()
        let finishGate = ScriptedFinishGate()
        let meter = makeMeter()
        let entries = Subspace("fusion-session", "scripted")
            .subspace("entries")
        let session = try makeScriptedSession(
            outcome: .row(entries.pack(Tuple("one")), [0x01]),
            cleanupFails: true,
            probe: probe,
            workMeter: meter,
            finishGate: finishGate
        )
        let cursor = try openFusionSubspaceCursor(
            using: session,
            in: entries,
            reverse: false
        )
        #expect(try await cursor.next() != nil)

        let first = Task {
            do {
                try await session.invalidate()
                return nil as ScriptedCursorError?
            } catch {
                return error as? ScriptedCursorError
            }
        }
        await finishGate.waitUntilFinishStarts()
        let second = Task {
            do {
                try await session.invalidate()
                return nil as ScriptedCursorError?
            } catch {
                return error as? ScriptedCursorError
            }
        }
        await Task.yield()
        finishGate.resume()

        #expect(await first.value == .cleanup)
        #expect(await second.value == .cleanup)
        #expect(probe.finishCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("An escaped cursor remains invalid after session invalidation")
    func escapedCursorCannotRecoverAfterInvalidation() async throws {
        let probe = ScriptedCursorProbe()
        let meter = makeMeter()
        let entries = Subspace("fusion-session", "scripted")
            .subspace("entries")
        let session = try makeScriptedSession(
            outcome: .row(entries.pack(Tuple("one")), [0x01]),
            cleanupFails: false,
            probe: probe,
            workMeter: meter
        )
        let cursor = try openFusionSubspaceCursor(
            using: session,
            in: entries,
            reverse: false
        )

        try await session.invalidate()

        for _ in 0..<2 {
            await #expect {
                _ = try await cursor.next()
            } throws: { error in
                error as? FusionExecutionContractError
                    == .indexReadSessionInvalidated(
                        index: session.index.descriptor.name
                    )
            }
        }
        #expect(probe.nextCount == 0)
        #expect(probe.finishCount == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeScriptedSession(
        outcome: ScriptedCursorOutcome,
        cleanupFails: Bool,
        probe: ScriptedCursorProbe,
        workMeter: DatabaseWorkMeter,
        finishGate: ScriptedFinishGate? = nil
    ) throws -> FusionIndexReadSession {
        let entity = try FusionIndexReadSessionItem.schemaEntity
        let descriptor = try #require(entity.indexes.first)
        return try FusionIndexReadSession.testing(
            index: ReadableIndex(
                descriptor: descriptor,
                physicalLayout: try IndexPhysicalLayout(
                    name: "test.fusion-session",
                    revision: 1
                ),
                subspace: Subspace("fusion-session", "scripted")
            ),
            transaction: ScriptedReadTransaction(
                outcome: outcome,
                cleanupFails: cleanupFails,
                probe: probe,
                finishGate: finishGate
            ),
            snapshot: false,
            workMeter: workMeter
        )
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 64 * 1_024
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}

private enum ScriptedCursorOutcome: Sendable {
    case iterationFailure
    case cancellationFailure
    case endOfStream
    case row(ByteString, ByteString)
    case suspendedRow(ScriptedAdvanceGate)
}

private enum ScriptedCursorError: Error, Equatable, Sendable {
    case iteration
    case cleanup
}

private final class ScriptedCursorProbe: Sendable {
    private struct State: Sendable {
        var nextCount = 0
        var finishCount = 0
    }

    private let state = Mutex(State())

    var nextCount: Int { state.withLock { $0.nextCount } }
    var finishCount: Int { state.withLock { $0.finishCount } }

    func recordNext() {
        state.withLock { $0.nextCount += 1 }
    }

    func recordFinish() {
        state.withLock { $0.finishCount += 1 }
    }
}

private struct ScriptedRangeResult: TransactionRangeResult {
    let outcome: ScriptedCursorOutcome
    let cleanupFails: Bool
    let probe: ScriptedCursorProbe
    let finishGate: ScriptedFinishGate?

    func makeCursor() -> ScriptedRangeCursor {
        ScriptedRangeCursor(
            outcome: outcome,
            cleanupFails: cleanupFails,
            probe: probe,
            finishGate: finishGate
        )
    }
}

private struct ScriptedRangeCursor: TransactionRangeCursor {
    let outcome: ScriptedCursorOutcome
    let cleanupFails: Bool
    let probe: ScriptedCursorProbe
    let finishGate: ScriptedFinishGate?
    private var didAdvance = false

    mutating func next() async throws -> (ByteString, ByteString)? {
        probe.recordNext()
        guard !didAdvance else { return nil }
        didAdvance = true
        switch outcome {
        case .iterationFailure:
            throw ScriptedCursorError.iteration
        case .cancellationFailure:
            throw CancellationError()
        case .endOfStream:
            return nil
        case .row(let key, let value):
            return (key, value)
        case .suspendedRow(let gate):
            return await gate.suspendAdvance()
        }
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {
        probe.recordFinish()
        await finishGate?.suspendFinish()
        if cleanupFails {
            throw ScriptedCursorError.cleanup
        }
    }
}

private final class ScriptedFinishGate: Sendable {
    private struct State: Sendable {
        var finishStarted = false
        var resumeRequested = false
        var startWaiters: [CheckedContinuation<Void, Never>] = []
        var finishContinuation: CheckedContinuation<Void, Never>?
    }

    private let state = Mutex(State())

    func waitUntilFinishStarts() async {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if state.finishStarted {
                    continuation.resume()
                } else {
                    state.startWaiters.append(continuation)
                }
            }
        }
    }

    func suspendFinish() async {
        let waiters = state.withLock { state in
            state.finishStarted = true
            let waiters = state.startWaiters
            state.startWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                if state.resumeRequested {
                    return true
                }
                precondition(state.finishContinuation == nil)
                state.finishContinuation = continuation
                return false
            }
            if resumeImmediately { continuation.resume() }
        }
    }

    func resume() {
        let continuation = state.withLock { state in
            let continuation = state.finishContinuation
            state.finishContinuation = nil
            if continuation == nil {
                state.resumeRequested = true
            }
            return continuation
        }
        continuation?.resume()
    }
}

private final class ScriptedAdvanceGate: Sendable {
    private struct State: Sendable {
        var advanceStarted = false
        var startWaiters: [CheckedContinuation<Void, Never>] = []
        var advanceContinuation:
            CheckedContinuation<(ByteString, ByteString)?, Never>?
    }

    private let state = Mutex(State())

    func waitUntilAdvanceStarts() async {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if state.advanceStarted {
                    continuation.resume()
                } else {
                    state.startWaiters.append(continuation)
                }
            }
        }
    }

    func suspendAdvance() async -> (ByteString, ByteString)? {
        let waiters = state.withLock { state in
            state.advanceStarted = true
            let waiters = state.startWaiters
            state.startWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
        return await withCheckedContinuation { continuation in
            state.withLock { state in
                precondition(state.advanceContinuation == nil)
                state.advanceContinuation = continuation
            }
        }
    }

    func resume(with row: (ByteString, ByteString)?) {
        let continuation = state.withLock { state in
            let continuation = state.advanceContinuation
            state.advanceContinuation = nil
            return continuation
        }
        precondition(continuation != nil)
        continuation?.resume(returning: row)
    }
}

private final class ScriptedReadTransaction: TransactionReadAccess, Sendable {
    let transactionDomain = StorageTransactionDomain()
    private let outcome: ScriptedCursorOutcome
    private let cleanupFails: Bool
    private let probe: ScriptedCursorProbe
    private let finishGate: ScriptedFinishGate?

    init(
        outcome: ScriptedCursorOutcome,
        cleanupFails: Bool,
        probe: ScriptedCursorProbe,
        finishGate: ScriptedFinishGate?
    ) {
        self.outcome = outcome
        self.cleanupFails = cleanupFails
        self.probe = probe
        self.finishGate = finishGate
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        nil
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(
            consuming: ScriptedRangeResult(
                outcome: outcome,
                cleanupFails: cleanupFails,
                probe: probe,
                finishGate: finishGate
            )
        )
    }
}

private final class SnapshotRecordingTransaction:
    TransactionReadAccess,
    Sendable {
    private struct State: Sendable {
        var pointSnapshots: [Bool] = []
        var pointMaximums: [Int] = []
        var rangeSnapshots: [Bool] = []
    }

    private let base: any TransactionReadAccess
    private let state = Mutex(State())

    init(base: any TransactionReadAccess) {
        self.base = base
    }

    var transactionDomain: StorageTransactionDomain {
        base.transactionDomain
    }

    var pointSnapshots: [Bool] {
        state.withLock { $0.pointSnapshots }
    }

    var pointMaximums: [Int] {
        state.withLock { $0.pointMaximums }
    }

    var rangeSnapshots: [Bool] {
        state.withLock { $0.rangeSnapshots }
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        state.withLock { $0.pointSnapshots.append(snapshot) }
        return try await base.getValue(for: key, snapshot: snapshot)
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        state.withLock { state in
            state.pointSnapshots.append(snapshot)
            state.pointMaximums.append(maximumByteCount)
        }
        return try await base.getValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await getValue(for: key, snapshot: false)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await base.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        state.withLock { $0.rangeSnapshots.append(snapshot) }
        return base.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}
