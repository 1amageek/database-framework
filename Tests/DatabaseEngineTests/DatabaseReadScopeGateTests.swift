import DatabaseTypes
import StorageKit
import Synchronization
import Testing
@testable import DatabaseEngine

private actor ReadScopeCloseObservation {
    private var closed = false
    private var cursorFinishCount = 0

    func markClosed() { closed = true }
    func isClosed() -> Bool { closed }
    func recordCursorFinish() { cursorFinishCount += 1 }
    func recordedCursorFinishCount() -> Int { cursorFinishCount }
}

private struct ReadScopeObservedRangeResult: TransactionRangeResult {
    let observation: ReadScopeCloseObservation

    func makeCursor() -> Cursor {
        Cursor(observation: observation)
    }

    struct Cursor: TransactionRangeCursor {
        let observation: ReadScopeCloseObservation
        private var yielded = false

        mutating func next() async throws -> (ByteString, ByteString)? {
            guard !yielded else { return nil }
            yielded = true
            return (ByteString([0x01]), ByteString([0x02]))
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            await observation.recordCursorFinish()
        }
    }
}

private final class ReadScopeResultReleaseProbe: Sendable {
    private let releaseCountStorage = Mutex(0)

    var releaseCount: Int { releaseCountStorage.withLock { $0 } }

    func recordRelease() {
        releaseCountStorage.withLock { $0 += 1 }
    }
}

private final class ReadScopeResultOwner: Sendable {
    private let probe: ReadScopeResultReleaseProbe

    init(probe: ReadScopeResultReleaseProbe) {
        self.probe = probe
    }

    deinit { probe.recordRelease() }
}

private struct ReadScopeUnopenedRangeResult: TransactionRangeResult {
    let owner: ReadScopeResultOwner

    func makeCursor() -> Cursor { Cursor() }

    struct Cursor: TransactionRangeCursor {
        mutating func next() async throws -> (ByteString, ByteString)? { nil }
        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {}
    }
}

private actor ReadScopeCursorSuspension {
    private var started = false
    private var released = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func suspendAfterStarting() async {
        started = true
        let pendingStartWaiters = startWaiters
        startWaiters.removeAll(keepingCapacity: false)
        for waiter in pendingStartWaiters { waiter.resume() }
        guard !released else { return }
        await withCheckedContinuation { continuation in
            releaseWaiters.append(continuation)
        }
    }

    func waitUntilStarted() async {
        guard !started else { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        released = true
        let pendingReleaseWaiters = releaseWaiters
        releaseWaiters.removeAll(keepingCapacity: false)
        for waiter in pendingReleaseWaiters { waiter.resume() }
    }
}

private struct ReadScopeSuspendingRangeResult: TransactionRangeResult {
    let observation: ReadScopeCloseObservation
    let suspension: ReadScopeCursorSuspension

    func makeCursor() -> Cursor {
        Cursor(observation: observation, suspension: suspension)
    }

    struct Cursor: TransactionRangeCursor {
        let observation: ReadScopeCloseObservation
        let suspension: ReadScopeCursorSuspension
        private var yielded = false

        mutating func next() async throws -> (ByteString, ByteString)? {
            guard !yielded else { return nil }
            yielded = true
            await suspension.suspendAfterStarting()
            return (ByteString([0x03]), ByteString([0x04]))
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            await observation.recordCursorFinish()
        }
    }
}

private enum ReadScopeFinishFailure: Error, Equatable {
    case expected
}

private struct ReadScopeFailingFinishRangeResult: TransactionRangeResult {
    func makeCursor() -> Cursor { Cursor() }

    struct Cursor: TransactionRangeCursor {
        private var yielded = false

        mutating func next() async throws -> (ByteString, ByteString)? {
            guard !yielded else { return nil }
            yielded = true
            return (ByteString([0x05]), ByteString([0x06]))
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            throw ReadScopeFinishFailure.expected
        }
    }
}

@Suite("Database read scope gate")
struct DatabaseReadScopeGateTests {
    @Test(
        "Close rejects new reads and drains an admitted read",
        .timeLimit(.minutes(1))
    )
    func closeDrainsAdmittedRead() async throws {
        let gate = DatabaseReadScopeGate()
        let lease = try gate.beginRead()
        let observation = ReadScopeCloseObservation()
        let closeTask = Task {
            try await gate.closeAndWait()
            await observation.markClosed()
        }

        await Task.yield()
        #expect(!(await observation.isClosed()))
        lease.finish()
        try await closeTask.value

        #expect(await observation.isClosed())
        #expect(throws: DatabaseReadTransactionError.snapshotClosed) {
            _ = try gate.beginRead()
        }
    }

    @Test(
        "Close authoritatively finishes a partially consumed cursor",
        .timeLimit(.minutes(1))
    )
    func closeFinishesPartiallyConsumedCursor() async throws {
        let gate = DatabaseReadScopeGate()
        let observation = ReadScopeCloseObservation()
        let admission = try gate.beginRead()
        var cursor = try gate.scope(
            KeyValueCursor(
                consuming: ReadScopeObservedRangeResult(
                    observation: observation
                )
            ),
            admittedBy: admission
        )
        admission.finish()

        #expect(try await cursor.next() != nil)
        try await gate.closeAndWait()

        #expect(await observation.recordedCursorFinishCount() == 1)
        await #expect(
            throws: DatabaseReadTransactionError.snapshotClosed
        ) {
            _ = try await cursor.next()
        }
    }

    @Test(
        "Close drains a cursor registered by an admitted read",
        .timeLimit(.minutes(1))
    )
    func closeDrainsCursorRegisteredByAdmittedRead() async throws {
        let gate = DatabaseReadScopeGate()
        let admission = try gate.beginRead()
        let releaseProbe = ReadScopeResultReleaseProbe()
        let closeTask = Task { try await gate.closeAndWait() }

        while true {
            do {
                let probe = try gate.beginRead()
                probe.finish()
                await Task.yield()
            } catch DatabaseReadTransactionError.snapshotClosed {
                break
            }
        }

        var cursor = try gate.scope(
            KeyValueCursor(
                consuming: ReadScopeUnopenedRangeResult(
                    owner: ReadScopeResultOwner(probe: releaseProbe)
                )
            ),
            admittedBy: admission
        )
        admission.finish()
        try await closeTask.value

        #expect(releaseProbe.releaseCount == 1)
        await #expect(
            throws: DatabaseReadTransactionError.snapshotClosed
        ) {
            _ = try await cursor.next()
        }
    }

    @Test("Close preserves cursor cleanup failures")
    func closePreservesCursorCleanupFailure() async throws {
        let gate = DatabaseReadScopeGate()
        let admission = try gate.beginRead()
        var cursor = try gate.scope(
            KeyValueCursor(consuming: ReadScopeFailingFinishRangeResult()),
            admittedBy: admission
        )
        admission.finish()
        #expect(try await cursor.next() != nil)

        for _ in 0..<2 {
            do {
                try await gate.closeAndWait()
                Issue.record("Expected the cursor cleanup failure")
            } catch let error as DatabaseReadScopeCleanupError {
                #expect(error.operationError == nil)
                #expect(error.cursorCleanupErrors.count == 1)
                #expect(
                    error.cursorCleanupErrors.first as? ReadScopeFinishFailure
                        == .expected
                )
            }
        }
    }

    @Test("A released admission cannot register a cursor")
    func releasedAdmissionCannotRegisterCursor() throws {
        let gate = DatabaseReadScopeGate()
        let admission = try gate.beginRead()
        let observation = ReadScopeCloseObservation()
        admission.finish()

        #expect(throws: DatabaseReadTransactionError.snapshotClosed) {
            _ = try gate.scope(
                KeyValueCursor(
                    consuming: ReadScopeObservedRangeResult(
                        observation: observation
                    )
                ),
                admittedBy: admission
            )
        }
    }

    @Test(
        "Concurrent cursor aliases retain registration through cleanup",
        .timeLimit(.minutes(1))
    )
    func concurrentAliasesRetainRegistrationThroughCleanup() async throws {
        let gate = DatabaseReadScopeGate()
        let observation = ReadScopeCloseObservation()
        let suspension = ReadScopeCursorSuspension()
        let admission = try gate.beginRead()
        var first = try gate.scope(
            KeyValueCursor(
                consuming: ReadScopeSuspendingRangeResult(
                    observation: observation,
                    suspension: suspension
                )
            ),
            admittedBy: admission
        )
        admission.finish()
        var alias = first

        let admittedAdvance = Task { try await first.next() }
        await suspension.waitUntilStarted()
        let rejectedAdvance = Task { try await alias.next() }
        for _ in 0..<20 { await Task.yield() }

        await suspension.release()
        #expect(try await admittedAdvance.value != nil)
        do {
            _ = try await rejectedAdvance.value
            Issue.record("Concurrent cursor advance must be rejected")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        try await gate.closeAndWait()
        #expect(await observation.recordedCursorFinishCount() == 1)
    }
}
