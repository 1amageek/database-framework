#if !os(WASI)
import StorageKit
import Synchronization
import TestSupport
import Testing

/// Verifies the single condition the serialized scenario gate releases on:
///
///     quiescent iff admission is closed and no admitted operation is running
///
/// Each test drives the ledger directly, because the condition is the ledger's
/// own and holds for every backend the scenario engine decorates.
@Suite("Scenario admission quiescence", .timeLimit(.minutes(1)))
struct ScenarioAdmissionQuiescenceTests {
    /// Yields often enough that a wait which was going to return has run.
    ///
    /// The assertions below never depend on this bound being large enough: a
    /// ledger that keeps its promise cannot return early however long the
    /// waiter is left to run. The bound only decides how reliably a ledger
    /// that breaks the promise is caught.
    private static let schedulingOpportunities = 200

    private static func yieldRepeatedly() async {
        for _ in 0..<schedulingOpportunities {
            await Task.yield()
        }
    }

    @Test("A cancelled waiter still holds the gate until the work ends")
    func cancellationDoesNotReleaseTheGate() async throws {
        let ledger = ScenarioAdmissionLedger(backend: .inMemory)
        try ledger.begin(.read)
        ledger.close()

        let workHasEnded = Mutex(false)
        let waiter = Task {
            _ = await ledger.waitUntilQuiescent()
            return workHasEnded.withLock { $0 }
        }
        waiter.cancel()
        await Self.yieldRepeatedly()

        workHasEnded.withLock { $0 = true }
        ledger.end(.read)

        #expect(
            await waiter.value,
            "The wait returned before the admitted read ended"
        )
    }

    @Test("An admitted operation holds the gate until it ends")
    func admittedWorkHoldsTheGate() async throws {
        let ledger = ScenarioAdmissionLedger(backend: .inMemory)
        try ledger.begin(.commit)
        ledger.close()

        let workHasEnded = Mutex(false)
        let waiter = Task {
            _ = await ledger.waitUntilQuiescent()
            return workHasEnded.withLock { $0 }
        }
        await Self.yieldRepeatedly()

        workHasEnded.withLock { $0 = true }
        ledger.end(.commit)

        #expect(
            await waiter.value,
            "The wait returned before the admitted commit ended"
        )
    }

    @Test("An open ledger holds the gate even with nothing running")
    func openAdmissionHoldsTheGate() async {
        let ledger = ScenarioAdmissionLedger(backend: .inMemory)
        #expect(ledger.inFlightCount == 0)

        let admissionIsClosed = Mutex(false)
        let waiter = Task {
            _ = await ledger.waitUntilQuiescent()
            return admissionIsClosed.withLock { $0 }
        }
        await Self.yieldRepeatedly()

        admissionIsClosed.withLock { $0 = true }
        ledger.close()

        #expect(
            await waiter.value,
            "The wait returned before admission closed"
        )
    }

    @Test("A sealed, idle ledger is quiescent at once")
    func sealedIdleLedgerIsQuiescent() async {
        let ledger = ScenarioAdmissionLedger(backend: .inMemory)
        ledger.close()

        let report = await ledger.waitUntilQuiescent()

        #expect(report.isTerminal)
        #expect(report.rejectedOperationCount == 0)
    }

    /// A refused operation never ran, so it cannot hold the gate. It is
    /// reported instead, because issuing it means the scenario reached the
    /// service after sealing itself.
    @Test("A refusal is reported without holding the gate")
    func refusalIsReportedWithoutHoldingTheGate() async {
        let ledger = ScenarioAdmissionLedger(backend: .inMemory)
        ledger.close()

        #expect(throws: StorageError.self) {
            try ledger.requireOpen(.beginTransaction)
        }
        #expect(throws: StorageError.self) {
            try ledger.begin(.read)
        }
        #expect(ledger.inFlightCount == 0)

        let report = await ledger.waitUntilQuiescent()

        #expect(!report.isTerminal)
        #expect(report.rejectedOperationCount == 2)
        #expect(report.rejectedOperations[.beginTransaction] == 1)
        #expect(report.rejectedOperations[.read] == 1)
    }

    @Test("Work admitted before sealing runs to its end")
    func admittedWorkCompletesAfterSealing() async throws {
        let ledger = ScenarioAdmissionLedger(backend: .inMemory)
        let completed = Mutex(false)
        let work = Task {
            try await ledger.withAdmission(.rangeRead) {
                await Self.yieldRepeatedly()
                completed.withLock { $0 = true }
            }
        }
        while ledger.inFlightCount == 0 {
            await Task.yield()
        }
        ledger.close()

        let report = await ledger.waitUntilQuiescent()
        try await work.value

        #expect(completed.withLock { $0 })
        #expect(report.isTerminal)
    }
}
#endif
