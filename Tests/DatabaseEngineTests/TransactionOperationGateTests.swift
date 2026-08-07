@testable import DatabaseEngine
import Testing

@Suite("Transaction operation gate")
struct TransactionOperationGateTests {
    @Test(
        "A gate rejects overlapping operation admission",
        .timeLimit(.minutes(1))
    )
    func rejectsOverlappingAdmission() throws {
        let gate = TransactionOperationGate()

        try gate.enter()
        #expect(throws: DatabaseTransactionError.concurrentOperation) {
            try gate.enter()
        }
        gate.leave()
    }

    @Test(
        "Closing waits for an admitted operation to leave",
        .timeLimit(.minutes(1))
    )
    func closeWaitsForAdmittedOperation() async throws {
        let gate = TransactionOperationGate()
        try gate.enter()

        let close = Task {
            await gate.closeAndWait()
        }
        let observedClosedAdmission = await waitUntilAdmissionCloses(gate)
        #expect(observedClosedAdmission)

        gate.leave()
        await close.value
        #expect(throws: DatabaseTransactionError.closed) {
            try gate.enter()
        }
    }

    @Test(
        "Leaving resumes every close waiter",
        .timeLimit(.minutes(1))
    )
    func leavingResumesEveryCloseWaiter() async throws {
        let gate = TransactionOperationGate()
        try gate.enter()

        let firstClose = Task {
            await gate.closeAndWait()
        }
        let secondClose = Task {
            await gate.closeAndWait()
        }
        let observedClosedAdmission = await waitUntilAdmissionCloses(gate)
        #expect(observedClosedAdmission)

        gate.leave()
        await firstClose.value
        await secondClose.value
    }

    private func waitUntilAdmissionCloses(
        _ gate: TransactionOperationGate
    ) async -> Bool {
        for _ in 0..<1_000 {
            do {
                try gate.enter()
            } catch DatabaseTransactionError.closed {
                return true
            } catch DatabaseTransactionError.concurrentOperation {
                await Task.yield()
            } catch {
                return false
            }
        }
        return false
    }
}
