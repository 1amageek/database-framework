@testable import DatabaseEngine
import Testing

@Suite("Database transaction scope")
struct DatabaseTransactionScopeTests {
    @Test(
        "A scope rejects overlapping operation admission",
        .timeLimit(.minutes(1))
    )
    func rejectsOverlappingAdmission() throws {
        let scope = DatabaseTransactionScope()

        try scope.enter()
        #expect(throws: DatabaseTransactionError.concurrentOperation) {
            try scope.enter()
        }
        scope.leave()
    }

    @Test(
        "Closing waits for an admitted operation to leave",
        .timeLimit(.minutes(1))
    )
    func closeWaitsForAdmittedOperation() async throws {
        let scope = DatabaseTransactionScope()
        try scope.enter()

        let close = Task {
            await scope.closeAndWait()
        }
        let observedClosedAdmission = await waitUntilAdmissionCloses(scope)
        #expect(observedClosedAdmission)

        scope.leave()
        await close.value
        #expect(throws: DatabaseTransactionError.closed) {
            try scope.enter()
        }
    }

    @Test(
        "Leaving resumes every close waiter",
        .timeLimit(.minutes(1))
    )
    func leavingResumesEveryCloseWaiter() async throws {
        let scope = DatabaseTransactionScope()
        try scope.enter()

        let firstClose = Task {
            await scope.closeAndWait()
        }
        let secondClose = Task {
            await scope.closeAndWait()
        }
        let observedClosedAdmission = await waitUntilAdmissionCloses(scope)
        #expect(observedClosedAdmission)

        scope.leave()
        await firstClose.value
        await secondClose.value
    }

    private func waitUntilAdmissionCloses(
        _ scope: DatabaseTransactionScope
    ) async -> Bool {
        for _ in 0..<1_000 {
            do {
                try scope.enter()
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
