import GraphIndex
import Testing

@Suite("Graph algorithm work budget")
struct GraphAlgorithmWorkBudgetTests {
    @Test("the exact boundary is sticky and never oversubscribes")
    func exactBoundaryIsSticky() throws {
        let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 3)

        #expect(try budget.consume(2))
        #expect(!(try budget.consume(2)))
        #expect(!(try budget.consume(1)))
        #expect(budget.consumedWorkUnits == 2)
        #expect(
            budget.limitReason == .maxWorkUnitsReached(consumed: 2, limit: 3)
        )
    }

    @Test("concurrent consumption never exceeds the maximum")
    func concurrentConsumptionNeverExceedsMaximum() async throws {
        let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 100)
        let successfulConsumptions = try await withThrowingTaskGroup(
            of: Int.self,
            returning: Int.self
        ) { group in
            for _ in 0..<20 {
                group.addTask {
                    var successful = 0
                    for _ in 0..<20 where try budget.consume() {
                        successful += 1
                    }
                    return successful
                }
            }
            var total = 0
            for try await successful in group {
                total += successful
            }
            return total
        }

        #expect(successfulConsumptions == 100)
        #expect(budget.consumedWorkUnits == 100)
        #expect(budget.limitReason == .maxWorkUnitsReached(consumed: 100, limit: 100))
    }

    @Test("cancelled consumption does not mutate state")
    func cancelledConsumptionDoesNotMutateState() async {
        let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 1)
        let task = Task { try budget.consume() }
        task.cancel()

        await #expect(throws: CancellationError.self) {
            try await task.value
        }
        #expect(budget.consumedWorkUnits == 0)
        #expect(budget.limitReason == nil)
    }
}
