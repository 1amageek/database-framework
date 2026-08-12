import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseEngine
import Synchronization

package final class SPARQLMutationMeter: Sendable {
    private let maximum: Int
    private let workMeter: DatabaseWorkMeter
    private let count = Mutex(0)

    package init(maximum: Int, workMeter: DatabaseWorkMeter) {
        self.maximum = maximum
        self.workMeter = workMeter
    }

    package func consume() throws {
        try consume(amount: 1)
    }

    package func consume(amount: UInt64) throws {
        try workMeter.consume(amount, at: .mutationPlanning)
        guard let amount = Int(exactly: amount) else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: Int.max,
                maximum: maximum
            )
        }
        try count.withLock { count in
            let (next, overflow) = count.addingReportingOverflow(amount)
            guard !overflow, next <= maximum else {
                throw DatabaseMutationError.mutationLimitExceeded(
                    actual: overflow ? Int.max : next,
                    maximum: maximum
                )
            }
            count = next
        }
    }
}

#endif
