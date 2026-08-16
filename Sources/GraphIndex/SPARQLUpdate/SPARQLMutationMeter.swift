@_spi(DatabaseExecution) import DatabaseEngine
import Synchronization

final class SPARQLMutationMeter: Sendable {
    private let maximum: Int
    private let workMeter: DatabaseWorkMeter
    private let count = Mutex(0)

    init(maximum: Int, workMeter: DatabaseWorkMeter) {
        self.maximum = maximum
        self.workMeter = workMeter
    }

    func consume() throws {
        try consume(amount: 1)
    }

    func consume(amount: UInt64) throws {
        try workMeter.consume(amount, at: .mutationPlanning)
        guard let amount = Int(exactly: amount) else {
            throw SPARQLUpdateError.mutationLimitExceeded(
                actual: Int.max,
                maximum: maximum
            )
        }
        try count.withLock { count in
            let (next, overflow) = count.addingReportingOverflow(amount)
            guard !overflow, next <= maximum else {
                throw SPARQLUpdateError.mutationLimitExceeded(
                    actual: overflow ? Int.max : next,
                    maximum: maximum
                )
            }
            count = next
        }
    }
}
