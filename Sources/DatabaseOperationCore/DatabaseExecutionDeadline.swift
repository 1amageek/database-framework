import DatabaseEngine
import StorageKit

public struct DatabaseExecutionDeadline: Sendable {
    public let timeoutMilliseconds: UInt32

    private let deadline: StorageInstant
    private let clock: any StorageMonotonicClock

    public init(
        timeoutMilliseconds: UInt32,
        clock: any StorageMonotonicClock
    ) {
        self.timeoutMilliseconds = timeoutMilliseconds
        self.clock = clock
        self.deadline = clock.now.advanced(
            by: .milliseconds(Int64(timeoutMilliseconds))
        )
    }

    public func run<Value: Sendable>(
        _ operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        try await DatabaseExecutionTimeout.run(
            until: deadline,
            timeoutMilliseconds: timeoutMilliseconds,
            clock: clock,
            operation: operation
        )
    }

    package var transactionExecutionDeadline: TransactionExecutionDeadline {
        TransactionExecutionDeadline(
            instant: deadline,
            timeoutMilliseconds: timeoutMilliseconds
        )
    }
}
