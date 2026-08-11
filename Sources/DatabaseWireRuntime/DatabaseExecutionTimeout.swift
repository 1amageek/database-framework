import StorageKit

public enum DatabaseExecutionTimeout {
    public static func run<Value: Sendable>(
        milliseconds: UInt32,
        clock: any StorageMonotonicClock,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        let deadline = clock.now.advanced(
            by: .milliseconds(Int64(milliseconds))
        )
        return try await run(
            until: deadline,
            timeoutMilliseconds: milliseconds,
            clock: clock,
            operation: operation
        )
    }

    static func run<Value: Sendable>(
        until deadline: StorageInstant,
        timeoutMilliseconds: UInt32,
        clock: any StorageMonotonicClock,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        guard clock.now < deadline else {
            throw DatabaseRuntimeLimitError.executionTimedOut(
                timeoutMilliseconds
            )
        }
        let outcome = await withTaskGroup(
            of: DatabaseExecutionTimeoutOutcome<Value>.self,
            returning: DatabaseExecutionTimeoutOutcome<Value>.self
        ) { group in
            group.addTask {
                do {
                    return .value(try await operation())
                } catch is CancellationError {
                    return .cancelled
                } catch {
                    return .failure(error)
                }
            }
            group.addTask {
                do {
                    try await clock.sleep(until: deadline)
                    return .timedOut
                } catch is CancellationError {
                    return .cancelled
                } catch {
                    return .failure(error)
                }
            }
            let first = await group.next() ?? .timedOut
            group.cancelAll()
            while await group.next() != nil {}
            return first
        }
        switch outcome {
        case .value(let value):
            return value
        case .failure(let error):
            throw error
        case .timedOut:
            throw DatabaseRuntimeLimitError.executionTimedOut(
                timeoutMilliseconds
            )
        case .cancelled:
            throw CancellationError()
        }
    }
}
