import StorageKit
import Synchronization

/// One statement race, retaining failure and cleanup until both tasks finish.
struct SQLBlockStatementDeadline: Sendable {
    private final class Selection: Sendable {
        private let chosen = Mutex(false)

        func claim() -> Bool {
            chosen.withLock { chosen in
                guard !chosen else { return false }
                chosen = true
                return true
            }
        }
    }

    private enum Outcome<Value: Sendable>: Sendable {
        case value(Value)
        case failure(any Error)
        case timedOut(StorageTransactionCleanupError?)
        case lateFailure(any Error)
        case cancelled
    }

    static func run<Value: Sendable>(milliseconds: UInt64, clock: any StorageMonotonicClock,
        deadline: StorageInstant? = nil,
        operation: @escaping @Sendable () async throws -> Value) async throws -> Value {
        guard milliseconds != 0 else { return try await operation() }
        let instant = deadline ?? clock.now.advanced(by: .milliseconds(Int64(milliseconds)))
        guard clock.now < instant else { throw SQLBlockExecutionError.statementTimeout }
        let winner = Selection()
        let cancel = ActiveTransactionCancellation.request
        return try await withThrowingTaskGroup(of: Outcome<Value>.self, returning: Value.self) { group in
            group.addTask {
                let outcome: Outcome<Value>
                do { outcome = .value(try await operation()) }
                catch { outcome = .failure(error) }
                let selected = winner.claim()
                if selected { return outcome }
                if case .failure(let error) = outcome { return .lateFailure(error) }
                return .cancelled
            }
            group.addTask {
                do { try await clock.sleep(until: instant) }
                catch StorageClockError.cancelled { return .cancelled }
                catch {
                    let selected = winner.claim()
                    guard selected else { return .cancelled }
                    if let cleanup = await cancel?(error) { return .failure(cleanup) }
                    return .failure(error)
                }
                let selected = winner.claim()
                guard selected else { return .cancelled }
                let failure = SQLBlockExecutionError.statementTimeout
                return .timedOut(await cancel?(failure))
            }
            while let outcome = try await group.next() {
                switch outcome {
                case .value(let value): group.cancelAll(); return value
                case .failure(let error): group.cancelAll(); throw error
                case .timedOut(let cleanup):
                    group.cancelAll()
                    var failure: any Error = cleanup.map { $0 as any Error } ?? SQLBlockExecutionError.statementTimeout
                    while let joined = try await group.next() {
                        if case .lateFailure(let error) = joined,
                           error is SQLRelationCleanupError || error is DatabaseReadSessionCleanupError
                            || error is StorageTransactionCleanupError {
                            failure = SQLRelationCleanupError(operationError: failure, cleanupError: error)
                        }
                    }
                    throw failure
                case .lateFailure, .cancelled: continue
                }
            }
            throw CancellationError()
        }
    }
}
