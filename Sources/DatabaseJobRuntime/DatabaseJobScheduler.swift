import DatabaseOperationCore
import DatabaseTypes

public protocol DatabaseJobScheduler: Sendable {
    /// Ensures that the next durable wake-up is not later than `timestamp`.
    /// A later request must never postpone an earlier scheduled wake-up.
    func ensureWakeUp(
        noLaterThan timestamp: Timestamp
    ) async throws
}
