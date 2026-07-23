/// An absolute transaction deadline inherited from a larger operation.
///
/// `TransactionConfiguration.timeout` remains the portable relative timeout
/// applied to a newly-created backend transaction. This value preserves an
/// earlier absolute boundary across preparation and other composed work.
public struct TransactionExecutionDeadline: Sendable {
    public let instant: ContinuousClock.Instant
    public let timeoutMilliseconds: UInt32

    public init(
        instant: ContinuousClock.Instant,
        timeoutMilliseconds: UInt32
    ) {
        self.instant = instant
        self.timeoutMilliseconds = timeoutMilliseconds
    }
}
