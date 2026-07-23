/// A portable transaction deadline expired before commit dispatch began.
///
/// This is distinct from a backend timeout. It is deterministic,
/// non-retryable, and preserves which execution policy supplied the deadline.
public struct TransactionExecutionDeadlineExceeded:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    public enum Source: Sendable, Equatable {
        case transactionConfiguration
        case inheritedOperation
    }

    public let timeoutMilliseconds: UInt64
    public let source: Source

    public init(
        timeoutMilliseconds: UInt64,
        source: Source
    ) {
        self.timeoutMilliseconds = timeoutMilliseconds
        self.source = source
    }

    public var description: String {
        "Transaction exceeded its \(timeoutMilliseconds) ms portable deadline"
    }
}
