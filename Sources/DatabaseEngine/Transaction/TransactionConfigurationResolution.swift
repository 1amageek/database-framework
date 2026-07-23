/// Records how transaction guarantees are enforced by a selected backend.
public struct TransactionConfigurationResolution: Sendable, Hashable {
    /// Advisory settings that the backend cannot enforce directly.
    public enum UnsupportedHint: String, Sendable, Hashable {
        case schedulingPriority
        case readPriority
        case readCacheControl
    }

    /// Whether the backend's own timeout option was configured.
    public let backendTimeoutApplied: Bool

    /// Timeout enforced for the complete transaction execution.
    public let executionTimeoutMilliseconds: Int?

    /// Advisory settings deliberately omitted for this backend.
    public let unsupportedHints: [UnsupportedHint]

    public init(
        backendTimeoutApplied: Bool,
        executionTimeoutMilliseconds: Int?,
        unsupportedHints: [UnsupportedHint]
    ) {
        self.backendTimeoutApplied = backendTimeoutApplied
        self.executionTimeoutMilliseconds = executionTimeoutMilliseconds
        self.unsupportedHints = unsupportedHints
    }
}
