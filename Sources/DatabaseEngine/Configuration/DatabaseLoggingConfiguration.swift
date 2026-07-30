/// Container-scoped construction policy for database loggers.
///
/// Applications select the operational logging destination without changing
/// database execution semantics or relying on process-global logger bootstrap.
public struct DatabaseLoggingConfiguration: Sendable {
    private let createLogger: @Sendable (String) -> DatabaseLogger

    /// Creates a logging configuration from an application-owned logger
    /// construction closure.
    public init(
        createLogger: @escaping @Sendable (String) -> DatabaseLogger
    ) {
        self.createLogger = createLogger
    }

    /// Returns the logger for one database subsystem.
    public func logger(label: String) -> DatabaseLogger {
        createLogger(label)
    }

    /// Discards log events while preserving the logging call contract.
    public static let disabled = DatabaseLoggingConfiguration { _ in
        .disabled
    }
}
