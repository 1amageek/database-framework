import Logging

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

    /// Uses the application's process-wide SwiftLog configuration.
    public static let system = DatabaseLoggingConfiguration { label in
        let logger = Logger(label: label)
        return DatabaseLogger(
            minimumLevel: DatabaseLogger.Level(logger.logLevel),
            emit: { level, message, metadata in
                logger.log(
                    level: Logger.Level(level),
                    "\(message)",
                    metadata: Logger.Metadata(
                        uniqueKeysWithValues: metadata.map {
                            ($0.key, .string($0.value))
                        }
                    )
                )
            }
        )
    }

    /// Discards log events while preserving the logging call contract.
    public static let disabled = DatabaseLoggingConfiguration { _ in
        .disabled
    }
}

private extension DatabaseLogger.Level {
    init(_ level: Logger.Level) {
        switch level {
        case .trace: self = .trace
        case .debug: self = .debug
        case .info: self = .info
        case .notice: self = .notice
        case .warning: self = .warning
        case .error: self = .error
        case .critical: self = .critical
        }
    }
}

private extension Logger.Level {
    init(_ level: DatabaseLogger.Level) {
        switch level {
        case .trace: self = .trace
        case .debug: self = .debug
        case .info: self = .info
        case .notice: self = .notice
        case .warning: self = .warning
        case .error: self = .error
        case .critical: self = .critical
        }
    }
}
