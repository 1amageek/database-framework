import DatabaseEngine
import Logging

public extension DatabaseLoggingConfiguration {
    /// Routes database log events through the application's SwiftLog backend.
    static let swiftLog = DatabaseLoggingConfiguration { label in
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
