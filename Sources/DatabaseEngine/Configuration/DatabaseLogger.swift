/// A database-scoped logging endpoint.
///
/// The value owns only database logging behavior. Platform logging values stay
/// inside the application-selected adapter closure.
public struct DatabaseLogger: Sendable {
    public enum Level: Int, Equatable, Sendable {
        case trace
        case debug
        case info
        case notice
        case warning
        case error
        case critical
    }

    private let minimumLevel: Level?
    private let emit: @Sendable (
        Level,
        String,
        [String: String]
    ) -> Void

    public init(
        minimumLevel: Level,
        emit: @escaping @Sendable (
            Level,
            String,
            [String: String]
        ) -> Void
    ) {
        self.minimumLevel = minimumLevel
        self.emit = emit
    }

    private init() {
        self.minimumLevel = nil
        self.emit = { _, _, _ in }
    }

    public static let disabled = DatabaseLogger()

    public func trace(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.trace, message: message, metadata: metadata)
    }

    public func debug(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.debug, message: message, metadata: metadata)
    }

    public func info(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.info, message: message, metadata: metadata)
    }

    public func notice(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.notice, message: message, metadata: metadata)
    }

    public func warning(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.warning, message: message, metadata: metadata)
    }

    public func error(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.error, message: message, metadata: metadata)
    }

    public func critical(
        _ message: @autoclosure () -> String,
        metadata: [String: String] = [:]
    ) {
        log(.critical, message: message, metadata: metadata)
    }

    private func log(
        _ level: Level,
        message: () -> String,
        metadata: [String: String]
    ) {
        guard let minimumLevel, level.rawValue >= minimumLevel.rawValue else {
            return
        }
        emit(level, message(), metadata)
    }
}
