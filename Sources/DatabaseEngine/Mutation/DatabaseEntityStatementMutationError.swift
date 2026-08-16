@_spi(DatabaseExecution)
public enum DatabaseEntityStatementMutationError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case unsupportedStatement(String)
    case scanLimitUnsupportedOnCurrentPlatform(maximum: UInt32)
    case scanLimitExceeded(actual: Int, maximum: UInt32)

    public var description: String {
        switch self {
        case .unsupportedStatement(let reason):
            return "Entity statement mutation is unsupported: \(reason)"
        case .scanLimitUnsupportedOnCurrentPlatform(let maximum):
            return "Entity statement scan limit \(maximum) is unsupported on this platform"
        case .scanLimitExceeded(let actual, let maximum):
            return "Entity statement scan produced \(actual) rows, exceeding the limit of \(maximum)"
        }
    }
}
