/// Preserves both a feature-executor failure and authoritative read-session
/// cleanup failure when escaped cursor cleanup also fails.
public struct DatabaseReadSessionCleanupError: Error {
    public let operationError: any Error
    public let cleanupError: any Error

    public init(operationError: any Error, cleanupError: any Error) {
        self.operationError = operationError
        self.cleanupError = cleanupError
    }
}
