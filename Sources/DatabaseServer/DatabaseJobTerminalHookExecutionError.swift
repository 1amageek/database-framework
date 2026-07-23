public struct DatabaseJobTerminalHookExecutionError: Error, CustomStringConvertible {
    public let underlyingError: any Error

    public init(underlyingError: any Error) {
        self.underlyingError = underlyingError
    }

    public var description: String {
        "Persistent job terminal hook failed: \(underlyingError)"
    }
}
