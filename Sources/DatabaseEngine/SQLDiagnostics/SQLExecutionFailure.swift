import DatabaseKit

/// A diagnostic and the exact native failure, including mandatory cleanup causes.
public struct SQLExecutionFailure: Error, Sendable {
    public let underlyingError: any Error
    public let diagnostic: SQLDiagnostic

    public init(underlyingError: any Error, diagnostic: SQLDiagnostic) {
        self.underlyingError = underlyingError
        self.diagnostic = diagnostic
    }
}
