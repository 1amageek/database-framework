import DatabaseKit

/// A semantic binding failure addressed in the caller's occurrence arena.
public struct CanonicalSQLBindingFailure: Error, Sendable {
    public let cause: CanonicalSQLBindingCause
    public let occurrence: QueryOccurrenceID?
    public let root: QueryOccurrenceID?

    public init(cause: CanonicalSQLBindingCause, occurrence: QueryOccurrenceID?, root: QueryOccurrenceID?) {
        self.cause = cause
        self.occurrence = occurrence
        self.root = root
    }
}
