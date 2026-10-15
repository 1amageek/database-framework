import DatabaseEngine
import DatabaseKit
import QueryAST

/// A typed SQL semantic failure with an optional exact source byte position.
public struct SQLBindingSourceFailure: Error, Sendable {
    public let cause: CanonicalSQLBindingCause
    public let occurrence: QueryOccurrenceID?
    public let position: Int?

    init(_ failure: CanonicalSQLBindingFailure, sourceMap: SQLSourceMap) {
        self.init(cause: failure.cause, occurrence: failure.occurrence, sourceMap: sourceMap)
    }

    init(cause: CanonicalSQLBindingCause, occurrence: QueryOccurrenceID?, sourceMap: SQLSourceMap) {
        self.cause = cause
        self.occurrence = occurrence
        if let occurrence,
           let annotation = sourceMap[occurrence] {
            self.position = annotation.identifierRange?.start ?? annotation.range.start
        } else {
            self.position = nil
        }
    }
}
