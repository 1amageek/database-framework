import DatabaseEngine
import DatabaseKit
import QueryAST

/// A typed SQL binding rejection with an optional exact source byte position.
public struct SQLBindingSourceFailure: Error, Sendable {
    public let cause: CanonicalSQLBindingCause
    public let occurrence: QueryOccurrenceID?
    public let position: Int?

    init(_ failure: CanonicalSQLBindingFailure, sourceMap: SQLSourceMap) {
        self.cause = failure.cause
        self.occurrence = failure.occurrence
        if let occurrence = failure.occurrence,
           let annotation = sourceMap[occurrence] {
            self.position = annotation.identifierRange?.start ?? annotation.range.start
        } else {
            self.position = nil
        }
    }
}
