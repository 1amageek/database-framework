import DatabaseKit

/// One request-accounted canonical page and the position of its successor.
///
/// The row owner and continuation cross the internal source boundary together,
/// keeping page lifetime and cursor advancement in one explicit contract.
@_spi(DatabaseExecution)
public struct DatabaseRetainedQueryPage: ~Copyable, Sendable {
    private var rows: DatabaseRetainedQueryRows

    package let continuation: QueryContinuation?

    package init(
        rows: consuming DatabaseRetainedQueryRows,
        continuation: QueryContinuation?
    ) {
        self.rows = consume rows
        self.continuation = continuation
    }

    package var count: Int { rows.count }
    package var isEmpty: Bool { rows.isEmpty }
    package var workMeter: DatabaseWorkMeter { rows.workMeter }

    package consuming func takeRows() -> DatabaseRetainedQueryRows {
        consume rows
    }
}
