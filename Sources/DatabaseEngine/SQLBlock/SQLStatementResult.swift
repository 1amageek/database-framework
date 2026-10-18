import DatabaseKit

/// One caller-owned statement or checkpoint outcome after block closure.
public enum SQLStatementResult: Sendable {
    case rows(QueryResponse)
    case copy(SQLCopyResult)
    case command(SQLCommandKind, affectedRows: UInt64?)
}
