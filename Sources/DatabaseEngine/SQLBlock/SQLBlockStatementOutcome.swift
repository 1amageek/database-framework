import DatabaseKit

/// Private aggregate input retained until authoritative transaction closure.
enum SQLBlockStatementOutcome: Sendable {
    case rows(SQLBlockCollectedRows)
    case command(SQLCommandKind, UInt64?)
    case copy(SQLCopyResult, DatabaseIntermediateReservation?)
}
