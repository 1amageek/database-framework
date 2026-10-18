/// COPY framing failures retain their one-based record and field coordinates.
public enum SQLCopyExecutionError: Error, Sendable, Equatable {
    case malformedInput(record: Int, field: Int)
    case invalidUTF8(record: Int, field: Int)
    case columnCount(record: Int, expected: Int, actual: Int)
    case invalidTarget
    case unknownRetainedInputSize
    case invalidStatementOutcome
}
