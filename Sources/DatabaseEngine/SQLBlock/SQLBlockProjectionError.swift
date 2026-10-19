/// Failures of the concrete provisional/ready encoding boundary.
@_spi(DatabaseExecution)
public enum SQLBlockProjectionError: Error, Sendable, Equatable {
    case invalidIndex
    case invalidMeasuredSize(UInt64)
    case unexpectedOutputMode
    case alreadyPromoted
}
