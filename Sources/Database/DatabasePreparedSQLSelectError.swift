@_spi(DatabaseExecution)
public enum DatabasePreparedSQLSelectError: Error, Sendable, Equatable {
    case workMeterMismatch
    /// Complete staging produced a continuation, so the staged rows are a
    /// prefix rather than the complete result. Publishing them as a durable
    /// snapshot would silently drop the remainder.
    case stagedResultIsIncomplete
}
