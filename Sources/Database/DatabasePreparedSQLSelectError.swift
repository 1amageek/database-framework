@_spi(DatabaseExecution)
public enum DatabasePreparedSQLSelectError: Error, Sendable, Equatable {
    case workMeterMismatch
}
