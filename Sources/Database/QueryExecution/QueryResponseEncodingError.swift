@_spi(DatabaseExecution) import DatabaseWire

/// Preserves a bounded response preparation failure before publication.
public enum QueryResponseEncodingError: Error, Sendable {
    case resourceLimit(DatabaseWireError)
}
