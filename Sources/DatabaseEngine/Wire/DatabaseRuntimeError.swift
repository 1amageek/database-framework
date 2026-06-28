import DatabaseWire

/// Errors raised by the wire database runtime.
public enum DatabaseRuntimeError: Error, Sendable, Equatable {
    case wire(DatabaseWireError)
    case unsupportedKeyValueOperation(DatabaseWireKeyValueOperation)
    case unsupportedPredicateComparison
    case invalidVectorQuery(String)
    case storageFailure(String)
    case invalidStorageResponse(String)
}
