import DatabaseKitWasmCore

/// Errors raised by the wire database runtime.
public enum DatabaseRuntimeError: Error, Sendable, Equatable {
    case wire(DatabaseKitWasmWireError)
    case unsupportedKeyValueOperation(DatabaseKitWasmKeyValueOperation)
    case unsupportedPredicateComparison
}
