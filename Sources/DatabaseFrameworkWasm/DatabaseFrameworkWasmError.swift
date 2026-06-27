import DatabaseKitWasmCore

/// Errors raised by the WASM database runtime.
public enum DatabaseFrameworkWasmError: Error, Sendable, Equatable {
    case wire(DatabaseKitWasmWireError)
    case unsupportedKeyValueOperation(DatabaseKitWasmKeyValueOperation)
    case unsupportedPredicateComparison
}
