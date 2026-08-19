import DatabaseKit

/// Errors raised while resolving a registered index maintenance provider.
public enum IndexMaintainerProviderError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    case typeMismatch(registered: IndexType, actual: IndexType)
    case invalidDefinition(indexType: IndexType, reason: String)
    case uniquenessNotSupported(indexType: IndexType)
    case unhandledRuntimeConfiguration(
        indexType: IndexType,
        indexName: String
    )

    public var description: String {
        switch self {
        case .typeMismatch(let registered, let actual):
            return "Index provider registered for '\(registered.diagnosticName)' cannot maintain '\(actual.diagnosticName)'"
        case .invalidDefinition(let indexType, let reason):
            return "Index definition for '\(indexType.diagnosticName)' is invalid: \(reason)"
        case .uniquenessNotSupported(let indexType):
            return "Index provider for '\(indexType.diagnosticName)' does not support uniqueness constraints"
        case .unhandledRuntimeConfiguration(let indexType, let indexName):
            return "Index provider for '\(indexType.diagnosticName)' does not accept runtime configuration for '\(indexName)'"
        }
    }
}
