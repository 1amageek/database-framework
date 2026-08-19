import DatabaseKit

/// Validation failures for deployment-specific index execution policy.
public enum IndexRuntimeConfigurationError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case unknownIndex(indexName: String)
    case indexTypeMismatch(
        indexName: String,
        expected: IndexType,
        actual: IndexType
    )
    case duplicateConfiguration(indexName: String
    )
    case invalidConfiguration(indexName: String, reason: String)
    case providerRejected(
        indexName: String,
        indexType: IndexType,
        reason: String
    )
    case inconsistentPhysicalLayout(indexName: String)

    public var description: String {
        switch self {
        case .unknownIndex(let indexName):
            "Index runtime configuration references unknown index '\(indexName)'"
        case .indexTypeMismatch(let indexName, let expected, let actual):
            "Index '\(indexName)' has type '\(expected.diagnosticName)', but its runtime configuration targets '\(actual.diagnosticName)'"
        case .duplicateConfiguration(let indexName):
            "Index '\(indexName)' has more than one exclusive runtime configuration"
        case .invalidConfiguration(let indexName, let reason):
            "Index '\(indexName)' has invalid runtime configuration: \(reason)"
        case .providerRejected(let indexName, let indexType, let reason):
            "Index '\(indexName)' provider for '\(indexType.diagnosticName)' rejected its runtime configuration: \(reason)"
        case .inconsistentPhysicalLayout(let indexName):
            "Polymorphic index '\(indexName)' resolved more than one physical layout"
        }
    }
}
