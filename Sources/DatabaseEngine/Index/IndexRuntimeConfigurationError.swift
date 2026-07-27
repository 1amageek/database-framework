/// Validation failures for deployment-specific index execution policy.
public enum IndexRuntimeConfigurationError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case unknownIndex(indexName: String)
    case indexKindMismatch(
        indexName: String,
        expected: String,
        actual: String
    )
    case duplicateConfiguration(indexName: String)
    case missingRequiredConfiguration(
        indexName: String,
        kindIdentifier: String
    )
    case invalidConfiguration(indexName: String, reason: String)

    public var description: String {
        switch self {
        case .unknownIndex(let indexName):
            "Index runtime configuration references unknown index '\(indexName)'"
        case .indexKindMismatch(let indexName, let expected, let actual):
            "Index '\(indexName)' has kind '\(expected)', but its runtime configuration targets '\(actual)'"
        case .duplicateConfiguration(let indexName):
            "Index '\(indexName)' has more than one exclusive runtime configuration"
        case .missingRequiredConfiguration(let indexName, let kindIdentifier):
            "Index '\(indexName)' of kind '\(kindIdentifier)' requires runtime configuration"
        case .invalidConfiguration(let indexName, let reason):
            "Index '\(indexName)' has invalid runtime configuration: \(reason)"
        }
    }
}
