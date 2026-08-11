public enum DatabaseRuntimeConfigurationError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    public enum Limit: String, Sendable, Equatable, CaseIterable {
        case maximumRows
        case maximumWorkUnits
        case maximumIntermediateRows
        case maximumIntermediateBytes
        case maximumTimeoutMilliseconds
        case maximumMutations
        case maximumPreconditions
        case maximumIdempotencyKeyBytes
        case maximumMutationAggregateBytes
        case maximumNestingDepth
        case maximumInputTokens
        case maximumTotalNodes
        case maximumCollectionElements
        case maximumBasicGraphPatterns
        case maximumTriplePatterns
        case maximumValuesRows
        case maximumValuesVariables
        case maximumValuesCells
        case maximumReifiedTripleExpansions
    }

    case nonPositiveLimit(Limit)
    case unsupportedOnCurrentPlatform(
        limit: Limit,
        actual: UInt64,
        maximum: UInt64
    )
    public var description: String {
        switch self {
        case .nonPositiveLimit(let limit):
            return "Database runtime limit '\(limit.rawValue)' must be positive"
        case .unsupportedOnCurrentPlatform(let limit, let actual, let maximum):
            return "Database runtime limit '\(limit.rawValue)' is \(actual), exceeding the current platform maximum of \(maximum)"
        }
    }
}
