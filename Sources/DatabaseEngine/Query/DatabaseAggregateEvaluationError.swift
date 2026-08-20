public enum DatabaseAggregateEvaluationError: Error, Sendable, Equatable {
    case invalidGroupedExpression(String)
    case incompatibleNumericKinds(function: String)
    case nonNumericValue(function: String)
    case nonFiniteValue(function: String)
    case numericOverflow(function: String)
    case resultNotRepresentable(function: String)
    case incomparable(function: String, left: String, right: String)
    case invalidStringValue(function: String)
    case countOverflow
}
