/// The original typed error retained by occurrence-addressed SQL binding.
public enum CanonicalSQLBindingCause: Sendable {
    case expression(DatabaseExpressionEvaluationError)
    case read(CanonicalReadError)
}
