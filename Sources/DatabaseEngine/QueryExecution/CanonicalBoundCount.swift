/// An invocation count preserves the full fixed non-SQL unsigned domain.
enum CanonicalBoundCount: Sendable {
    case fixed(UInt64)
    case expression(CanonicalBoundExpression)
}
