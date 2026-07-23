public struct SPARQLOrderKeyPlan: Sendable, Hashable {
    public let expression: SPARQLExpressionPlan
    public let ascending: Bool
    public let nullsLast: Bool

    package init(
        expression: SPARQLExpressionPlan,
        ascending: Bool,
        nullsLast: Bool
    ) {
        self.expression = expression
        self.ascending = ascending
        self.nullsLast = nullsLast
    }
}
