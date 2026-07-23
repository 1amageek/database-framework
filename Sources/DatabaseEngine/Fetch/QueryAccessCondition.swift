import Core

/// A predicate clause consumed by the selected storage access path.
public struct QueryAccessCondition: Sendable, Equatable, Hashable {
    public let fieldName: String
    public let comparison: ComparisonOperator
    public let value: FieldValue

    public init(
        fieldName: String,
        comparison: ComparisonOperator,
        value: FieldValue
    ) {
        self.fieldName = fieldName
        self.comparison = comparison
        self.value = value
    }
}
