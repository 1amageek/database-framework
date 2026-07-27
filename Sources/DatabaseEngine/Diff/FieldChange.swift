import DatabaseTypes

/// One canonical field transition produced by model diff execution.
public struct FieldChange: Sendable, Hashable {
    public let fieldPath: String
    public let oldValue: FieldValue
    public let newValue: FieldValue
    public let changeType: ChangeType

    public init(
        fieldPath: String,
        oldValue: FieldValue,
        newValue: FieldValue,
        changeTypeOverride: ChangeType? = nil
    ) {
        self.fieldPath = fieldPath
        self.oldValue = oldValue
        self.newValue = newValue
        self.changeType = changeTypeOverride
            ?? Self.transition(from: oldValue, to: newValue)
    }

    private static func transition(
        from oldValue: FieldValue,
        to newValue: FieldValue
    ) -> ChangeType {
        if oldValue == newValue {
            return .unchanged
        }
        if case .null = oldValue {
            return .added
        }
        if case .null = newValue {
            return .removed
        }
        return .modified
    }
}
