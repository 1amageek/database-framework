import DatabaseTypes

public enum RelationshipError: Error, Sendable, CustomStringConvertible {
    case deleteRuleDenied(
        itemType: String,
        relationshipType: String,
        propertyName: String,
        count: Int
    )
    case mutationLimitExceeded(actual: Int, maximum: Int)
    case workLimitExceeded(maximum: UInt64)
    case catalogOwnerMissing(EntityReference)

    public var description: String {
        switch self {
        case .deleteRuleDenied(
            let itemType,
            let relationshipType,
            let propertyName,
            let count
        ):
            return "Cannot delete \(itemType): \(count) \(relationshipType) item(s) reference it via '\(propertyName)' with deleteRule=.deny"
        case .mutationLimitExceeded(let actual, let maximum):
            return "Relationship delete expands to \(actual) mutations, exceeding \(maximum)"
        case .workLimitExceeded(let maximum):
            return "Relationship delete exceeded the work limit of \(maximum)"
        case .catalogOwnerMissing(let identity):
            return "Relationship catalog references missing owner '\(identity)'"
        }
    }
}
