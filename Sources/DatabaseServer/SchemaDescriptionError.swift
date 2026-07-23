public enum SchemaDescriptionError: Error, Sendable, CustomStringConvertible {
    case invalidFieldNumber(entity: String, field: String, number: Int)
    case indexFieldNotFound(entity: String, index: String, field: String)
    case relationshipMetadataNotFound(entity: String, field: String)

    public var description: String {
        switch self {
        case .invalidFieldNumber(let entity, let field, let number):
            return "Schema field '\(entity).\(field)' has invalid number \(number)"
        case .indexFieldNotFound(let entity, let index, let field):
            return "Index '\(entity).\(index)' references unknown field '\(field)'"
        case .relationshipMetadataNotFound(let entity, let field):
            return "Reference field '\(entity).\(field)' has no relationship metadata"
        }
    }
}
