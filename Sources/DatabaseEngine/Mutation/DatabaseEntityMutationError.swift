import DatabaseTypes

@_spi(DatabaseExecution)
public enum DatabaseEntityMutationError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case emptyMutation
    case changeLimitExceeded(actual: Int, maximum: Int)
    case preconditionLimitExceeded(actual: Int, maximum: Int)
    case unknownEntity(String)
    case entityHasNoPersistableType(String)
    case invalidPersistableIdentifier(entity: String, reason: String)
    case invalidPartition(entity: String, reason: String)
    case entityTypeMismatch(expected: String, actual: String)
    case persistableIdentityMismatch(EntityReference)
    case duplicateChange(EntityReference)
    case duplicatePrecondition(EntityReference)
    case incompatiblePreconditions(EntityReference)
    case entityAlreadyExists(EntityReference)
    case entityNotFound(EntityReference)
    case entityVersionMismatch(EntityReference)
    case fieldNotRepresentable(entity: String, field: String)
    case fieldValueNotRepresentable(entity: String, type: String, reason: String)
    case invalidCompiledSchema(entity: String, reason: String)
    case fieldsRequired(EntityReference)
    case fieldsMustBeEmptyForDelete(EntityReference)

    public var description: String {
        switch self {
        case .emptyMutation:
            return "An entity mutation must contain at least one change"
        case .changeLimitExceeded(let actual, let maximum):
            return "Entity mutation contains \(actual) changes, exceeding the limit of \(maximum)"
        case .preconditionLimitExceeded(let actual, let maximum):
            return "Entity mutation contains \(actual) preconditions, exceeding the limit of \(maximum)"
        case .unknownEntity(let entity):
            return "Entity '\(entity)' is not registered in the runtime schema"
        case .entityHasNoPersistableType(let entity):
            return "Entity '\(entity)' has no compiled Persistable type"
        case .invalidPersistableIdentifier(let entity, let reason):
            return "Entity '\(entity)' has an invalid persistable identifier: \(reason)"
        case .invalidPartition(let entity, let reason):
            return "Entity '\(entity)' has an invalid partition: \(reason)"
        case .entityTypeMismatch(let expected, let actual):
            return "Decoded entity type '\(actual)' does not match entity '\(expected)'"
        case .persistableIdentityMismatch(let identity):
            return "Decoded entity does not match identity '\(identity)'"
        case .duplicateChange(let identity):
            return "Entity mutation contains more than one change for '\(identity)'"
        case .duplicatePrecondition(let identity):
            return "Entity mutation contains more than one precondition for '\(identity)'"
        case .incompatiblePreconditions(let identity):
            return "Entity mutation contains incompatible preconditions for '\(identity)'"
        case .entityAlreadyExists(let identity):
            return "Entity '\(identity)' already exists"
        case .entityNotFound(let identity):
            return "Entity '\(identity)' does not exist"
        case .entityVersionMismatch(let identity):
            return "Entity '\(identity)' changed after the supplied version was read"
        case .fieldNotRepresentable(let entity, let field):
            return "Entity '\(entity)' field '\(field)' cannot be represented by the canonical field model"
        case .fieldValueNotRepresentable(let entity, let type, let reason):
            return "Entity '\(entity)' value of type '\(type)' cannot be represented: \(reason)"
        case .invalidCompiledSchema(let entity, let reason):
            return "Entity '\(entity)' has an invalid compiled schema: \(reason)"
        case .fieldsRequired(let identity):
            return "Mutation fields are required for '\(identity)'"
        case .fieldsMustBeEmptyForDelete(let identity):
            return "Delete mutation fields must be empty for '\(identity)'"
        }
    }
}
