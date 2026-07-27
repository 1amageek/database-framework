import DatabaseTypes

public enum DatabaseMutationError: Error, Sendable, CustomStringConvertible {
    case emptyMutation
    case mutationLimitExceeded(actual: Int, maximum: Int)
    case preconditionLimitExceeded(actual: Int, maximum: Int)
    case idempotencyKeyRequired
    case idempotencyKeyTooLarge(actual: Int, maximum: Int)
    case idempotencyKeyConflict
    case idempotencyEntryCorrupted
    case logicalVersionOverflow
    case unknownEntity(String)
    case entityHasNoPersistableType(String)
    case invalidPersistableIdentifier(entity: String, reason: String)
    case invalidPartition(entity: String, reason: String)
    case invalidGraphPartitions(String)
    case entityTypeMismatch(expected: String, actual: String)
    case persistableIdentityMismatch(EntityReference)
    case duplicateChange(EntityReference)
    case duplicatePrecondition(EntityReference)
    case incompatiblePreconditions(EntityReference)
    case entityAlreadyExists(EntityReference)
    case entityNotFound(EntityReference)
    case entityVersionMismatch(EntityReference)
    case identifierNotRepresentable(String)
    case fieldNotRepresentable(entity: String, field: String)
    case invalidCompiledSchema(entity: String, reason: String)
    case unsupportedStatement(String)
    case fieldsRequired(EntityReference)
    case fieldsMustBeEmptyForDelete(EntityReference)
    case stateStoreContainerMismatch

    public var description: String {
        switch self {
        case .emptyMutation:
            return "A mutation request must contain at least one change"
        case .mutationLimitExceeded(let actual, let maximum):
            return "Mutation contains \(actual) changes, exceeding the limit of \(maximum)"
        case .preconditionLimitExceeded(let actual, let maximum):
            return "Mutation contains \(actual) preconditions, exceeding the limit of \(maximum)"
        case .idempotencyKeyRequired:
            return "A mutation request requires an idempotency key"
        case .idempotencyKeyTooLarge(let actual, let maximum):
            return "Idempotency key contains \(actual) UTF-8 bytes, exceeding the limit of \(maximum)"
        case .idempotencyKeyConflict:
            return "The idempotency key is already associated with a different request"
        case .idempotencyEntryCorrupted:
            return "The stored idempotency entry is corrupted"
        case .logicalVersionOverflow:
            return "The logical commit version reached UInt64.max"
        case .unknownEntity(let entity):
            return "Entity '\(entity)' is not registered in the runtime schema"
        case .entityHasNoPersistableType(let entity):
            return "Entity '\(entity)' has no compiled Persistable type"
        case .invalidPersistableIdentifier(let entity, let reason):
            return "Entity '\(entity)' has an invalid persistable identifier: \(reason)"
        case .invalidPartition(let entity, let reason):
            return "Entity '\(entity)' has an invalid partition: \(reason)"
        case .invalidGraphPartitions(let reason):
            return "Mutation graph partitions are invalid: \(reason)"
        case .entityTypeMismatch(let expected, let actual):
            return "Decoded entity type '\(actual)' does not match entity '\(expected)'"
        case .persistableIdentityMismatch(let identity):
            return "Decoded entity does not match identity '\(identity)'"
        case .duplicateChange(let identity):
            return "Mutation contains more than one change for '\(identity)'"
        case .duplicatePrecondition(let identity):
            return "Mutation contains more than one precondition for '\(identity)'"
        case .incompatiblePreconditions(let identity):
            return "Mutation contains incompatible preconditions for '\(identity)'"
        case .entityAlreadyExists(let identity):
            return "Entity '\(identity)' already exists"
        case .entityNotFound(let identity):
            return "Entity '\(identity)' does not exist"
        case .entityVersionMismatch(let identity):
            return "Entity '\(identity)' changed after the supplied version was read"
        case .identifierNotRepresentable(let entity):
            return "Entity '\(entity)' has an identifier that DatabaseWire cannot represent"
        case .fieldNotRepresentable(let entity, let field):
            return "Entity '\(entity)' field '\(field)' cannot be represented by DatabaseWire"
        case .invalidCompiledSchema(let entity, let reason):
            return "Entity '\(entity)' has an invalid compiled schema: \(reason)"
        case .unsupportedStatement(let reason):
            return "Statement mutation is unsupported: \(reason)"
        case .fieldsRequired(let identity):
            return "Mutation fields are required for '\(identity)'"
        case .fieldsMustBeEmptyForDelete(let identity):
            return "Delete mutation fields must be empty for '\(identity)'"
        case .stateStoreContainerMismatch:
            return "Mutation state store and operation context use different containers"
        }
    }
}
import DatabaseKit
