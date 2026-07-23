import DatabaseValue

public enum DatabaseMutationError: Error, Sendable, CustomStringConvertible {
    case emptyMutation
    case mutationLimitExceeded(actual: Int, maximum: Int)
    case preconditionLimitExceeded(actual: Int, maximum: Int)
    case idempotencyKeyRequired
    case idempotencyKeyTooLarge(actual: Int, maximum: Int)
    case idempotencyKeyConflict
    case idempotencyRecordCorrupted
    case logicalVersionOverflow
    case unknownEntity(String)
    case entityHasNoPersistableType(String)
    case invalidRecordIdentifier(entity: String, reason: String)
    case invalidPartition(entity: String, reason: String)
    case invalidGraphPartitions(String)
    case recordTypeMismatch(expected: String, actual: String)
    case recordIdentityMismatch(RecordIdentity)
    case duplicateChange(RecordIdentity)
    case duplicatePrecondition(RecordIdentity)
    case incompatiblePreconditions(RecordIdentity)
    case recordAlreadyExists(RecordIdentity)
    case recordNotFound(RecordIdentity)
    case recordVersionMismatch(RecordIdentity)
    case recordIdentifierNotRepresentable(String)
    case recordFieldNotRepresentable(entity: String, field: String)
    case invalidCompiledSchema(entity: String, reason: String)
    case unsupportedStatement(String)
    case fieldsRequired(RecordIdentity)
    case fieldsMustBeEmptyForDelete(RecordIdentity)
    case statementExecutorNotConfigured
    case stateStoreContainerMismatch
    case relationshipWorkLimitExceeded(maximum: UInt64)
    case relationshipMutationConflict(RecordIdentity)
    case relationshipTargetNotFound(owner: RecordIdentity, target: RecordIdentity)
    case relationshipCatalogCorrupted(RecordIdentity)

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
        case .idempotencyRecordCorrupted:
            return "The stored idempotency record is corrupted"
        case .logicalVersionOverflow:
            return "The logical commit version reached UInt64.max"
        case .unknownEntity(let entity):
            return "Entity '\(entity)' is not registered in the runtime schema"
        case .entityHasNoPersistableType(let entity):
            return "Entity '\(entity)' has no compiled Persistable type"
        case .invalidRecordIdentifier(let entity, let reason):
            return "Entity '\(entity)' has an invalid record identifier: \(reason)"
        case .invalidPartition(let entity, let reason):
            return "Entity '\(entity)' has an invalid partition: \(reason)"
        case .invalidGraphPartitions(let reason):
            return "Mutation graph partitions are invalid: \(reason)"
        case .recordTypeMismatch(let expected, let actual):
            return "Decoded record type '\(actual)' does not match entity '\(expected)'"
        case .recordIdentityMismatch(let identity):
            return "Decoded record does not match identity '\(identity)'"
        case .duplicateChange(let identity):
            return "Mutation contains more than one change for '\(identity)'"
        case .duplicatePrecondition(let identity):
            return "Mutation contains more than one precondition for '\(identity)'"
        case .incompatiblePreconditions(let identity):
            return "Mutation contains incompatible preconditions for '\(identity)'"
        case .recordAlreadyExists(let identity):
            return "Record '\(identity)' already exists"
        case .recordNotFound(let identity):
            return "Record '\(identity)' does not exist"
        case .recordVersionMismatch(let identity):
            return "Record '\(identity)' changed after the supplied version was read"
        case .recordIdentifierNotRepresentable(let entity):
            return "Entity '\(entity)' has an identifier that DatabaseWire cannot represent"
        case .recordFieldNotRepresentable(let entity, let field):
            return "Entity '\(entity)' field '\(field)' cannot be represented by DatabaseWire"
        case .invalidCompiledSchema(let entity, let reason):
            return "Entity '\(entity)' has an invalid compiled schema: \(reason)"
        case .unsupportedStatement(let reason):
            return "Statement mutation is unsupported: \(reason)"
        case .fieldsRequired(let identity):
            return "Mutation fields are required for '\(identity)'"
        case .fieldsMustBeEmptyForDelete(let identity):
            return "Delete mutation fields must be empty for '\(identity)'"
        case .statementExecutorNotConfigured:
            return "A statement mutation executor is not configured"
        case .stateStoreContainerMismatch:
            return "Mutation state store and operation context use different containers"
        case .relationshipWorkLimitExceeded(let maximum):
            return "Relationship planning exceeded the work limit of \(maximum)"
        case .relationshipMutationConflict(let identity):
            return "Explicit mutation for '\(identity)' conflicts with a cascade delete"
        case .relationshipTargetNotFound(let owner, let target):
            return "Record '\(owner)' references missing record '\(target)'"
        case .relationshipCatalogCorrupted(let identity):
            return "Relationship catalog references missing owner '\(identity)'"
        }
    }
}
