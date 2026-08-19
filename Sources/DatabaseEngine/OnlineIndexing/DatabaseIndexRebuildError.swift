import DatabaseTypes

public enum DatabaseIndexRebuildError: Error, Sendable, Equatable {
    case entityNotFound(String)
    case polymorphicGroupNotFound(String)
    case polymorphicGroupHasNoMembers(String)
    case polymorphicTypeCodeCollision(group: String, typeCode: Int64)
    case compiledTypeMissing(String)
    case indexNotFound(entity: String, index: String)
    case indexGenerationMismatch(String)
    case buildAlreadyActive(index: String, generation: DatabaseTypes.UUID)
    case invalidContinuation
    case invalidWorkLimit(UInt64)
    case corruptedRebuildState
    case entityCountOverflow
    case uniquenessViolation(index: String)
}
