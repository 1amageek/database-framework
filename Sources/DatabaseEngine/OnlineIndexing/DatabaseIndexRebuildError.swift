import DatabaseTypes

public enum DatabaseIndexRebuildError: Error, Sendable, Equatable {
    case entityNotFound(String)
    case polymorphicGroupNotFound(String)
    case polymorphicGroupHasNoMembers(String)
    case polymorphicTypeCodeCollision(group: String, typeCode: Int64)
    case compiledTypeMissing(String)
    case indexNotFound(entity: String, index: String)

    /// The directory the target addresses does not exist.
    ///
    /// Maintenance opens a directory that `prepareResources` created, so its
    /// absence means the partition was removed under a running job rather than
    /// that the job has no work.
    case directoryNotFound(entity: String, index: String)
    case indexGenerationMismatch(String)
    case buildAlreadyActive(index: String, generation: DatabaseTypes.UUID)
    case invalidContinuation
    case invalidWorkLimit(UInt64)
    case corruptedRebuildState
    case entityCountOverflow
    case uniquenessViolation(index: String)
}
