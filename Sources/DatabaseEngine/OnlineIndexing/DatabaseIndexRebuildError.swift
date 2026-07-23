import DatabaseValue

package enum DatabaseIndexRebuildError: Error, Sendable, Equatable {
    case entityNotFound(String)
    case compiledTypeMissing(String)
    case indexNotFound(entity: String, index: String)
    case buildAlreadyActive(index: String, generation: DatabaseUUID)
    case invalidContinuation
    case invalidWorkLimit(UInt64)
    case corruptedRebuildState
    case entityCountOverflow
    case uniquenessViolation(index: String)
}
