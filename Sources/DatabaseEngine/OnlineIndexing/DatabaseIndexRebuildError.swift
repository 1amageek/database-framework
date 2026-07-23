import DatabaseValue

package enum DatabaseIndexRebuildError: Error, Sendable, Equatable {
    case entityNotFound(String)
    case compiledTypeMissing(String)
    case indexNotFound(entity: String, index: String)
    case buildAlreadyActive(index: String, generation: DatabaseUUID)
    case invalidContinuation
    case invalidWorkLimit(UInt64)
    case corruptedRecord
    case recordCountOverflow
    case uniquenessViolation(index: String)
}
