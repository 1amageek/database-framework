import DatabaseKit

/// Typed failures produced by the durable Composition catalog.
public enum DatabaseCompositionCatalogError: Error, Sendable, Equatable {
    case compositionNotFound(Base.Composition.ID)
    case compositionAlreadyExists(Base.Composition.ID)
    case revisionConflict(expected: UInt64, actual: UInt64)
    case memberBaseNotFound(Base.ID)
    case memberBaseNotActive(Base.ID)
    case catalogTooLarge(maximum: Int)
    case corruptedRecord(Base.Composition.ID?)
}
