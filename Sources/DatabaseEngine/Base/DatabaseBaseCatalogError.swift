import DatabaseKit

/// Typed failures produced by the durable Base catalog.
public enum DatabaseBaseCatalogError: Error, Sendable, Equatable {
    case baseNotFound(Base.ID)
    case baseAlreadyExists(Base.ID)
    case baseIdentifierRetired(Base.ID)
    case revisionConflict(expected: UInt64, actual: UInt64)
    case invalidLifecycleTransition(
        baseID: Base.ID,
        from: UInt8,
        to: UInt8
    )
    case placementNotFound(Base.Placement.ID)
    case storageDomainNotFound(DatabaseStorageDomain.ID)
    case baseReferencedByComposition(Base.ID)
    case placementAlreadySelected(Base.Placement.ID)
    case placementDestinationMatchesSource(Base.ID)
    case placementDestinationClaimed(Base.ID)
    case placementDestinationNotEmpty(Base.ID)
    case placementDigestMismatch(Base.ID?)
    case placementTransferOverflow
    case invalidPlacementMoveOwner
    case invalidDeletionOwner
    case baseDeletionClaimed(Base.ID)
    case baseDeletionMarkerMissing(Base.ID)
    case catalogTooLarge(maximum: Int)
    case corruptedRecord(Base.ID?)
}
