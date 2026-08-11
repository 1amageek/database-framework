#if DATABASE_MULTIPLE_BASES
import DatabaseKit
#endif

/// Validation and lifecycle failures for a container-owned storage topology.
public enum DatabaseStorageTopologyError: Error, Sendable, Equatable {
    case invalidDomainID(String)
    case noDomains
    case duplicateDomainID(DatabaseStorageDomain.ID)
    case duplicateStorageEngine(
        first: DatabaseStorageDomain.ID,
        second: DatabaseStorageDomain.ID
    )
    case missingControlDomain(DatabaseStorageDomain.ID)
    case emptyDomainNamespace(domainID: DatabaseStorageDomain.ID)
    case emptyNamespaceComponent(domainID: DatabaseStorageDomain.ID)
    #if DATABASE_MULTIPLE_BASES
    case noPlacements
    case duplicatePlacementID(Base.Placement.ID)
    case duplicatePlacementDestination(
        first: Base.Placement.ID,
        second: Base.Placement.ID
    )
    case missingPlacementDomain(
        placementID: Base.Placement.ID,
        domainID: DatabaseStorageDomain.ID
    )
    case missingDefaultPlacement(Base.Placement.ID)
    case emptyPlacementPath(placementID: Base.Placement.ID)
    case emptyPlacementPathComponent(placementID: Base.Placement.ID)
    #endif
    case configurationAlreadyClaimed
}
