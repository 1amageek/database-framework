#if DATABASE_MULTI_BASE
import DatabaseKit

/// Immutable, validated storage-domain and placement configuration.
public struct DatabaseStorageTopology: Sendable {
    public let controlDomainID: DatabaseStorageDomain.ID
    public let domains: [DatabaseStorageDomain]
    public let placements: [DatabaseStoragePlacement]
    public let defaultPlacementID: Base.Placement.ID

    public init(
        controlDomainID: DatabaseStorageDomain.ID,
        domains: [DatabaseStorageDomain],
        placements: [DatabaseStoragePlacement],
        defaultPlacementID: Base.Placement.ID
    ) throws(DatabaseStorageTopologyError) {
        guard !domains.isEmpty else {
            throw .noDomains
        }

        var domainIDs: Set<DatabaseStorageDomain.ID> = []
        var validatedDomains: [DatabaseStorageDomain] = []
        validatedDomains.reserveCapacity(domains.count)
        for domain in domains {
            guard domainIDs.insert(domain.id).inserted else {
                throw .duplicateDomainID(domain.id)
            }
            if let owner = validatedDomains.first(where: {
                $0.storageEngine === domain.storageEngine
            }) {
                throw .duplicateStorageEngine(
                    first: owner.id,
                    second: domain.id
                )
            }
            validatedDomains.append(domain)
        }
        guard domainIDs.contains(controlDomainID) else {
            throw .missingControlDomain(controlDomainID)
        }

        guard !placements.isEmpty else {
            throw .noPlacements
        }
        var placementIDs: Set<Base.Placement.ID> = []
        var validatedPlacements: [DatabaseStoragePlacement] = []
        for placement in placements {
            guard placementIDs.insert(placement.id).inserted else {
                throw .duplicatePlacementID(placement.id)
            }
            guard domainIDs.contains(placement.domainID) else {
                throw .missingPlacementDomain(
                    placementID: placement.id,
                    domainID: placement.domainID
                )
            }
            if let existing = validatedPlacements.first(where: {
                $0.domainID == placement.domainID && $0.path == placement.path
            }) {
                throw .duplicatePlacementDestination(
                    first: existing.id,
                    second: placement.id
                )
            }
            validatedPlacements.append(placement)
        }
        guard placementIDs.contains(defaultPlacementID) else {
            throw .missingDefaultPlacement(defaultPlacementID)
        }

        self.controlDomainID = controlDomainID
        self.domains = domains.sorted { $0.id < $1.id }
        self.placements = placements.sorted { $0.id < $1.id }
        self.defaultPlacementID = defaultPlacementID
    }

    public func domain(
        identifiedBy id: DatabaseStorageDomain.ID
    ) -> DatabaseStorageDomain? {
        domains.first { $0.id == id }
    }

    public func placement(
        identifiedBy id: Base.Placement.ID
    ) -> DatabaseStoragePlacement? {
        placements.first { $0.id == id }
    }
}
#endif
