#if DATABASE_MULTIPLE_BASES
import DatabaseKit
#endif

/// Prepared, container-owned domain roots and named placements.
package struct DatabaseStorageRuntimeTopology: Sendable {
    package let controlDomainID: DatabaseStorageDomain.ID
    package let domains: [DatabaseStorageDomain.ID: DatabaseStorageDomainRuntime]
    #if DATABASE_MULTIPLE_BASES
    package let placements: [Base.Placement.ID: DatabaseStoragePlacement]
    package let defaultPlacementID: Base.Placement.ID
    #endif

    package var controlDomain: DatabaseStorageDomainRuntime {
        guard let domain = domains[controlDomainID] else {
            preconditionFailure("A prepared topology must retain its control domain")
        }
        return domain
    }

    package func domain(
        identifiedBy id: DatabaseStorageDomain.ID
    ) -> DatabaseStorageDomainRuntime? {
        domains[id]
    }

    #if DATABASE_MULTIPLE_BASES
    package func placement(
        identifiedBy id: Base.Placement.ID
    ) -> DatabaseStoragePlacement? {
        placements[id]
    }
    #endif
}
