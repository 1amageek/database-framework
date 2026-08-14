#if DATABASE_MULTIPLE_BASES
#if DATABASE_MULTIPLE_BASES
import DatabaseKit
#endif
import StorageKit

/// Container-exclusive storage engines paired with their validated topology.
struct ClaimedDatabaseStorageTopology: Sendable {
    struct Domain: Sendable {
        let id: DatabaseStorageDomain.ID
        let namespacePath: [String]
        let engine: any StorageEngine
    }

    let controlDomainID: DatabaseStorageDomain.ID
    let domains: [DatabaseStorageDomain.ID: Domain]
    #if DATABASE_MULTIPLE_BASES
    let placements: [Base.Placement.ID: DatabaseStoragePlacement]
    let defaultPlacementID: Base.Placement.ID
    #endif

    var controlDomain: Domain {
        guard let domain = domains[controlDomainID] else {
            preconditionFailure("A validated topology must retain its control domain")
        }
        return domain
    }
}
#endif
