#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

/// Container-exclusive storage engines paired with their validated topology.
struct ClaimedDatabaseStorageTopology: Sendable {
    struct Domain: Sendable {
        let id: DatabaseStorageDomain.ID
        let rootPath: [String]
        let engine: any StorageEngine
    }

    let controlDomainID: DatabaseStorageDomain.ID
    let domains: [DatabaseStorageDomain.ID: Domain]
    let placements: [Base.Placement.ID: DatabaseStoragePlacement]
    let defaultPlacementID: Base.Placement.ID

    var controlDomain: Domain {
        guard let domain = domains[controlDomainID] else {
            preconditionFailure("A validated topology must retain its control domain")
        }
        return domain
    }
}
#endif
