#if DATABASE_MULTIPLE_BASES
import DatabaseKit

/// A named placement maps newly provisioned Bases to one storage domain path.
public struct DatabaseStoragePlacement: Sendable, Hashable {
    public let id: Base.Placement.ID
    public let domainID: DatabaseStorageDomain.ID
    public let path: [String]

    public init(
        id: Base.Placement.ID,
        domainID: DatabaseStorageDomain.ID,
        path: [String]
    ) throws(DatabaseStorageTopologyError) {
        guard !path.isEmpty else {
            throw .emptyPlacementPath(placementID: id)
        }
        for component in path {
            guard !component.isEmpty else {
                throw .emptyPlacementPathComponent(placementID: id)
            }
        }
        self.id = id
        self.domainID = domainID
        self.path = path
    }
}
#endif
