#if DATABASE_MULTI_BASE
import DatabaseKit

/// A named placement routes newly provisioned Bases to one storage domain.
///
/// Section 14 fixes the address of a Base Partition at `bases/<Base.ID>` below
/// the database root of its domain, so a placement carries no path of its own:
/// naming the domain fully determines where the Partition is created.
public struct DatabaseStoragePlacement: Sendable, Hashable {
    public let id: Base.Placement.ID
    public let domainID: DatabaseStorageDomain.ID

    public init(
        id: Base.Placement.ID,
        domainID: DatabaseStorageDomain.ID
    ) {
        self.id = id
        self.domainID = domainID
    }
}
#endif
