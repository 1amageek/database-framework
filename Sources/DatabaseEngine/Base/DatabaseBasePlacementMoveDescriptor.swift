#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes

/// Immutable source and destination identity retained by a resumable Base move.
///
/// The descriptor names storage domains, not Directory paths. Section 14 fixes
/// the address of a Base Partition at `bases/<Base.ID>` below the database root
/// of its domain, so naming the domain fully determines where the Partition is.
///
/// The two root prefixes are the generations of the resolved Partitions. They
/// are recorded once, at prepare, so a later slice can detect a Partition that
/// was removed and recreated underneath the move instead of copying into the
/// wrong keyspace.
@_spi(DatabaseExecution)
public struct DatabaseBasePlacementMoveDescriptor: Sendable, Hashable {
    public let baseID: Base.ID
    public let sourcePlacementID: Base.Placement.ID
    public let sourceDomainID: DatabaseStorageDomain.ID
    public let sourcePlacementGeneration: UInt64
    public let movingRevision: UInt64
    public let destinationPlacementID: Base.Placement.ID
    public let destinationDomainID: DatabaseStorageDomain.ID
    public let destinationPlacementGeneration: UInt64
    public let sourceRootPrefix: ByteString?
    public let destinationRootPrefix: ByteString?

    public init(
        baseID: Base.ID,
        sourcePlacementID: Base.Placement.ID,
        sourceDomainID: DatabaseStorageDomain.ID,
        sourcePlacementGeneration: UInt64,
        movingRevision: UInt64,
        destinationPlacementID: Base.Placement.ID,
        destinationDomainID: DatabaseStorageDomain.ID,
        destinationPlacementGeneration: UInt64,
        sourceRootPrefix: ByteString? = nil,
        destinationRootPrefix: ByteString? = nil
    ) {
        self.baseID = baseID
        self.sourcePlacementID = sourcePlacementID
        self.sourceDomainID = sourceDomainID
        self.sourcePlacementGeneration = sourcePlacementGeneration
        self.movingRevision = movingRevision
        self.destinationPlacementID = destinationPlacementID
        self.destinationDomainID = destinationDomainID
        self.destinationPlacementGeneration = destinationPlacementGeneration
        self.sourceRootPrefix = sourceRootPrefix
        self.destinationRootPrefix = destinationRootPrefix
    }
}

#endif
