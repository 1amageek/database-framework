#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes

/// Immutable source and destination identity retained by a resumable Base move.
@_spi(DatabaseExecution)
public struct DatabaseBasePlacementMoveDescriptor: Sendable, Hashable {
    public let baseID: Base.ID
    public let sourcePlacementID: Base.Placement.ID
    public let sourceDomainID: DatabaseStorageDomain.ID
    public let sourceNamespacePath: [String]
    public let sourcePlacementGeneration: UInt64
    public let movingRevision: UInt64
    public let destinationPlacementID: Base.Placement.ID
    public let destinationDomainID: DatabaseStorageDomain.ID
    public let destinationNamespacePath: [String]
    public let destinationPlacementGeneration: UInt64
    public let sourceRootPrefix: ByteString?
    public let destinationRootPrefix: ByteString?

    public init(
        baseID: Base.ID,
        sourcePlacementID: Base.Placement.ID,
        sourceDomainID: DatabaseStorageDomain.ID,
        sourceNamespacePath: [String],
        sourcePlacementGeneration: UInt64,
        movingRevision: UInt64,
        destinationPlacementID: Base.Placement.ID,
        destinationDomainID: DatabaseStorageDomain.ID,
        destinationNamespacePath: [String],
        destinationPlacementGeneration: UInt64,
        sourceRootPrefix: ByteString? = nil,
        destinationRootPrefix: ByteString? = nil
    ) {
        self.baseID = baseID
        self.sourcePlacementID = sourcePlacementID
        self.sourceDomainID = sourceDomainID
        self.sourceNamespacePath = sourceNamespacePath
        self.sourcePlacementGeneration = sourcePlacementGeneration
        self.movingRevision = movingRevision
        self.destinationPlacementID = destinationPlacementID
        self.destinationDomainID = destinationDomainID
        self.destinationNamespacePath = destinationNamespacePath
        self.destinationPlacementGeneration = destinationPlacementGeneration
        self.sourceRootPrefix = sourceRootPrefix
        self.destinationRootPrefix = destinationRootPrefix
    }
}

#endif
