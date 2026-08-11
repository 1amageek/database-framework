#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import DatabaseTypes

/// Immutable source and destination identity retained by a resumable Base move.
package struct DatabaseBasePlacementMoveDescriptor: Sendable, Hashable {
    package let baseID: Base.ID
    package let sourcePlacementID: Base.Placement.ID
    package let sourceDomainID: DatabaseStorageDomain.ID
    package let sourceNamespacePath: [String]
    package let sourcePlacementGeneration: UInt64
    package let movingRevision: UInt64
    package let destinationPlacementID: Base.Placement.ID
    package let destinationDomainID: DatabaseStorageDomain.ID
    package let destinationNamespacePath: [String]
    package let destinationPlacementGeneration: UInt64
    package let sourceRootPrefix: ByteString?
    package let destinationRootPrefix: ByteString?

    package init(
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
