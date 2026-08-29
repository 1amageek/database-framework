#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Durable control-domain identity for an in-progress placement transition.
package struct DatabaseBasePlacementMoveRecord:
    Sendable,
    Hashable,
    StorageFrameValue
{
    package let descriptor: DatabaseBasePlacementMoveDescriptor
    package let owner: ByteString

    package init(
        descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) {
        self.descriptor = descriptor
        self.owner = owner
    }

    package func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(descriptor.baseID.value)
        try encoder.writeString(descriptor.sourcePlacementID.value)
        try encoder.writeString(descriptor.sourceDomainID.value)
        encoder.writeUInt64(descriptor.sourcePlacementGeneration)
        encoder.writeUInt64(descriptor.movingRevision)
        try encoder.writeString(descriptor.destinationPlacementID.value)
        try encoder.writeString(descriptor.destinationDomainID.value)
        encoder.writeUInt64(descriptor.destinationPlacementGeneration)
        try encoder.writeOptionalBytes(descriptor.sourceRootPrefix)
        try encoder.writeOptionalBytes(descriptor.destinationRootPrefix)
        try encoder.writeBytes(owner)
    }

    package init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        do {
            let baseID = try Base.ID(decoder.readString())
            let sourcePlacementID = try Base.Placement.ID(
                decoder.readString()
            )
            let sourceDomainID = try DatabaseStorageDomain.ID(
                decoder.readString()
            )
            let sourceGeneration = try decoder.readUInt64()
            let movingRevision = try decoder.readUInt64()
            let destinationPlacementID = try Base.Placement.ID(
                decoder.readString()
            )
            let destinationDomainID = try DatabaseStorageDomain.ID(
                decoder.readString()
            )
            let destinationGeneration = try decoder.readUInt64()
            let sourceRootPrefix = try decoder.readOptionalBytes()
            let destinationRootPrefix = try decoder.readOptionalBytes()
            let owner = try decoder.readBytes()
            guard owner.count == 16 else {
                throw StorageFrameError.invalidValue
            }
            self.init(
                descriptor: DatabaseBasePlacementMoveDescriptor(
                    baseID: baseID,
                    sourcePlacementID: sourcePlacementID,
                    sourceDomainID: sourceDomainID,
                    sourcePlacementGeneration: sourceGeneration,
                    movingRevision: movingRevision,
                    destinationPlacementID: destinationPlacementID,
                    destinationDomainID: destinationDomainID,
                    destinationPlacementGeneration: destinationGeneration,
                    sourceRootPrefix: sourceRootPrefix,
                    destinationRootPrefix: destinationRootPrefix
                ),
                owner: owner
            )
        } catch let error as StorageFrameError {
            throw error
        } catch {
            throw .invalidValue
        }
    }
}

#endif
