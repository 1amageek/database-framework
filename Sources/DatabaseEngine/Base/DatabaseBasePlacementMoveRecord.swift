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
        try Self.write(descriptor.sourceNamespacePath, to: &encoder)
        encoder.writeUInt64(descriptor.sourcePlacementGeneration)
        encoder.writeUInt64(descriptor.movingRevision)
        try encoder.writeString(descriptor.destinationPlacementID.value)
        try encoder.writeString(descriptor.destinationDomainID.value)
        try Self.write(descriptor.destinationNamespacePath, to: &encoder)
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
            let sourcePath = try Self.readPath(from: &decoder)
            let sourceGeneration = try decoder.readUInt64()
            let movingRevision = try decoder.readUInt64()
            let destinationPlacementID = try Base.Placement.ID(
                decoder.readString()
            )
            let destinationDomainID = try DatabaseStorageDomain.ID(
                decoder.readString()
            )
            let destinationPath = try Self.readPath(from: &decoder)
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
                    sourceNamespacePath: sourcePath,
                    sourcePlacementGeneration: sourceGeneration,
                    movingRevision: movingRevision,
                    destinationPlacementID: destinationPlacementID,
                    destinationDomainID: destinationDomainID,
                    destinationNamespacePath: destinationPath,
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

    private static func write(
        _ path: [String],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(path.count)
        for component in path {
            try encoder.writeString(component)
        }
    }

    private static func readPath(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [String] {
        let count = try decoder.readCount()
        guard count > 0 else { throw .invalidValue }
        var path: [String] = []
        path.reserveCapacity(count)
        for _ in 0..<count {
            let component = try decoder.readString()
            guard !component.isEmpty else { throw .invalidValue }
            path.append(component)
        }
        return path
    }
}
