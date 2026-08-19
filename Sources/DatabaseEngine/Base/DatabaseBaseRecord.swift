#if DATABASE_MULTI_BASE
import DatabaseKit

/// Durable control-domain description of one Base placement and lifecycle.
@_spi(DatabaseExecution)
public struct DatabaseBaseRecord: Sendable, Hashable, StorageFrameValue {
    public let id: Base.ID
    public let placementID: Base.Placement.ID
    public let domainID: DatabaseStorageDomain.ID
    public let namespacePath: [String]
    public let placementGeneration: UInt64
    public let revision: UInt64
    public let lifecycle: DatabaseBaseLifecycleState

    package init(
        id: Base.ID,
        placementID: Base.Placement.ID,
        domainID: DatabaseStorageDomain.ID,
        namespacePath: [String],
        placementGeneration: UInt64,
        revision: UInt64,
        lifecycle: DatabaseBaseLifecycleState
    ) {
        self.id = id
        self.placementID = placementID
        self.domainID = domainID
        self.namespacePath = namespacePath
        self.placementGeneration = placementGeneration
        self.revision = revision
        self.lifecycle = lifecycle
    }

    package func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(id.value)
        try encoder.writeString(placementID.value)
        try encoder.writeString(domainID.value)
        try encoder.writeCount(namespacePath.count)
        for component in namespacePath {
            try encoder.writeString(component)
        }
        encoder.writeUInt64(placementGeneration)
        encoder.writeUInt64(revision)
        encoder.writeUInt8(lifecycle.rawValue)
    }

    package init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        do {
            self.id = try Base.ID(decoder.readString())
            self.placementID = try Base.Placement.ID(decoder.readString())
            self.domainID = try DatabaseStorageDomain.ID(decoder.readString())
        } catch {
            throw .invalidValue
        }
        let componentCount = try decoder.readCount()
        guard componentCount > 0 else {
            throw .invalidValue
        }
        var namespacePath: [String] = []
        namespacePath.reserveCapacity(componentCount)
        for _ in 0..<componentCount {
            let component = try decoder.readString()
            guard !component.isEmpty else {
                throw .invalidValue
            }
            namespacePath.append(component)
        }
        self.namespacePath = namespacePath
        self.placementGeneration = try decoder.readUInt64()
        self.revision = try decoder.readUInt64()
        let rawLifecycle = try decoder.readUInt8()
        guard let lifecycle = DatabaseBaseLifecycleState(
            rawValue: rawLifecycle
        ) else {
            throw .invalidValue
        }
        self.lifecycle = lifecycle
    }
}

#endif
