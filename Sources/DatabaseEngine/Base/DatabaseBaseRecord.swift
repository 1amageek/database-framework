#if DATABASE_MULTIPLE_BASES
import DatabaseKit

/// Durable control-domain description of one Base placement and lifecycle.
package struct DatabaseBaseRecord: Sendable, Hashable, StorageFrameValue {
    package let id: Base.ID
    package let placementID: Base.Placement.ID
    package let domainID: DatabaseStorageDomain.ID
    package let namespacePath: [String]
    package let placementGeneration: UInt64
    package let revision: UInt64
    package let lifecycle: DatabaseBaseLifecycleState

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
