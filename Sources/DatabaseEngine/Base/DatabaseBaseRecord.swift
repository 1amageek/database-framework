#if DATABASE_MULTI_BASE
import DatabaseKit

/// Durable control-domain description of one Base placement and lifecycle.
///
/// The record does not store a Directory path. Section 14 fixes the address of
/// a Base Partition at `bases/<Base.ID>` below the database root of the domain
/// named by ``domainID``, so the path is derived from the record rather than
/// persisted beside it. Storing it would create a second binding record for a
/// value the storage layout already determines.
@_spi(DatabaseExecution)
public struct DatabaseBaseRecord: Sendable, Hashable, StorageFrameValue {
    public let id: Base.ID
    public let placementID: Base.Placement.ID
    public let domainID: DatabaseStorageDomain.ID
    public let placementGeneration: UInt64
    public let revision: UInt64
    public let lifecycle: DatabaseBaseLifecycleState

    package init(
        id: Base.ID,
        placementID: Base.Placement.ID,
        domainID: DatabaseStorageDomain.ID,
        placementGeneration: UInt64,
        revision: UInt64,
        lifecycle: DatabaseBaseLifecycleState
    ) {
        self.id = id
        self.placementID = placementID
        self.domainID = domainID
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
