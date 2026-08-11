#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import DatabaseTypes

/// Durable identity of the one lifecycle job authorized to finalize a Base deletion.
package struct DatabaseBaseDeletionRecord:
    Sendable,
    Hashable,
    StorageFrameValue
{
    package let baseID: Base.ID
    package let owner: ByteString
    package let deletingRevision: UInt64

    package init(
        baseID: Base.ID,
        owner: ByteString,
        deletingRevision: UInt64
    ) {
        self.baseID = baseID
        self.owner = owner
        self.deletingRevision = deletingRevision
    }

    package func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(baseID.value)
        try encoder.writeBytes(owner)
        encoder.writeUInt64(deletingRevision)
    }

    package init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        do {
            self.baseID = try Base.ID(decoder.readString())
        } catch {
            throw .invalidValue
        }
        let owner = try decoder.readBytes()
        guard owner.count == 16 else {
            throw .invalidValue
        }
        self.owner = owner
        self.deletingRevision = try decoder.readUInt64()
    }
}

#endif
