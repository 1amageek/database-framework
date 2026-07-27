import DatabaseTypes
import StorageKit

package struct DatabaseIndexRebuildState: StorageFrameValue, Hashable {
    package enum Phase: UInt8, Sendable, Hashable {
        case building = 1
        case complete = 2
        case failed = 3
    }

    package let entity: String
    package let index: String
    package let generation: DatabaseTypes.UUID
    package let phase: Phase
    package let lastProcessedKey: Bytes?
    package let indexedEntityCount: UInt64
    package let detail: String?

    package init(
        entity: String,
        index: String,
        generation: DatabaseTypes.UUID,
        phase: Phase,
        lastProcessedKey: Bytes? = nil,
        indexedEntityCount: UInt64 = 0,
        detail: String? = nil
    ) {
        self.entity = entity
        self.index = index
        self.generation = generation
        self.phase = phase
        self.lastProcessedKey = lastProcessedKey
        self.indexedEntityCount = indexedEntityCount
        self.detail = detail
    }

    package func encode(
        to writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(entity)
        try writer.writeString(index)
        writer.writeUInt64(generation.high)
        writer.writeUInt64(generation.low)
        writer.writeUInt8(phase.rawValue)
        try writer.writeOptionalBytes(
            lastProcessedKey.map(ByteString.init(retaining:))
        )
        writer.writeUInt64(indexedEntityCount)
        try writer.writeOptionalString(detail)
    }

    package init(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        let entity = try reader.readString()
        let index = try reader.readString()
        let generation = DatabaseTypes.UUID(
            high: try reader.readUInt64(),
            low: try reader.readUInt64()
        )
        let rawPhase = try reader.readUInt8()
        guard let phase = Phase(rawValue: rawPhase) else {
            throw StorageFrameError.invalidValueTag(rawPhase)
        }
        self.init(
            entity: entity,
            index: index,
            generation: generation,
            phase: phase,
            lastProcessedKey: try reader.readOptionalBytes().map(Bytes.init(retaining:)),
            indexedEntityCount: try reader.readUInt64(),
            detail: try reader.readOptionalString()
        )
    }
}
