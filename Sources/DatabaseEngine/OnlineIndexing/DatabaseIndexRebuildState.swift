import DatabaseValue
import DatabaseWire
import StorageKit

package struct DatabaseIndexRebuildState: DatabaseWireValue, Hashable {
    package enum Phase: UInt8, Sendable, Hashable {
        case building = 1
        case complete = 2
        case failed = 3
    }

    package let entity: String
    package let index: String
    package let generation: DatabaseUUID
    package let phase: Phase
    package let lastProcessedKey: Bytes?
    package let indexedEntityCount: UInt64
    package let detail: String?

    package init(
        entity: String,
        index: String,
        generation: DatabaseUUID,
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
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try writer.writeString(entity)
        try writer.writeString(index)
        try generation.encode(into: &writer)
        writer.writeUInt8(phase.rawValue)
        try writer.writeOptionalBytes(
            lastProcessedKey.map(DatabaseBytes.init(retaining:))
        )
        writer.writeUInt64(indexedEntityCount)
        try writer.writeOptionalString(detail)
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let entity = try reader.readString()
        let index = try reader.readString()
        let generation = try DatabaseUUID(from: &reader)
        let rawPhase = try reader.readUInt8()
        guard let phase = Phase(rawValue: rawPhase) else {
            throw DatabaseWireError.invalidValueTag(rawPhase)
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
