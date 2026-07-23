import DatabaseValue
import DatabaseWire

struct DatabasePartitionCatalogContinuation: DatabaseWireValue {
    private static let version: UInt8 = 1

    let entity: String?
    let lastKey: DatabaseBytes

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.version)
        try writer.writeOptionalString(entity)
        try writer.writeBytes(lastKey)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        guard try reader.readUInt8() == Self.version else {
            throw DatabaseWireError.invalidValueTag(0)
        }
        self.entity = try reader.readOptionalString()
        self.lastKey = try reader.readBytes()
    }

    init(entity: String?, lastKey: DatabaseBytes) {
        self.entity = entity
        self.lastKey = lastKey
    }
}
