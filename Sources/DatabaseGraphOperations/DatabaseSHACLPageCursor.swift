import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

struct DatabaseSHACLPageCursor: DatabaseRuntimePayloadValue, Hashable {
    private static let formatVersion: UInt8 = 1

    let shapesGraph: String
    let validationFingerprint: ByteString
    let offset: UInt64

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try writer.writeString(shapesGraph)
        try writer.writeBytes(validationFingerprint)
        writer.writeUInt64(offset)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .invalidValueTag(version)
        }
        self.shapesGraph = try reader.readString()
        self.validationFingerprint = try reader.readBytes()
        self.offset = try reader.readUInt64()
    }

    init(
        shapesGraph: String,
        validationFingerprint: ByteString,
        offset: UInt64
    ) {
        self.shapesGraph = shapesGraph
        self.validationFingerprint = validationFingerprint
        self.offset = offset
    }
}

#endif
