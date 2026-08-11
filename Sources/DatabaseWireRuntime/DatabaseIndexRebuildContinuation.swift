import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabaseIndexRebuildContinuation: DatabaseRuntimePayloadValue {
    private static let version: UInt8 = 1

    let generation: DatabaseTypes.UUID
    let requestFingerprint: ByteString

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.version)
        try generation.encode(into: &writer)
        try writer.writeBytes(requestFingerprint)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.version else {
            throw DatabaseWireError.invalidValueTag(version)
        }
        self.generation = try DatabaseTypes.UUID(from: &reader)
        self.requestFingerprint = try reader.readBytes()
    }

    init(
        generation: DatabaseTypes.UUID,
        requestFingerprint: ByteString
    ) {
        self.generation = generation
        self.requestFingerprint = requestFingerprint
    }
}
