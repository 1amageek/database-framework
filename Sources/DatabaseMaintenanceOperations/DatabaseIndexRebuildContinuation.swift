import DatabaseJobRuntime
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseIndexRebuildContinuation: DatabaseRuntimePayloadValue {
    private static let version: UInt8 = 1

    package let generation: DatabaseTypes.UUID
    package let requestFingerprint: ByteString

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.version)
        try generation.encode(into: &writer)
        try writer.writeBytes(requestFingerprint)
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.version else {
            throw DatabaseWireError.invalidValueTag(version)
        }
        self.generation = try DatabaseTypes.UUID(from: &reader)
        self.requestFingerprint = try reader.readBytes()
    }

    package init(
        generation: DatabaseTypes.UUID,
        requestFingerprint: ByteString
    ) {
        self.generation = generation
        self.requestFingerprint = requestFingerprint
    }
}
