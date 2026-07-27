import DatabaseKit
import DatabaseWire
import StorageKit

enum DatabaseSchemaFingerprint {
    private static let formatVersion: UInt16 = 1

    static func compute(_ schema: Schema) throws -> Bytes {
        let projection = try StorageFrameEncoder.encode {
            writer throws(StorageFrameError) in
            try writer.writeString("database-framework.schema")
            writer.writeUInt16(formatVersion)
            write(schema.version, into: &writer)

            let entities = schema.entities.sorted { $0.name < $1.name }
            try writer.writeCount(entities.count)
            for entity in entities {
                try writer.writeLengthPrefixed {
                    (writer: inout StorageFrameEncoder) throws(
                        StorageFrameError
                    ) in
                    try SchemaEntityEntryCodec.writeCanonical(
                        entity,
                        into: &writer
                    )
                }
            }
        }
        var accumulator = SHA256Accumulator()
        accumulator.update(projection)
        return Bytes(retaining: accumulator.finalize())
    }

    private static func write(
        _ version: SchemaVersion,
        into writer: inout StorageFrameEncoder
    ) {
        writer.writeUInt32(version.major)
        writer.writeUInt32(version.minor)
        writer.writeUInt32(version.patch)
    }
}
