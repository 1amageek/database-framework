import Core
import DatabaseDigest
import DatabaseValue
import DatabaseWire
import StorageKit

enum DatabaseSchemaFingerprint {
    private static let formatVersion: UInt16 = 1

    static func compute(_ schema: Schema) throws -> Bytes {
        let projection = try DatabaseWireWriter.encodeThrowing { writer in
            try writer.writeString("database-framework.schema")
            writer.writeUInt16(formatVersion)
            try schema.version.encode(into: &writer)
            try schema.encodingVersion.encode(into: &writer)

            let entities = schema.entities.sorted { $0.name < $1.name }
            try writer.writeCount(entities.count)
            for entity in entities {
                try SchemaEntityRecordCodec.writeCanonical(
                    entity,
                    into: &writer
                )
            }

            let groups = schema.polymorphicGroups.sorted {
                $0.identifier < $1.identifier
            }
            try writer.writeCount(groups.count)
            for group in groups {
                try writer.writeString(group.identifier)
                try SchemaEntityRecordCodec.writeDirectory(
                    group.directoryComponents,
                    layer: group.directoryLayer,
                    into: &writer
                )
                let indexes = group.indexes.sorted { $0.name < $1.name }
                try writer.writeCount(indexes.count)
                for index in indexes {
                    try SchemaEntityRecordCodec.write(
                        index,
                        into: &writer
                    )
                }
                try SchemaEntityRecordCodec.writeStringArray(
                    group.memberTypeNames.sorted(),
                    into: &writer
                )
            }
        }
        var accumulator = SHA256Accumulator()
        accumulator.update(projection)
        return Bytes(retaining: accumulator.finalize())
    }
}
