import Core
import DatabaseWire
import StorageKit

/// Encodes compiled records into the single canonical binary storage format.
public enum DatabaseRecordStorageCodec {
    public static let formatVersion: UInt16 = 1

    private static let magic: [UInt8] = [0x44, 0x42, 0x52, 0x43]
    private static func wireLimits() throws -> DatabaseWireLimits {
        try DatabaseWireLimits(
            maximumFrameBytes: 16 * 1_024 * 1_024,
            maximumStringBytes: 1 * 1_024 * 1_024,
            maximumByteStringBytes: 16 * 1_024 * 1_024,
            maximumCollectionCount: 100_000,
            maximumNestingDepth: 64,
            maximumObjectCount: 250_000
        )
    }

    public static func encode(
        _ model: any Persistable
    ) throws -> Bytes {
        let modelType = type(of: model)
        try requireCompiledSchema(modelType)
        let fields = try DatabaseRecordEncoder.encode(model)
        return try encode(
            entity: modelType.persistableType,
            fields: fields
        )
    }

    /// Encodes catalog-validated fields into the canonical DBRC v1 frame.
    public static func encode(
        entity: String,
        fields: [DatabaseRecordField]
    ) throws -> Bytes {
        try DatabaseRecordFieldFrameCodec.encode(
            magic: magic,
            version: formatVersion,
            entity: entity,
            fields: fields,
            limits: try wireLimits()
        )
    }

    public static func decode<Model: Persistable>(
        _ type: Model.Type,
        from bytes: Bytes
    ) throws -> Model {
        try requireCompiledSchema(type)
        let fields = try decodeFields(
            from: bytes,
            expectedEntity: type.persistableType
        ).fields
        return try type.decodeDatabaseRecord(fields)
    }

    public static func decodeAny(
        _ type: any Persistable.Type,
        from bytes: Bytes
    ) throws -> any Persistable {
        try requireCompiledSchema(type)
        let fields = try decodeFields(
            from: bytes,
            expectedEntity: type.persistableType
        ).fields
        return try type.decodeDatabaseRecord(fields)
    }

    /// Decodes a bounded canonical DBRC v1 frame for dynamic catalog tooling.
    public static func decodeFields(
        from bytes: Bytes,
        expectedEntity: String? = nil
    ) throws -> (entity: String, fields: [DatabaseRecordField]) {
        try DatabaseRecordFieldFrameCodec.decode(
            bytes,
            magic: magic,
            version: formatVersion,
            expectedEntity: expectedEntity,
            limits: try wireLimits()
        )
    }

    private static func requireCompiledSchema(
        _ type: any Persistable.Type
    ) throws {
        guard !type.fieldSchemas.isEmpty else {
            throw DatabaseRecordFrameError.missingCompiledSchema(
                entity: type.persistableType
            )
        }
    }
}
