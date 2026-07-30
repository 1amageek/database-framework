import DatabaseTypes
import DatabaseKit
import StorageKit

/// Encodes compiled entities into the single canonical binary storage format.
public enum PersistableStorageCodec {
    public static let formatVersion: UInt16 = 1

    private static let magic: [UInt8] = [0x44, 0x42, 0x52, 0x43]
    private static func storageLimits() throws -> StorageFrameLimits {
        try StorageFrameLimits(
            maximumFrameBytes: 16 * 1_024 * 1_024,
            maximumStringBytes: 1 * 1_024 * 1_024,
            maximumByteStringBytes: 16 * 1_024 * 1_024,
            maximumCollectionCount: 100_000,
            maximumNestingDepth: 64
        )
    }

    public static func encode<Model: Persistable>(
        _ model: Model
    ) throws -> ByteString {
        try requireCompiledSchema(Model.self)
        return try encode(try PersistedModel(model))
    }

    public static func encode(
        _ model: PersistedModel
    ) throws -> ByteString {
        return try encode(
            entity: model.entity,
            fields: model.fields
        )
    }

    /// Measures the canonical frame without allocating its final byte buffer.
    public static func encodedByteCount<Model: Persistable>(
        _ model: Model
    ) throws -> Int {
        try requireCompiledSchema(Model.self)
        return try encodedByteCount(try PersistedModel(model))
    }

    public static func encodedByteCount(
        _ model: PersistedModel
    ) throws -> Int {
        return try PersistableFieldFrameCodec.encodedByteCount(
            magic: magic,
            version: formatVersion,
            entity: model.entity,
            fields: model.fields,
            limits: try storageLimits()
        )
    }

    /// Encodes catalog-validated fields into the canonical DBRC v1 frame.
    public static func encode(
        entity: String,
        fields: [PersistableField]
    ) throws -> ByteString {
        try PersistableFieldFrameCodec.encode(
            magic: magic,
            version: formatVersion,
            entity: entity,
            fields: fields,
            limits: try storageLimits()
        )
    }

    public static func decode<Model: Persistable>(
        _ type: Model.Type,
        from bytes: ByteString
    ) throws -> Model {
        try requireCompiledSchema(type)
        return try decodePersistedModel(
            from: bytes,
            expectedEntity: type.persistableType
        ).decode(as: type)
    }

    /// Decodes a bounded canonical DBRC v1 frame for dynamic catalog tooling.
    public static func decodeFields(
        from bytes: ByteString,
        expectedEntity: String? = nil
    ) throws -> (entity: String, fields: [PersistableField]) {
        try PersistableFieldFrameCodec.decode(
            bytes,
            magic: magic,
            version: formatVersion,
            expectedEntity: expectedEntity,
            limits: try storageLimits()
        )
    }

    public static func decodePersistedModel(
        from bytes: ByteString,
        expectedEntity: String? = nil
    ) throws -> PersistedModel {
        let decoded = try decodeFields(
            from: bytes,
            expectedEntity: expectedEntity
        )
        return try PersistedModel(
            entity: decoded.entity,
            fields: decoded.fields
        )
    }

    private static func requireCompiledSchema<Model: Persistable>(
        _ type: Model.Type
    ) throws {
        guard !type.fieldSchemas.isEmpty else {
            throw PersistableFieldFrameError.missingCompiledSchema(
                entity: type.persistableType
            )
        }
    }

}
