import Core
import DatabaseValue
import DatabaseWire
import StorageKit

/// Shared bounded framing for canonical record fields.
public enum DatabaseRecordFieldFrameCodec {
    public static func encode(
        magic: [UInt8],
        version: UInt16,
        entity: String,
        fields: [DatabaseRecordField],
        limits: DatabaseWireLimits
    ) throws -> Bytes {
        guard magic.count == 4 else {
            throw DatabaseRecordFrameError.invalidMagicLength(actual: magic.count)
        }

        let bytes = try DatabaseWireWriter.encode(limits: limits) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            for byte in magic {
                writer.writeUInt8(byte)
            }
            writer.writeUInt16(version)
            try writer.writeString(entity)
            try writer.writeCount(fields.count)
            for field in fields {
                writer.writeUInt32(field.number)
                try writer.writeString(field.name)
                try writer.writeLengthPrefixed {
                    (writer: inout DatabaseWireWriter) throws(DatabaseWireError) -> Void in
                    try field.value.encode(into: &writer)
                }
            }
        }
        return Bytes(retaining: bytes)
    }

    public static func decode(
        _ bytes: Bytes,
        magic: [UInt8],
        version: UInt16,
        expectedEntity: String? = nil,
        limits: DatabaseWireLimits
    ) throws -> (entity: String, fields: [DatabaseRecordField]) {
        guard magic.count == 4 else {
            throw DatabaseRecordFrameError.invalidMagicLength(actual: magic.count)
        }
        guard bytes.count <= limits.maximumFrameBytes else {
            throw DatabaseRecordFrameError.frameTooLarge(
                actual: bytes.count,
                maximum: limits.maximumFrameBytes
            )
        }

        var reader = DatabaseWireReader(
            DatabaseBytes(retaining: bytes),
            limits: limits
        )
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw DatabaseRecordFrameError.invalidMagic
            }
        }
        let actualVersion = try reader.readUInt16()
        guard actualVersion == version else {
            throw DatabaseRecordFrameError.unsupportedVersion(actualVersion)
        }
        let entity = try reader.readString()
        if let expectedEntity, entity != expectedEntity {
            throw DatabaseRecordFrameError.entityMismatch(
                expected: expectedEntity,
                actual: entity
            )
        }

        let count = try reader.readCount()
        var fields: [DatabaseRecordField] = []
        fields.reserveCapacity(count)
        var fieldNames: Set<String> = []
        var fieldNumbers: Set<UInt32> = []
        for _ in 0..<count {
            let number = try reader.readUInt32()
            let name = try reader.readString()
            guard fieldNames.insert(name).inserted else {
                throw DatabaseRecordFrameError.duplicateFieldName(name)
            }
            guard fieldNumbers.insert(number).inserted else {
                throw DatabaseRecordFrameError.duplicateFieldNumber(number)
            }
            let value = try reader.readLengthPrefixed {
                (reader: inout DatabaseWireReader) throws(DatabaseWireError) -> DatabaseValue in
                try DatabaseValue(from: &reader)
            }
            fields.append(
                DatabaseRecordField(number: number, name: name, value: value)
            )
        }
        try reader.ensureFullyRead()
        return (entity: entity, fields: fields)
    }

    /// Decodes only selected root fields while structurally skipping every
    /// other length-delimited value without materializing it.
    public static func decodeSelected(
        _ bytes: Bytes,
        magic: [UInt8],
        version: UInt16,
        selectedFieldNames: Set<String>,
        expectedEntity: String? = nil,
        limits: DatabaseWireLimits
    ) throws -> (entity: String, fieldsByName: [String: DatabaseValue]) {
        guard magic.count == 4 else {
            throw DatabaseRecordFrameError.invalidMagicLength(actual: magic.count)
        }
        guard bytes.count <= limits.maximumFrameBytes else {
            throw DatabaseRecordFrameError.frameTooLarge(
                actual: bytes.count,
                maximum: limits.maximumFrameBytes
            )
        }

        var reader = DatabaseWireReader(
            DatabaseBytes(retaining: bytes),
            limits: limits
        )
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw DatabaseRecordFrameError.invalidMagic
            }
        }
        let actualVersion = try reader.readUInt16()
        guard actualVersion == version else {
            throw DatabaseRecordFrameError.unsupportedVersion(actualVersion)
        }
        let entity = try reader.readString()
        if let expectedEntity, entity != expectedEntity {
            throw DatabaseRecordFrameError.entityMismatch(
                expected: expectedEntity,
                actual: entity
            )
        }

        let count = try reader.readCount()
        var fieldsByName: [String: DatabaseValue] = [:]
        fieldsByName.reserveCapacity(min(count, selectedFieldNames.count))
        var fieldNames: Set<String> = []
        var fieldNumbers: Set<UInt32> = []
        for _ in 0..<count {
            let number = try reader.readUInt32()
            let name = try reader.readString()
            guard fieldNames.insert(name).inserted else {
                throw DatabaseRecordFrameError.duplicateFieldName(name)
            }
            guard fieldNumbers.insert(number).inserted else {
                throw DatabaseRecordFrameError.duplicateFieldNumber(number)
            }
            if selectedFieldNames.contains(name) {
                fieldsByName[name] = try reader.readLengthPrefixed {
                    (reader: inout DatabaseWireReader) throws(DatabaseWireError) -> DatabaseValue in
                    try DatabaseValue(from: &reader)
                }
            } else {
                // readBytes advances over the same length-delimited payload and
                // returns a retained view; assigning to `_` releases it without
                // allocating an Array, Data, String, or DatabaseValue tree.
                _ = try reader.readBytes()
            }
        }
        try reader.ensureFullyRead()
        return (entity: entity, fieldsByName: fieldsByName)
    }
}
