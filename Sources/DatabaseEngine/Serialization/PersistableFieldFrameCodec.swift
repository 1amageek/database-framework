import DatabaseKit
import DatabaseTypes
import StorageKit

/// Shared bounded framing for canonical entity fields.
public enum PersistableFieldFrameCodec {
    package static func retainedFootprint(
        of model: PersistedModel
    ) throws -> UInt64 {
        var footprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<PersistedModel>.stride + 64)
                + UInt64(model.entity.utf8.count)
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<PersistableField>.stride + 16)
            ).multiplied(by: UInt64(model.fields.count))
        )
        for field in model.fields {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(field.name.utf8.count)
                )
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: try StorageValueDecoder.retainedFootprint(
                        of: field.value
                    )
                )
            )
        }
        return footprint.bytes
    }

    package static func decodedFootprint(
        _ bytes: ByteString,
        magic: [UInt8],
        version: UInt16,
        expectedEntity: String? = nil,
        limits: StorageFrameLimits = .default
    ) throws -> (
        retainedByteCount: UInt64,
        transientByteCount: UInt64
    ) {
        guard magic.count == 4 else {
            throw PersistableFieldFrameError.invalidMagicLength(
                actual: magic.count
            )
        }
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw PersistableFieldFrameError.invalidMagic
            }
        }
        let actualVersion = try reader.readUInt16()
        guard actualVersion == version else {
            throw PersistableFieldFrameError.unsupportedVersion(actualVersion)
        }
        let entityBytes = try reader.readValidatedStringBytes()
        if let expectedEntity,
           !entityBytes.elementsEqual(expectedEntity.utf8) {
            throw PersistableFieldFrameError.entityMismatch(
                expected: expectedEntity,
                actual: String(decoding: entityBytes, as: UTF8.self)
            )
        }
        let count = try reader.readCount()
        var footprint = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<PersistedModel>.stride + 64)
                + UInt64(entityBytes.count)
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<PersistableField>.stride + 16)
            ).multiplied(by: UInt64(count))
        )
        for _ in 0..<count {
            _ = try reader.readUInt32()
            let nameBytes = try reader.readValidatedStringBytes()
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(nameBytes.count)
                )
            )
            let valueFootprint = try reader.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(
                    StorageFrameError
                ) -> UInt64 in
                try StorageValueDecoder.retainedFootprint(from: &child)
            }
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(bytes: valueFootprint)
            )
        }
        try reader.ensureFullyRead()
        return (
            retainedByteCount: footprint.bytes,
            transientByteCount: try PersistedModelAdmissionFootprint
                .validationScratchByteCount(fieldCount: count)
        )
    }

    public static func encode(
        magic: [UInt8],
        version: UInt16,
        entity: String,
        fields: [PersistableField],
        limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        guard magic.count == 4 else {
            throw PersistableFieldFrameError.invalidMagicLength(actual: magic.count)
        }

        let bytes = try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            try write(
                magic: magic,
                version: version,
                entity: entity,
                fields: fields,
                to: &writer
            )
        }
        return bytes
    }

    public static func encodedByteCount(
        magic: [UInt8],
        version: UInt16,
        entity: String,
        fields: [PersistableField],
        limits: StorageFrameLimits = .default
    ) throws -> Int {
        guard magic.count == 4 else {
            throw PersistableFieldFrameError.invalidMagicLength(
                actual: magic.count
            )
        }
        return try StorageFrameEncoder.measure(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            try write(
                magic: magic,
                version: version,
                entity: entity,
                fields: fields,
                to: &writer
            )
        }
    }

    private static func write(
        magic: [UInt8],
        version: UInt16,
        entity: String,
        fields: [PersistableField],
        to writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
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
                (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
                try StorageValueEncoder.write(
                    field.value,
                    into: &writer
                )
            }
        }
    }

    public static func decode(
        _ bytes: ByteString,
        magic: [UInt8],
        version: UInt16,
        expectedEntity: String? = nil,
        limits: StorageFrameLimits = .default
    ) throws -> (entity: String, fields: [PersistableField]) {
        guard magic.count == 4 else {
            throw PersistableFieldFrameError.invalidMagicLength(actual: magic.count)
        }
        guard bytes.count <= limits.maximumFrameBytes else {
            throw PersistableFieldFrameError.frameTooLarge(
                actual: bytes.count,
                maximum: limits.maximumFrameBytes
            )
        }

        var reader = try StorageFrameDecoder(
            bytes,
            limits: limits
        )
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw PersistableFieldFrameError.invalidMagic
            }
        }
        let actualVersion = try reader.readUInt16()
        guard actualVersion == version else {
            throw PersistableFieldFrameError.unsupportedVersion(actualVersion)
        }
        let entity = try reader.readString()
        if let expectedEntity, entity != expectedEntity {
            throw PersistableFieldFrameError.entityMismatch(
                expected: expectedEntity,
                actual: entity
            )
        }

        let count = try reader.readCount()
        var fields: [PersistableField] = []
        fields.reserveCapacity(count)
        var fieldNames: Set<String> = []
        var fieldNumbers: Set<UInt32> = []
        for _ in 0..<count {
            let number = try reader.readUInt32()
            let name = try reader.readString()
            guard fieldNames.insert(name).inserted else {
                throw PersistableFieldFrameError.duplicateFieldName(name)
            }
            guard fieldNumbers.insert(number).inserted else {
                throw PersistableFieldFrameError.duplicateFieldNumber(number)
            }
            let value = try reader.readLengthPrefixed {
                (reader: inout StorageFrameDecoder) throws(
                    StorageFrameError
                ) in
                try StorageValueDecoder.read(from: &reader)
            }
            fields.append(
                try PersistableField(
                    number: number,
                    name: name,
                    value: value
                )
            )
        }
        try reader.ensureFullyRead()
        return (entity: entity, fields: fields)
    }

    /// Decodes only selected root fields while structurally skipping every
    /// other length-delimited value without materializing it.
    public static func decodeSelected(
        _ bytes: ByteString,
        magic: [UInt8],
        version: UInt16,
        selectedFieldNames: Set<String>,
        expectedEntity: String? = nil,
        limits: StorageFrameLimits = .default
    ) throws -> (entity: String, fieldsByName: [String: FieldValue]) {
        guard magic.count == 4 else {
            throw PersistableFieldFrameError.invalidMagicLength(actual: magic.count)
        }
        guard bytes.count <= limits.maximumFrameBytes else {
            throw PersistableFieldFrameError.frameTooLarge(
                actual: bytes.count,
                maximum: limits.maximumFrameBytes
            )
        }

        var reader = try StorageFrameDecoder(
            bytes,
            limits: limits
        )
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw PersistableFieldFrameError.invalidMagic
            }
        }
        let actualVersion = try reader.readUInt16()
        guard actualVersion == version else {
            throw PersistableFieldFrameError.unsupportedVersion(actualVersion)
        }
        let entity = try reader.readString()
        if let expectedEntity, entity != expectedEntity {
            throw PersistableFieldFrameError.entityMismatch(
                expected: expectedEntity,
                actual: entity
            )
        }

        let count = try reader.readCount()
        var fieldsByName: [String: FieldValue] = [:]
        fieldsByName.reserveCapacity(min(count, selectedFieldNames.count))
        var fieldNames: Set<String> = []
        var fieldNumbers: Set<UInt32> = []
        for _ in 0..<count {
            let number = try reader.readUInt32()
            let name = try reader.readString()
            guard fieldNames.insert(name).inserted else {
                throw PersistableFieldFrameError.duplicateFieldName(name)
            }
            guard fieldNumbers.insert(number).inserted else {
                throw PersistableFieldFrameError.duplicateFieldNumber(number)
            }
            if selectedFieldNames.contains(name) {
                fieldsByName[name] = try reader.readLengthPrefixed {
                    (reader: inout StorageFrameDecoder) throws(
                        StorageFrameError
                    ) in
                    try StorageValueDecoder.read(from: &reader)
                }
            } else {
                // readBytes advances over the same length-delimited payload and
                // returns a retained view; assigning to `_` releases it without
                // allocating an Array, Data, String, or FieldValue tree.
                _ = try reader.readBytes()
            }
        }
        try reader.ensureFullyRead()
        return (entity: entity, fieldsByName: fieldsByName)
    }
}
