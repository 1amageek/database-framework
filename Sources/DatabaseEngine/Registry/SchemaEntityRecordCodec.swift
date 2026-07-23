import Core
import DatabaseValue
import DatabaseWire
import StorageKit

enum SchemaEntityRecordCodec {
    private static let magic: UInt32 = 0x4353_4244
    private static let version: UInt16 = 1

    static func encode(
        _ entity: Schema.Entity,
        limits: DatabaseWireLimits = .default
    ) throws -> Bytes {
        try validate(entity)
        let encoded = try DatabaseWireWriter.encodeThrowing(limits: limits) {
            writer in
            writer.writeUInt32(magic)
            writer.writeUInt16(version)
            try write(entity, into: &writer)
        }
        return Bytes(retaining: encoded)
    }

    static func decode(
        _ bytes: Bytes,
        limits: DatabaseWireLimits = .default
    ) throws -> Schema.Entity {
        var reader = DatabaseWireReader(
            DatabaseBytes(retaining: bytes),
            limits: limits
        )
        let decodedMagic = try reader.readUInt32()
        guard decodedMagic == magic else {
            throw SchemaEntityRecordCodecError.invalidMagic(decodedMagic)
        }
        let decodedVersion = try reader.readUInt16()
        guard decodedVersion == version else {
            throw SchemaEntityRecordCodecError.unsupportedVersion(decodedVersion)
        }
        let entity = try readEntity(from: &reader)
        try reader.ensureFullyRead()
        try validate(entity)
        return entity
    }

    private static func write(
        _ entity: Schema.Entity,
        into writer: inout DatabaseWireWriter
    ) throws {
        try writer.writeString(entity.name)
        try writer.writeCount(entity.fields.count)
        for field in entity.fields {
            try write(field, into: &writer)
        }

        try writer.writeCount(entity.directoryComponents.count)
        for component in entity.directoryComponents {
            switch component {
            case .staticPath(let value):
                writer.writeUInt8(0)
                try writer.writeString(value)
            case .dynamicField(let fieldName):
                writer.writeUInt8(1)
                try writer.writeString(fieldName)
            }
        }
        try writer.writeString(entity.directoryLayer.rawValue)

        try writer.writeCount(entity.indexes.count)
        for index in entity.indexes {
            try write(index, into: &writer)
        }

        try writeStringArrayMap(entity.enumMetadata, into: &writer)
        try writer.writeOptionalString(entity.ontologyClassIRI)
        try writer.writeOptionalString(entity.objectPropertyIRI)
        try writer.writeOptionalString(entity.objectPropertyFromField)
        try writer.writeOptionalString(entity.objectPropertyToField)
        try writeOptionalStringArray(entity.dataPropertyIRIs, into: &writer)
    }

    private static func write(
        _ field: FieldSchema,
        into writer: inout DatabaseWireWriter
    ) throws {
        guard let fieldNumber = Int64(exactly: field.fieldNumber) else {
            throw SchemaEntityRecordCodecError.invalidInteger(
                field: "fieldNumber",
                value: field.fieldNumber < 0 ? Int64.min : Int64.max
            )
        }
        try writer.writeString(field.name)
        writer.writeInt64(fieldNumber)
        try writer.writeString(field.type.rawValue)
        writer.writeBool(field.isOptional)
        writer.writeBool(field.isArray)
        try writer.writeOptionalString(field.referenceTargetEntity)
    }

    private static func write(
        _ index: IndexDescriptorMetadata,
        into writer: inout DatabaseWireWriter
    ) throws {
        try writer.writeString(index.name)
        try writer.writeString(index.kind.identifier)
        try writer.writeString(index.kind.subspaceStructure.rawValue)
        try writeStringArray(index.kind.fieldNames, into: &writer)
        try writeMetadataMap(index.kind.metadata, into: &writer)
        try writeMetadataMap(index.commonMetadata, into: &writer)
    }

    private static func writeMetadataMap(
        _ values: [String: IndexMetadataValue],
        into writer: inout DatabaseWireWriter
    ) throws {
        let keys = values.keys.sorted()
        try writer.writeCount(keys.count)
        for key in keys {
            guard let value = values[key] else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "metadata key '\(key)' disappeared during encoding"
                )
            }
            try writer.writeString(key)
            try write(value, into: &writer)
        }
    }

    private static func write(
        _ value: IndexMetadataValue,
        into writer: inout DatabaseWireWriter
    ) throws {
        switch value {
        case .string(let value):
            writer.writeUInt8(0)
            try writer.writeString(value)
        case .int(let value):
            guard let encoded = Int64(exactly: value) else {
                throw SchemaEntityRecordCodecError.invalidInteger(
                    field: "indexMetadata",
                    value: value < 0 ? Int64.min : Int64.max
                )
            }
            writer.writeUInt8(1)
            writer.writeInt64(encoded)
        case .double(let value):
            writer.writeUInt8(2)
            writer.writeDouble(value)
        case .bool(let value):
            writer.writeUInt8(3)
            writer.writeBool(value)
        case .stringArray(let values):
            writer.writeUInt8(4)
            try writeStringArray(values, into: &writer)
        case .intArray(let values):
            writer.writeUInt8(5)
            try writer.writeCount(values.count)
            for value in values {
                guard let encoded = Int64(exactly: value) else {
                    throw SchemaEntityRecordCodecError.invalidInteger(
                        field: "indexMetadataArray",
                        value: value < 0 ? Int64.min : Int64.max
                    )
                }
                writer.writeInt64(encoded)
            }
        case .rdfTerm(let value):
            writer.writeUInt8(6)
            try value.encode(into: &writer)
        }
    }

    private static func writeStringArrayMap(
        _ values: [String: [String]],
        into writer: inout DatabaseWireWriter
    ) throws {
        let keys = values.keys.sorted()
        try writer.writeCount(keys.count)
        for key in keys {
            guard let value = values[key] else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "enum metadata key '\(key)' disappeared during encoding"
                )
            }
            try writer.writeString(key)
            try writeStringArray(value, into: &writer)
        }
    }

    private static func writeStringArray(
        _ values: [String],
        into writer: inout DatabaseWireWriter
    ) throws {
        try writer.writeCount(values.count)
        for value in values {
            try writer.writeString(value)
        }
    }

    private static func writeOptionalStringArray(
        _ values: [String]?,
        into writer: inout DatabaseWireWriter
    ) throws {
        guard let values else {
            writer.writeBool(false)
            return
        }
        writer.writeBool(true)
        try writeStringArray(values, into: &writer)
    }

    private static func readEntity(
        from reader: inout DatabaseWireReader
    ) throws -> Schema.Entity {
        let name = try reader.readString()

        let fieldCount = try reader.readCount()
        var fields: [FieldSchema] = []
        fields.reserveCapacity(fieldCount)
        for _ in 0..<fieldCount {
            fields.append(try readField(from: &reader))
        }

        let directoryCount = try reader.readCount()
        var directoryComponents: [DirectoryPathComponent] = []
        directoryComponents.reserveCapacity(directoryCount)
        for _ in 0..<directoryCount {
            let tag = try reader.readUInt8()
            let value = try reader.readString()
            switch tag {
            case 0:
                directoryComponents.append(.staticPath(value))
            case 1:
                directoryComponents.append(.dynamicField(fieldName: value))
            default:
                throw SchemaEntityRecordCodecError.invalidEnum(
                    type: "directory component tag",
                    value: String(tag)
                )
            }
        }

        let directoryLayerValue = try reader.readString()
        guard let directoryLayer = DirectoryLayer(rawValue: directoryLayerValue) else {
            throw SchemaEntityRecordCodecError.invalidEnum(
                type: "DirectoryLayer",
                value: directoryLayerValue
            )
        }

        let indexCount = try reader.readCount()
        var indexes: [IndexDescriptorMetadata] = []
        indexes.reserveCapacity(indexCount)
        for _ in 0..<indexCount {
            indexes.append(try readIndex(from: &reader))
        }

        return Schema.Entity(
            name: name,
            fields: fields,
            directoryComponents: directoryComponents,
            directoryLayer: directoryLayer,
            indexes: indexes,
            enumMetadata: try readStringArrayMap(
                context: "enum metadata",
                from: &reader
            ),
            ontologyClassIRI: try reader.readOptionalString(),
            objectPropertyIRI: try reader.readOptionalString(),
            objectPropertyFromField: try reader.readOptionalString(),
            objectPropertyToField: try reader.readOptionalString(),
            dataPropertyIRIs: try readOptionalStringArray(from: &reader)
        )
    }

    private static func readField(
        from reader: inout DatabaseWireReader
    ) throws -> FieldSchema {
        let name = try reader.readString()
        let encodedFieldNumber = try reader.readInt64()
        guard let fieldNumber = Int(exactly: encodedFieldNumber) else {
            throw SchemaEntityRecordCodecError.invalidInteger(
                field: "fieldNumber",
                value: encodedFieldNumber
            )
        }
        let fieldTypeValue = try reader.readString()
        guard let fieldType = FieldSchemaType(rawValue: fieldTypeValue) else {
            throw SchemaEntityRecordCodecError.invalidEnum(
                type: "FieldSchemaType",
                value: fieldTypeValue
            )
        }
        return FieldSchema(
            name: name,
            fieldNumber: fieldNumber,
            type: fieldType,
            isOptional: try reader.readBool(),
            isArray: try reader.readBool(),
            referenceTargetEntity: try reader.readOptionalString()
        )
    }

    private static func readIndex(
        from reader: inout DatabaseWireReader
    ) throws -> IndexDescriptorMetadata {
        let name = try reader.readString()
        let identifier = try reader.readString()
        let subspaceValue = try reader.readString()
        guard let subspace = SubspaceStructure(rawValue: subspaceValue) else {
            throw SchemaEntityRecordCodecError.invalidEnum(
                type: "SubspaceStructure",
                value: subspaceValue
            )
        }
        let kind = IndexKindMetadata(
            identifier: identifier,
            subspaceStructure: subspace,
            fieldNames: try readStringArray(from: &reader),
            metadata: try readMetadataMap(
                context: "index kind metadata",
                from: &reader
            )
        )
        return IndexDescriptorMetadata(
            name: name,
            kind: kind,
            commonMetadata: try readMetadataMap(
                context: "index common metadata",
                from: &reader
            )
        )
    }

    private static func readMetadataMap(
        context: String,
        from reader: inout DatabaseWireReader
    ) throws -> [String: IndexMetadataValue] {
        let count = try reader.readCount()
        var result: [String: IndexMetadataValue] = [:]
        result.reserveCapacity(count)
        for _ in 0..<count {
            let key = try reader.readString()
            guard result[key] == nil else {
                throw SchemaEntityRecordCodecError.duplicateMapKey(
                    context: context,
                    key: key
                )
            }
            result[key] = try readMetadata(from: &reader)
        }
        return result
    }

    private static func readMetadata(
        from reader: inout DatabaseWireReader
    ) throws -> IndexMetadataValue {
        switch try reader.readUInt8() {
        case 0:
            return .string(try reader.readString())
        case 1:
            let encoded = try reader.readInt64()
            guard let value = Int(exactly: encoded) else {
                throw SchemaEntityRecordCodecError.invalidInteger(
                    field: "indexMetadata",
                    value: encoded
                )
            }
            return .int(value)
        case 2:
            return .double(try reader.readDouble())
        case 3:
            return .bool(try reader.readBool())
        case 4:
            return .stringArray(try readStringArray(from: &reader))
        case 5:
            let count = try reader.readCount()
            var values: [Int] = []
            values.reserveCapacity(count)
            for _ in 0..<count {
                let encoded = try reader.readInt64()
                guard let value = Int(exactly: encoded) else {
                    throw SchemaEntityRecordCodecError.invalidInteger(
                        field: "indexMetadataArray",
                        value: encoded
                    )
                }
                values.append(value)
            }
            return .intArray(values)
        case 6:
            return .rdfTerm(try DatabaseRDFTerm(from: &reader))
        case let tag:
            throw SchemaEntityRecordCodecError.invalidMetadataTag(tag)
        }
    }

    private static func readStringArrayMap(
        context: String,
        from reader: inout DatabaseWireReader
    ) throws -> [String: [String]] {
        let count = try reader.readCount()
        var result: [String: [String]] = [:]
        result.reserveCapacity(count)
        for _ in 0..<count {
            let key = try reader.readString()
            guard result[key] == nil else {
                throw SchemaEntityRecordCodecError.duplicateMapKey(
                    context: context,
                    key: key
                )
            }
            result[key] = try readStringArray(from: &reader)
        }
        return result
    }

    private static func readStringArray(
        from reader: inout DatabaseWireReader
    ) throws -> [String] {
        let count = try reader.readCount()
        var values: [String] = []
        values.reserveCapacity(count)
        for _ in 0..<count {
            values.append(try reader.readString())
        }
        return values
    }

    private static func readOptionalStringArray(
        from reader: inout DatabaseWireReader
    ) throws -> [String]? {
        guard try reader.readBool() else { return nil }
        return try readStringArray(from: &reader)
    }

    private static func validate(_ entity: Schema.Entity) throws {
        guard !entity.name.isEmpty else {
            throw SchemaEntityRecordCodecError.invalidDefinition(
                "entity name must not be empty"
            )
        }

        var fieldNames = Set<String>()
        var fieldNumbers = Set<Int>()
        for field in entity.fields {
            guard !field.name.isEmpty else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "field name must not be empty"
                )
            }
            guard field.fieldNumber > 0 else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "field '\(field.name)' must have a positive field number"
                )
            }
            guard fieldNames.insert(field.name).inserted else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "field name '\(field.name)' occurs more than once"
                )
            }
            guard fieldNumbers.insert(field.fieldNumber).inserted else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "field number \(field.fieldNumber) occurs more than once"
                )
            }
        }

        var dynamicNames = Set<String>()
        for component in entity.directoryComponents {
            switch component {
            case .staticPath(let value):
                guard !value.isEmpty else {
                    throw SchemaEntityRecordCodecError.invalidDefinition(
                        "static directory components must not be empty"
                    )
                }
            case .dynamicField(let fieldName):
                guard fieldNames.contains(fieldName) else {
                    throw SchemaEntityRecordCodecError.invalidDefinition(
                        "directory field '\(fieldName)' is not in the entity schema"
                    )
                }
                guard dynamicNames.insert(fieldName).inserted else {
                    throw SchemaEntityRecordCodecError.invalidDefinition(
                        "directory field '\(fieldName)' occurs more than once"
                    )
                }
            }
        }
        if entity.directoryLayer == .partition, dynamicNames.isEmpty {
            throw SchemaEntityRecordCodecError.invalidDefinition(
                "partition directory layer requires a dynamic field"
            )
        }

        var indexNames = Set<String>()
        for index in entity.indexes {
            guard !index.name.isEmpty, !index.kind.identifier.isEmpty else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "index name and kind identifier must not be empty"
                )
            }
            guard indexNames.insert(index.name).inserted else {
                throw SchemaEntityRecordCodecError.invalidDefinition(
                    "index name '\(index.name)' occurs more than once"
                )
            }
        }
    }
}
