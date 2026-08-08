import DatabaseKit
import DatabaseTypes
import StorageKit

/// Persists the complete semantic schema declaration owned by `DatabaseKit`.
///
/// This is an engine storage format, not the client/server wire protocol.
enum SchemaEntityEntryCodec {
    private static let magic: UInt32 = 0x4353_4244
    private static let version: UInt16 = 2

    static func encode(
        _ entity: Schema.Entity,
        limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        let encoded = try StorageFrameEncoder.encode(limits: limits) {
            writer throws(StorageFrameError) in
            writer.writeUInt32(magic)
            writer.writeUInt16(version)
            try writeCanonical(entity, into: &writer)
        }
        return encoded
    }

    static func decode(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws -> Schema.Entity {
        var reader = try StorageFrameDecoder(
            bytes,
            limits: limits
        )
        let decodedMagic = try reader.readUInt32()
        guard decodedMagic == magic else {
            throw SchemaEntityEntryCodecError.invalidMagic(decodedMagic)
        }
        let decodedVersion = try reader.readUInt16()
        guard decodedVersion == version else {
            throw SchemaEntityEntryCodecError.unsupportedVersion(decodedVersion)
        }
        let entity = try readEntity(from: &reader)
        try reader.ensureFullyRead()
        return entity
    }

    static func writeCanonical(
        _ entity: Schema.Entity,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(entity.name)
        try write(entity.identifierType, into: &writer)

        let fields = entity.fields.sorted {
            ($0.fieldNumber, $0.name) < ($1.fieldNumber, $1.name)
        }
        try writer.writeCount(fields.count)
        for field in fields {
            try write(field, into: &writer)
        }

        try writeDirectory(
            entity.directoryComponents,
            layer: entity.directoryLayer,
            into: &writer
        )

        let indexes = entity.indexes.sorted { $0.name < $1.name }
        try writer.writeCount(indexes.count)
        for index in indexes {
            try write(index, into: &writer)
        }

        let relationships = entity.relationships.sorted {
            ($0.propertyFieldNumber, $0.propertyName)
                < ($1.propertyFieldNumber, $1.propertyName)
        }
        try writer.writeCount(relationships.count)
        for relationship in relationships {
            try write(relationship, into: &writer)
        }

        let accessRules = entity.fieldAccessRules.sorted {
            ($0.field.number, $0.field.name)
                < ($1.field.number, $1.field.name)
        }
        try writer.writeCount(accessRules.count)
        for rule in accessRules {
            try write(rule, into: &writer)
        }

        try writeStringArrayMap(entity.enumMetadata, into: &writer)
        try write(entity.ontology, into: &writer)
        try write(entity.polymorphicMembership, into: &writer)
    }

    static func writeDirectory(
        _ components: [DirectoryPathComponent],
        layer: DirectoryLayer,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(components.count)
        for component in components {
            switch component {
            case .staticPath(let value):
                writer.writeUInt8(0)
                try writer.writeString(value)
            case .dynamicField(let fieldName):
                writer.writeUInt8(1)
                try writer.writeString(fieldName)
            }
        }
        try writer.writeString(layer.rawValue)
    }

    static func writeStringArray(
        _ values: [String],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(values.count)
        for value in values {
            try writer.writeString(value)
        }
    }

    private static func write(
        _ identifierType: PersistableIdentifierType,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch identifierType {
        case .bool: writer.writeUInt8(0)
        case .int8: writer.writeUInt8(1)
        case .int16: writer.writeUInt8(2)
        case .int32: writer.writeUInt8(3)
        case .int64: writer.writeUInt8(4)
        case .uint8: writer.writeUInt8(5)
        case .uint16: writer.writeUInt8(6)
        case .uint32: writer.writeUInt8(7)
        case .uint64: writer.writeUInt8(8)
        case .string: writer.writeUInt8(9)
        case .bytes: writer.writeUInt8(10)
        case .uuid: writer.writeUInt8(11)
        case .composite(let components):
            writer.writeUInt8(12)
            try writer.writeCount(components.count)
            for component in components {
                try writer.writeLengthPrefixed {
                    (writer: inout StorageFrameEncoder) throws(
                        StorageFrameError
                    ) in
                    try write(component, into: &writer)
                }
            }
        }
    }

    private static func write(
        _ field: FieldSchema,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(field.name)
        guard let fieldNumber = Int64(exactly: field.fieldNumber) else {
            throw .integerOutOfRange
        }
        writer.writeInt64(fieldNumber)
        try writer.writeString(field.type.rawValue)
        writer.writeBool(field.isOptional)
        writer.writeBool(field.isArray)
        try writer.writeOptionalString(field.referenceTargetEntity)
    }

    static func write(
        _ index: IndexDescriptorMetadata,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(index.entityName)
        try writer.writeString(index.name)
        try writer.writeString(index.kind.identifier)
        try writer.writeString(index.kind.subspaceStructure.rawValue)
        try writer.writeCount(index.kind.fields.count)
        for field in index.kind.fields {
            try write(field.identity, into: &writer)
            try writer.writeString(field.order.rawValue)
        }
        try writeFieldValueMap(index.kind.metadata, into: &writer)
        writer.writeBool(index.commonOptions.unique)
        writer.writeBool(index.commonOptions.sparse)
        try writeStringMap(index.commonOptions.metadata, into: &writer)
        try writeStringArray(index.storedFieldNames, into: &writer)
    }

    private static func write(
        _ relationship: RelationshipDescriptor,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(relationship.ownerTypeName)
        try writer.writeString(relationship.propertyName)
        writer.writeUInt32(relationship.propertyFieldNumber)
        try writer.writeString(relationship.relatedTypeName)
        try writer.writeString(relationship.cardinality.rawValue)
        try writer.writeString(relationship.deleteRule.rawValue)
    }

    private static func write(
        _ rule: FieldAccessRule,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try write(rule.field, into: &writer)
        try write(rule.read, into: &writer)
        try write(rule.write, into: &writer)
    }

    private static func write(
        _ identity: FieldIdentity,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(identity.name)
        guard let number = Int64(exactly: identity.number) else {
            throw .integerOutOfRange
        }
        writer.writeInt64(number)
    }

    private static func write(
        _ access: FieldAccessLevel,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch access {
        case .public:
            writer.writeUInt8(0)
        case .authenticated:
            writer.writeUInt8(1)
        case .roles(let roles):
            writer.writeUInt8(2)
            try writeStringArray(roles.sorted(), into: &writer)
        }
    }

    private static func write(
        _ ontology: OntologyBinding?,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        guard let ontology else {
            writer.writeUInt8(0)
            return
        }
        switch ontology {
        case .owlClass(let iri, let properties):
            writer.writeUInt8(1)
            try writer.writeString(iri)
            try write(properties, into: &writer)
        case .owlObjectProperty(
            let iri,
            let fromField,
            let toField,
            let properties
        ):
            writer.writeUInt8(2)
            try writer.writeString(iri)
            try writer.writeString(fromField)
            try writer.writeString(toField)
            try write(properties, into: &writer)
        }
    }

    private static func write(
        _ properties: [OWLDataPropertyDescriptor],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(properties.count)
        for property in properties {
            try writer.writeString(property.name)
            try writer.writeString(property.fieldName)
            try writer.writeString(property.iri)
            try writer.writeOptionalString(property.label)
            try writer.writeOptionalString(property.targetTypeName)
            try writer.writeOptionalString(property.targetFieldName)
        }
    }

    private static func write(
        _ membership: PolymorphicMembership?,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        guard let membership else {
            writer.writeBool(false)
            return
        }
        writer.writeBool(true)
        try writer.writeString(membership.identifier)
        try writeDirectory(
            membership.directoryComponents,
            layer: membership.directoryLayer,
            into: &writer
        )
        try writer.writeCount(membership.indexes.count)
        for index in membership.indexes {
            try writer.writeString(index.name)
            try write(index.definition, into: &writer)
            try writer.writeCount(index.fields.count)
            for field in index.fields {
                try writer.writeString(field.name)
                try writer.writeString(field.order.rawValue)
            }
            writer.writeBool(index.commonOptions.unique)
            writer.writeBool(index.commonOptions.sparse)
            try writeStringMap(index.commonOptions.metadata, into: &writer)
            try writeStringArray(index.storedFieldNames, into: &writer)
        }
    }

    private static func write(
        _ definition: IndexDefinition,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch definition {
        case .scalar: writer.writeUInt8(0)
        case .count: writer.writeUInt8(1)
        case .sum: writer.writeUInt8(2)
        case .minimum: writer.writeUInt8(3)
        case .maximum: writer.writeUInt8(4)
        case .average: writer.writeUInt8(5)
        case .version(let strategy):
            writer.writeUInt8(6)
            switch strategy {
            case .keepAll:
                writer.writeUInt8(0)
            case .keepLast(let count):
                writer.writeUInt8(1)
                try write(count, into: &writer)
            case .keepForDuration(let duration):
                writer.writeUInt8(2)
                writer.writeInt64(duration.seconds)
                writer.writeUInt32(duration.nanoseconds)
            }
        case .countUpdates: writer.writeUInt8(7)
        case .countNotNull: writer.writeUInt8(8)
        case .bitmap: writer.writeUInt8(9)
        case .timeWindowLeaderboard(let window, let windowCount):
            writer.writeUInt8(10)
            switch window {
            case .hourly: writer.writeUInt8(0)
            case .daily: writer.writeUInt8(1)
            case .weekly: writer.writeUInt8(2)
            case .monthly: writer.writeUInt8(3)
            case .custom(let duration):
                writer.writeUInt8(4)
                writer.writeDouble(duration)
            }
            try write(windowCount, into: &writer)
        case .distinct(let precision):
            writer.writeUInt8(11)
            try write(precision, into: &writer)
        case .percentile(let compression):
            writer.writeUInt8(12)
            writer.writeDouble(compression)
        case .vector(let dimensions, let metric):
            writer.writeUInt8(13)
            try write(dimensions, into: &writer)
            try writer.writeString(metric.rawValue)
        case .fullText(
            let tokenizer,
            let storePositions,
            let ngramSize,
            let minTermLength
        ):
            writer.writeUInt8(14)
            try writer.writeString(tokenizer.rawValue)
            writer.writeBool(storePositions)
            try write(ngramSize, into: &writer)
            try write(minTermLength, into: &writer)
        case .spatial(let encoding, let level):
            writer.writeUInt8(15)
            try writer.writeString(encoding.rawValue)
            try write(level, into: &writer)
        case .rank:
            writer.writeUInt8(16)
        case .permuted(let pattern):
            writer.writeUInt8(17)
            switch pattern {
            case .identity(let size):
                writer.writeUInt8(0)
                try write(size, into: &writer)
            case .swapping(let first, let second, let size):
                writer.writeUInt8(1)
                try write(first, into: &writer)
                try write(second, into: &writer)
                try write(size, into: &writer)
            case .ordering(let indices):
                writer.writeUInt8(2)
                try writer.writeCount(indices.count)
                for index in indices {
                    try write(index, into: &writer)
                }
            }
        case .propertyGraph(let strategy, let label):
            writer.writeUInt8(18)
            try writer.writeString(strategy.rawValue)
            writer.writeUInt8(label == .field ? 0 : 1)
        case .autocomplete(let minPrefixLength, let maxPrefixLength):
            writer.writeUInt8(19)
            try write(minPrefixLength, into: &writer)
            try write(maxPrefixLength, into: &writer)
        case .rdfDataset:
            writer.writeUInt8(20)
        }
    }

    private static func write(
        _ value: Int,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        guard let encoded = Int64(exactly: value) else {
            throw .integerOutOfRange
        }
        writer.writeInt64(encoded)
    }

    private static func writeFieldValueMap(
        _ values: [String: FieldValue],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        let keys = values.keys.sorted()
        try writer.writeCount(keys.count)
        for key in keys {
            guard let value = values[key] else {
                throw .invalidValue
            }
            try writer.writeString(key)
            try writer.writeLengthPrefixed {
                (writer: inout StorageFrameEncoder) throws(
                    StorageFrameError
                ) in
                try StorageValueEncoder.write(value, into: &writer)
            }
        }
    }

    private static func writeStringMap(
        _ values: [String: String],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        let keys = values.keys.sorted()
        try writer.writeCount(keys.count)
        for key in keys {
            guard let value = values[key] else {
                throw .invalidValue
            }
            try writer.writeString(key)
            try writer.writeString(value)
        }
    }

    private static func writeStringArrayMap(
        _ values: [String: [String]],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        let keys = values.keys.sorted()
        try writer.writeCount(keys.count)
        for key in keys {
            guard let value = values[key] else {
                throw .invalidValue
            }
            try writer.writeString(key)
            try writeStringArray(value, into: &writer)
        }
    }

    private static func readEntity(
        from reader: inout StorageFrameDecoder
    ) throws -> Schema.Entity {
        let name = try reader.readString()
        let identifierType = try readIdentifierType(from: &reader)

        let fieldCount = try reader.readCount()
        var fields: [FieldSchema] = []
        fields.reserveCapacity(fieldCount)
        for _ in 0..<fieldCount {
            fields.append(try readField(from: &reader))
        }

        let directory = try readDirectory(from: &reader)

        let indexCount = try reader.readCount()
        var indexes: [IndexDescriptorMetadata] = []
        indexes.reserveCapacity(indexCount)
        for _ in 0..<indexCount {
            indexes.append(try readIndex(from: &reader))
        }

        let relationshipCount = try reader.readCount()
        var relationships: [RelationshipDescriptor] = []
        relationships.reserveCapacity(relationshipCount)
        for _ in 0..<relationshipCount {
            relationships.append(try readRelationship(from: &reader))
        }

        let accessRuleCount = try reader.readCount()
        var accessRules: [FieldAccessRule] = []
        accessRules.reserveCapacity(accessRuleCount)
        for _ in 0..<accessRuleCount {
            accessRules.append(try readAccessRule(from: &reader))
        }

        return try Schema.Entity(
            name: name,
            identifierType: identifierType,
            fields: fields,
            directoryComponents: directory.components,
            directoryLayer: directory.layer,
            indexes: indexes,
            relationships: relationships,
            fieldAccessRules: accessRules,
            enumMetadata: try readStringArrayMap(
                context: "enum metadata",
                from: &reader
            ),
            ontology: try readOntology(from: &reader),
            polymorphicMembership: try readPolymorphicMembership(from: &reader)
        )
    }

    private static func readIdentifierType(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> PersistableIdentifierType {
        switch try reader.readUInt8() {
        case 0: return .bool
        case 1: return .int8
        case 2: return .int16
        case 3: return .int32
        case 4: return .int64
        case 5: return .uint8
        case 6: return .uint16
        case 7: return .uint32
        case 8: return .uint64
        case 9: return .string
        case 10: return .bytes
        case 11: return .uuid
        case 12:
            let count = try reader.readCount()
            var components: [PersistableIdentifierType] = []
            components.reserveCapacity(count)
            for _ in 0..<count {
                components.append(
                    try reader.readLengthPrefixed {
                        (reader: inout StorageFrameDecoder) throws(
                            StorageFrameError
                        ) in
                        try readIdentifierType(from: &reader)
                    }
                )
            }
            return .composite(components)
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    private static func readDirectory(
        from reader: inout StorageFrameDecoder
    ) throws -> (components: [DirectoryPathComponent], layer: DirectoryLayer) {
        let count = try reader.readCount()
        var components: [DirectoryPathComponent] = []
        components.reserveCapacity(count)
        for _ in 0..<count {
            let tag = try reader.readUInt8()
            let value = try reader.readString()
            switch tag {
            case 0: components.append(.staticPath(value))
            case 1: components.append(.dynamicField(fieldName: value))
            default:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
        }
        let rawLayer = try reader.readString()
        guard let layer = DirectoryLayer(rawValue: rawLayer) else {
            throw SchemaEntityEntryCodecError.invalidEnum(
                type: "DirectoryLayer",
                value: rawLayer
            )
        }
        return (components, layer)
    }

    private static func readField(
        from reader: inout StorageFrameDecoder
    ) throws -> FieldSchema {
        let name = try reader.readString()
        let number = try readInt(from: &reader, field: "fieldNumber")
        let rawType = try reader.readString()
        guard let type = FieldSchemaType(rawValue: rawType) else {
            throw SchemaEntityEntryCodecError.invalidEnum(
                type: "FieldSchemaType",
                value: rawType
            )
        }
        return FieldSchema(
            name: name,
            fieldNumber: number,
            type: type,
            isOptional: try reader.readBool(),
            isArray: try reader.readBool(),
            referenceTargetEntity: try reader.readOptionalString()
        )
    }

    private static func readIndex(
        from reader: inout StorageFrameDecoder
    ) throws -> IndexDescriptorMetadata {
        let entityName = try reader.readString()
        let name = try reader.readString()
        let identifier = try reader.readString()
        let rawSubspace = try reader.readString()
        guard let subspace = SubspaceStructure(rawValue: rawSubspace) else {
            throw SchemaEntityEntryCodecError.invalidEnum(
                type: "SubspaceStructure",
                value: rawSubspace
            )
        }
        let fieldCount = try reader.readCount()
        var fields: [IndexFieldMetadata] = []
        fields.reserveCapacity(fieldCount)
        for _ in 0..<fieldCount {
            let identity = try readFieldIdentity(from: &reader)
            let rawOrder = try reader.readString()
            guard let order = IndexFieldOrder(rawValue: rawOrder) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "IndexFieldOrder",
                    value: rawOrder
                )
            }
            fields.append(IndexFieldMetadata(identity: identity, order: order))
        }
        let metadata = try readFieldValueMap(
            context: "index kind metadata",
            from: &reader
        )
        let options = CommonIndexOptions(
            unique: try reader.readBool(),
            sparse: try reader.readBool(),
            metadata: try readStringMap(
                context: "index common metadata",
                from: &reader
            )
        )
        return IndexDescriptorMetadata(
            entityName: entityName,
            name: name,
            kind: IndexKindMetadata(
                identifier: identifier,
                subspaceStructure: subspace,
                fields: fields,
                metadata: metadata
            ),
            commonOptions: options,
            storedFieldNames: try readStringArray(from: &reader)
        )
    }

    private static func readRelationship(
        from reader: inout StorageFrameDecoder
    ) throws -> RelationshipDescriptor {
        let ownerTypeName = try reader.readString()
        let propertyName = try reader.readString()
        let propertyFieldNumber = try reader.readUInt32()
        let relatedTypeName = try reader.readString()
        let rawCardinality = try reader.readString()
        guard let cardinality = RelationshipCardinality(
            rawValue: rawCardinality
        ) else {
            throw SchemaEntityEntryCodecError.invalidEnum(
                type: "RelationshipCardinality",
                value: rawCardinality
            )
        }
        let rawDeleteRule = try reader.readString()
        guard let deleteRule = DeleteRule(rawValue: rawDeleteRule) else {
            throw SchemaEntityEntryCodecError.invalidEnum(
                type: "DeleteRule",
                value: rawDeleteRule
            )
        }
        return RelationshipDescriptor(
            ownerTypeName: ownerTypeName,
            propertyName: propertyName,
            propertyFieldNumber: propertyFieldNumber,
            relatedTypeName: relatedTypeName,
            cardinality: cardinality,
            deleteRule: deleteRule
        )
    }

    private static func readAccessRule(
        from reader: inout StorageFrameDecoder
    ) throws -> FieldAccessRule {
        FieldAccessRule(
            field: try readFieldIdentity(from: &reader),
            read: try readAccessLevel(from: &reader),
            write: try readAccessLevel(from: &reader)
        )
    }

    private static func readFieldIdentity(
        from reader: inout StorageFrameDecoder
    ) throws -> FieldIdentity {
        let name = try reader.readString()
        let number = try readInt(from: &reader, field: "fieldIdentity")
        return FieldIdentity(name: name, number: number)
    }

    private static func readAccessLevel(
        from reader: inout StorageFrameDecoder
    ) throws -> FieldAccessLevel {
        switch try reader.readUInt8() {
        case 0: return .public
        case 1: return .authenticated
        case 2: return .roles(Set(try readStringArray(from: &reader)))
        case let tag:
            throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
        }
    }

    private static func readOntology(
        from reader: inout StorageFrameDecoder
    ) throws -> OntologyBinding? {
        switch try reader.readUInt8() {
        case 0:
            return nil
        case 1:
            return .owlClass(
                iri: try reader.readString(),
                properties: try readOWLProperties(from: &reader)
            )
        case 2:
            return .owlObjectProperty(
                iri: try reader.readString(),
                fromField: try reader.readString(),
                toField: try reader.readString(),
                properties: try readOWLProperties(from: &reader)
            )
        case let tag:
            throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
        }
    }

    private static func readOWLProperties(
        from reader: inout StorageFrameDecoder
    ) throws -> [OWLDataPropertyDescriptor] {
        let count = try reader.readCount()
        var properties: [OWLDataPropertyDescriptor] = []
        properties.reserveCapacity(count)
        for _ in 0..<count {
            properties.append(
                OWLDataPropertyDescriptor(
                    name: try reader.readString(),
                    fieldName: try reader.readString(),
                    iri: try reader.readString(),
                    label: try reader.readOptionalString(),
                    targetTypeName: try reader.readOptionalString(),
                    targetFieldName: try reader.readOptionalString()
                )
            )
        }
        return properties
    }

    private static func readPolymorphicMembership(
        from reader: inout StorageFrameDecoder
    ) throws -> PolymorphicMembership? {
        guard try reader.readBool() else {
            return nil
        }
        let identifier = try reader.readString()
        let directory = try readDirectory(from: &reader)
        let count = try reader.readCount()
        var indexes: [PolymorphicIndexDefinition] = []
        indexes.reserveCapacity(count)
        for _ in 0..<count {
            let name = try reader.readString()
            let definition = try readIndexDefinition(from: &reader)
            let fieldCount = try reader.readCount()
            var fields: [PolymorphicIndexField] = []
            fields.reserveCapacity(fieldCount)
            for _ in 0..<fieldCount {
                let fieldName = try reader.readString()
                let rawOrder = try reader.readString()
                guard let order = IndexFieldOrder(rawValue: rawOrder) else {
                    throw SchemaEntityEntryCodecError.invalidEnum(
                        type: "IndexFieldOrder",
                        value: rawOrder
                    )
                }
                fields.append(
                    PolymorphicIndexField(name: fieldName, order: order)
                )
            }
            indexes.append(
                PolymorphicIndexDefinition(
                    name: name,
                    definition: definition,
                    fields: fields,
                    commonOptions: CommonIndexOptions(
                        unique: try reader.readBool(),
                        sparse: try reader.readBool(),
                        metadata: try readStringMap(
                            context: "polymorphic index common metadata",
                            from: &reader
                        )
                    ),
                    storedFieldNames: try readStringArray(from: &reader)
                )
            )
        }
        return PolymorphicMembership(
            identifier: identifier,
            directoryComponents: directory.components,
            directoryLayer: directory.layer,
            indexes: indexes
        )
    }

    private static func readIndexDefinition(
        from reader: inout StorageFrameDecoder
    ) throws -> IndexDefinition {
        switch try reader.readUInt8() {
        case 0: return .scalar
        case 1: return .count
        case 2: return .sum
        case 3: return .minimum
        case 4: return .maximum
        case 5: return .average
        case 6:
            switch try reader.readUInt8() {
            case 0: return .version(strategy: .keepAll)
            case 1:
                return .version(
                    strategy: .keepLast(
                        try readInt(from: &reader, field: "versionCount")
                    )
                )
            case 2:
                let seconds = try reader.readInt64()
                let nanoseconds = try reader.readUInt32()
                do {
                    return .version(
                        strategy: .keepForDuration(
                            try TimeSpan(
                                seconds: seconds,
                                nanoseconds: nanoseconds
                            )
                        )
                    )
                } catch {
                    throw SchemaEntityEntryCodecError.invalidDefinition(
                        "Invalid version retention duration"
                    )
                }
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
        case 7: return .countUpdates
        case 8: return .countNotNull
        case 9: return .bitmap
        case 10:
            let window: LeaderboardWindowType
            switch try reader.readUInt8() {
            case 0: window = .hourly
            case 1: window = .daily
            case 2: window = .weekly
            case 3: window = .monthly
            case 4: window = .custom(duration: try reader.readDouble())
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            return .timeWindowLeaderboard(
                window: window,
                windowCount: try readInt(
                    from: &reader,
                    field: "windowCount"
                )
            )
        case 11:
            return .distinct(
                precision: try readInt(from: &reader, field: "precision")
            )
        case 12:
            return .percentile(compression: try reader.readDouble())
        case 13:
            let dimensions = try readInt(from: &reader, field: "dimensions")
            let rawMetric = try reader.readString()
            guard let metric = VectorMetric(rawValue: rawMetric) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "VectorMetric",
                    value: rawMetric
                )
            }
            return .vector(dimensions: dimensions, metric: metric)
        case 14:
            let rawTokenizer = try reader.readString()
            guard let tokenizer = TokenizationStrategy(
                rawValue: rawTokenizer
            ) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "TokenizationStrategy",
                    value: rawTokenizer
                )
            }
            return .fullText(
                tokenizer: tokenizer,
                storePositions: try reader.readBool(),
                ngramSize: try readInt(from: &reader, field: "ngramSize"),
                minTermLength: try readInt(
                    from: &reader,
                    field: "minTermLength"
                )
            )
        case 15:
            let rawEncoding = try reader.readString()
            guard let encoding = SpatialEncoding(rawValue: rawEncoding) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "SpatialEncoding",
                    value: rawEncoding
                )
            }
            return .spatial(
                encoding: encoding,
                level: try readInt(from: &reader, field: "level")
            )
        case 16:
            return .rank
        case 17:
            let pattern: PermutationPattern
            switch try reader.readUInt8() {
            case 0:
                pattern = .identity(
                    size: try readInt(from: &reader, field: "size")
                )
            case 1:
                pattern = .swapping(
                    try readInt(from: &reader, field: "first"),
                    try readInt(from: &reader, field: "second"),
                    size: try readInt(from: &reader, field: "size")
                )
            case 2:
                let count = try reader.readCount()
                var indices: [Int] = []
                indices.reserveCapacity(count)
                for _ in 0..<count {
                    indices.append(
                        try readInt(from: &reader, field: "permutation")
                    )
                }
                pattern = .ordering(indices)
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            return .permuted(pattern)
        case 18:
            let rawStrategy = try reader.readString()
            guard let strategy = PropertyGraphIndexStrategy(
                rawValue: rawStrategy
            ) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "PropertyGraphIndexStrategy",
                    value: rawStrategy
                )
            }
            let label: PropertyGraphLabelSource
            switch try reader.readUInt8() {
            case 0: label = .field
            case 1: label = .implicit
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            return .propertyGraph(strategy: strategy, label: label)
        case 19:
            return .autocomplete(
                minPrefixLength: try readInt(
                    from: &reader,
                    field: "minPrefixLength"
                ),
                maxPrefixLength: try readInt(
                    from: &reader,
                    field: "maxPrefixLength"
                )
            )
        case 20:
            return .rdfDataset
        case let tag:
            throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
        }
    }

    private static func readFieldValueMap(
        context: String,
        from reader: inout StorageFrameDecoder
    ) throws -> [String: FieldValue] {
        let count = try reader.readCount()
        var values: [String: FieldValue] = [:]
        values.reserveCapacity(count)
        for _ in 0..<count {
            let key = try reader.readString()
            guard values[key] == nil else {
                throw SchemaEntityEntryCodecError.duplicateMapKey(
                    context: context,
                    key: key
                )
            }
            values[key] = try reader.readLengthPrefixed {
                (reader: inout StorageFrameDecoder) throws(
                    StorageFrameError
                ) in
                try StorageValueDecoder.read(from: &reader)
            }
        }
        return values
    }

    private static func readStringMap(
        context: String,
        from reader: inout StorageFrameDecoder
    ) throws -> [String: String] {
        let count = try reader.readCount()
        var values: [String: String] = [:]
        values.reserveCapacity(count)
        for _ in 0..<count {
            let key = try reader.readString()
            guard values[key] == nil else {
                throw SchemaEntityEntryCodecError.duplicateMapKey(
                    context: context,
                    key: key
                )
            }
            values[key] = try reader.readString()
        }
        return values
    }

    private static func readStringArrayMap(
        context: String,
        from reader: inout StorageFrameDecoder
    ) throws -> [String: [String]] {
        let count = try reader.readCount()
        var values: [String: [String]] = [:]
        values.reserveCapacity(count)
        for _ in 0..<count {
            let key = try reader.readString()
            guard values[key] == nil else {
                throw SchemaEntityEntryCodecError.duplicateMapKey(
                    context: context,
                    key: key
                )
            }
            values[key] = try readStringArray(from: &reader)
        }
        return values
    }

    private static func readStringArray(
        from reader: inout StorageFrameDecoder
    ) throws -> [String] {
        let count = try reader.readCount()
        var values: [String] = []
        values.reserveCapacity(count)
        for _ in 0..<count {
            values.append(try reader.readString())
        }
        return values
    }

    private static func readInt(
        from reader: inout StorageFrameDecoder,
        field: String
    ) throws -> Int {
        let encoded = try reader.readInt64()
        guard let value = Int(exactly: encoded) else {
            throw SchemaEntityEntryCodecError.invalidInteger(
                field: field,
                value: encoded
            )
        }
        return value
    }
}
