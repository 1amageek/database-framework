import DatabaseKit
import DatabaseTypes
import StorageKit

/// Persists the complete semantic schema declaration owned by `DatabaseKit`.
///
/// This is an engine storage format, not the client/server wire protocol.
enum SchemaEntityEntryCodec {
    private static let magic: UInt32 = 0x4353_4244
    private static let version: UInt16 = 4

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
        writer.writeBool(field.defaultValue != nil)
        if let defaultValue = field.defaultValue {
            try writer.writeLengthPrefixed {
                writer throws(StorageFrameError) in
                try StorageValueEncoder.write(defaultValue, into: &writer)
            }
        }
    }

    static func write(
        _ index: IndexDescriptor,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(index.entityName)
        try write(
            index.declaration,
            into: &writer,
            writeField: { field, writer throws(StorageFrameError) in
                try write(field, into: &writer)
            }
        )
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
            try write(
                index,
                into: &writer,
                writeField: { field, writer throws(StorageFrameError) in
                    try writer.writeString(field)
                }
            )
        }
    }

    private static func write<FieldReference>(
        _ declaration: IndexDeclaration<FieldReference>,
        into writer: inout StorageFrameEncoder,
        writeField: (FieldReference, inout StorageFrameEncoder)
            throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) {
        try writer.writeString(declaration.name)
        try write(
            declaration.definition,
            into: &writer,
            writeField: writeField
        )
    }

    private static func write<FieldReference>(
        _ definition: IndexDefinition<FieldReference>,
        into writer: inout StorageFrameEncoder,
        writeField: (FieldReference, inout StorageFrameEncoder)
            throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) {
        switch definition {
        case .ordered(let keys, let includedFields, let unique):
            writer.writeUInt8(0)
            try writeIndexKeys(keys, into: &writer, writeField: writeField)
            try writeIndexFields(
                includedFields,
                into: &writer,
                writeField: writeField
            )
            writer.writeBool(unique)
        case .aggregate(let function, let groupBy, let value):
            writer.writeUInt8(1)
            switch function {
            case .count: writer.writeUInt8(0)
            case .sum: writer.writeUInt8(1)
            case .minimum: writer.writeUInt8(2)
            case .maximum: writer.writeUInt8(3)
            case .average: writer.writeUInt8(4)
            case .nonNullCount: writer.writeUInt8(5)
            case .approximateDistinct(let precision):
                writer.writeUInt8(6)
                try write(precision, into: &writer)
            case .percentile(let compression):
                writer.writeUInt8(7)
                writer.writeDouble(compression)
            }
            try writeIndexKeys(groupBy, into: &writer, writeField: writeField)
            writer.writeBool(value != nil)
            if let value {
                try writeField(value, &writer)
            }
        case .updateCount(let field):
            writer.writeUInt8(2)
            try writeField(field, &writer)
        case .history(let version, let retention):
            writer.writeUInt8(3)
            try writeField(version, &writer)
            switch retention {
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
        case .bitmap(let field):
            writer.writeUInt8(4)
            try writeField(field, &writer)
        case .leaderboard(let groupBy, let score, let window, let windowCount):
            writer.writeUInt8(5)
            try writeIndexKeys(groupBy, into: &writer, writeField: writeField)
            try writeField(score, &writer)
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
        case .vector(let embedding, let dimensions, let metric):
            writer.writeUInt8(6)
            try writeField(embedding, &writer)
            try write(dimensions, into: &writer)
            try writer.writeString(metric.rawValue)
        case .text(let fields, let mode):
            writer.writeUInt8(7)
            try writeIndexFields(fields, into: &writer, writeField: writeField)
            switch mode {
            case .fullText(
                let tokenizer,
                let storePositions,
                let ngramSize,
                let minimumTermLength
            ):
                writer.writeUInt8(0)
                try writer.writeString(tokenizer.rawValue)
                writer.writeBool(storePositions)
                try write(ngramSize, into: &writer)
                try write(minimumTermLength, into: &writer)
            case .autocomplete(
                let minimumPrefixLength,
                let maximumPrefixLength
            ):
                writer.writeUInt8(1)
                try write(minimumPrefixLength, into: &writer)
                try write(maximumPrefixLength, into: &writer)
            }
        case .spatial(let location, let encoding, let level):
            writer.writeUInt8(8)
            try writeField(location, &writer)
            try writer.writeString(encoding.rawValue)
            try write(level, into: &writer)
        case .rank(let score):
            writer.writeUInt8(9)
            try writeField(score, &writer)
        case .graph(let graph, let includedFields):
            writer.writeUInt8(10)
            switch graph {
            case .property(let source, let label, let target, let graph, let strategy):
                writer.writeUInt8(0)
                try writeField(source, &writer)
                switch label {
                case .field(let field):
                    writer.writeUInt8(0)
                    try writeField(field, &writer)
                case .implicit:
                    writer.writeUInt8(1)
                }
                try writeField(target, &writer)
                writer.writeBool(graph != nil)
                if let graph { try writeField(graph, &writer) }
                try writer.writeString(strategy.rawValue)
            case .rdf(let subject, let predicate, let object, let graph):
                writer.writeUInt8(1)
                try writeField(subject, &writer)
                try writeField(predicate, &writer)
                try writeField(object, &writer)
                writer.writeBool(graph != nil)
                if let graph { try writeField(graph, &writer) }
            case .ontologyProjection(let individualIRIBase, let graph):
                writer.writeUInt8(2)
                try writer.writeString(individualIRIBase)
                writer.writeBool(graph != nil)
                if let graph {
                    try writer.writeLengthPrefixed {
                        writer throws(StorageFrameError) in
                        try StorageValueEncoder.write(
                            .rdfTerm(graph.term),
                            into: &writer
                        )
                    }
                }
            }
            try writeIndexFields(
                includedFields,
                into: &writer,
                writeField: writeField
            )
        case .custom(let custom):
            writer.writeUInt8(11)
            try writer.writeString(custom.identifier)
            try writeIndexKeys(custom.keys, into: &writer, writeField: writeField)
            try writeIndexFields(
                custom.includedFields,
                into: &writer,
                writeField: writeField
            )
            try writeFieldValueMap(custom.parameters, into: &writer)
        }
    }

    private static func writeIndexKeys<FieldReference>(
        _ keys: [IndexKey<FieldReference>],
        into writer: inout StorageFrameEncoder,
        writeField: (FieldReference, inout StorageFrameEncoder)
            throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) {
        try writer.writeCount(keys.count)
        for key in keys {
            try writeField(key.field, &writer)
            writer.writeUInt8(key.order == .ascending ? 0 : 1)
        }
    }

    private static func writeIndexFields<FieldReference>(
        _ fields: [FieldReference],
        into writer: inout StorageFrameEncoder,
        writeField: (FieldReference, inout StorageFrameEncoder)
            throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) {
        try writer.writeCount(fields.count)
        for field in fields {
            try writeField(field, &writer)
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
        var indexes: [IndexDescriptor] = []
        indexes.reserveCapacity(indexCount)
        for _ in 0..<indexCount {
            indexes.append(try readIndex(
                    entityName: name,
                    fieldSchemas: fields,
                    from: &reader))
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
        let isOptional = try reader.readBool()
        let isArray = try reader.readBool()
        let referenceTargetEntity = try reader.readOptionalString()
        let defaultValue: FieldValue?
        if try reader.readBool() {
            defaultValue = try reader.readLengthPrefixed {
                reader throws(StorageFrameError) in
                try StorageValueDecoder.read(from: &reader)
            }
        } else {
            defaultValue = Optional<FieldValue>.none
        }
        return FieldSchema(
            name: name,
            fieldNumber: number,
            type: type,
            isOptional: isOptional,
            isArray: isArray,
            referenceTargetEntity: referenceTargetEntity,
            defaultValue: defaultValue
        )
    }

    private static func readIndex(
        entityName: String,
        fieldSchemas: [FieldSchema],
        from reader: inout StorageFrameDecoder
    ) throws -> IndexDescriptor {
        let encodedEntityName = try reader.readString()
        guard encodedEntityName == entityName else {
            throw SchemaEntityEntryCodecError.invalidDefinition(
                "Index entity '\(encodedEntityName)' does not match '\(entityName)'"
            )
        }
        let declaration: IndexDeclaration<FieldIdentity> =
            try readIndexDeclaration(
                from: &reader,
                readField: { reader in
                    try readFieldIdentity(from: &reader)
                }
            )
        do {
            return try IndexDescriptor(
                entityName: entityName,
                declaration: declaration,
                fieldSchemas: fieldSchemas
            )
        } catch {
            throw SchemaEntityEntryCodecError.invalidDefinition(
                error.description
            )
        }
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
        var indexes: [IndexDeclaration<String>] = []
        indexes.reserveCapacity(count)
        for _ in 0..<count {
            indexes.append(
                try readIndexDeclaration(
                    from: &reader,
                    readField: { reader in
                        try reader.readString()
                    }
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

    private static func readIndexDeclaration<FieldReference>(
        from reader: inout StorageFrameDecoder,
        readField: (inout StorageFrameDecoder) throws -> FieldReference
    ) throws -> IndexDeclaration<FieldReference> {
        IndexDeclaration(
            name: try reader.readString(),
            definition: try readIndexDefinition(
                from: &reader,
                readField: readField
            )
        )
    }

    private static func readIndexDefinition<FieldReference>(
        from reader: inout StorageFrameDecoder,
        readField: (inout StorageFrameDecoder) throws -> FieldReference
    ) throws -> IndexDefinition<FieldReference> {
        switch try reader.readUInt8() {
        case 0:
            return .ordered(
                keys: try readIndexKeys(from: &reader, readField: readField),
                includedFields: try readIndexFields(
                    from: &reader,
                    readField: readField
                ),
                unique: try reader.readBool()
            )
        case 1:
            let function: AggregateIndexFunction
            switch try reader.readUInt8() {
            case 0: function = .count
            case 1: function = .sum
            case 2: function = .minimum
            case 3: function = .maximum
            case 4: function = .average
            case 5: function = .nonNullCount
            case 6:
                function = .approximateDistinct(
                    precision: try readInt(from: &reader, field: "precision")
                )
            case 7:
                function = .percentile(compression: try reader.readDouble())
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            let groupBy = try readIndexKeys(
                from: &reader,
                readField: readField
            )
            let value: FieldReference?
            if try reader.readBool() {
                value = try readField(&reader)
            } else {
                value = nil
            }
            return .aggregate(function: function, groupBy: groupBy, value: value)
        case 2:
            return .updateCount(field: try readField(&reader))
        case 3:
            let version = try readField(&reader)
            let retention: VersionHistoryStrategy
            switch try reader.readUInt8() {
            case 0:
                retention = .keepAll
            case 1:
                retention = .keepLast(
                    try readInt(from: &reader, field: "versionCount")
                )
            case 2:
                do {
                    retention = .keepForDuration(
                        try TimeSpan(
                            seconds: try reader.readInt64(),
                            nanoseconds: try reader.readUInt32()
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
            return .history(version: version, retention: retention)
        case 4:
            return .bitmap(field: try readField(&reader))
        case 5:
            let groupBy = try readIndexKeys(
                from: &reader,
                readField: readField
            )
            let score = try readField(&reader)
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
            return .leaderboard(
                groupBy: groupBy,
                score: score,
                window: window,
                windowCount: try readInt(
                    from: &reader,
                    field: "windowCount"
                )
            )
        case 6:
            let embedding = try readField(&reader)
            let dimensions = try readInt(from: &reader, field: "dimensions")
            let rawMetric = try reader.readString()
            guard let metric = VectorMetric(rawValue: rawMetric) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "VectorMetric",
                    value: rawMetric
                )
            }
            return .vector(
                embedding: embedding,
                dimensions: dimensions, metric: metric
            )
        case 7:
            let fields = try readIndexFields(
                from: &reader,
                readField: readField
            )
            let mode: TextIndexMode
            switch try reader.readUInt8() {
            case 0:
                let rawTokenizer = try reader.readString()
            guard let tokenizer = TokenizationStrategy(
                rawValue: rawTokenizer
            ) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "TokenizationStrategy",
                    value: rawTokenizer
                    )
                }
                mode = .fullText(
                tokenizer: tokenizer,
                storePositions: try reader.readBool(),
                ngramSize: try readInt(from: &reader, field: "ngramSize"),
                    minimumTermLength: try readInt(
                    from: &reader,
                        field: "minimumTermLength"
                    )
                )
            case 1:
                mode = .autocomplete(
                    minimumPrefixLength: try readInt(
                        from: &reader,
                        field: "minimumPrefixLength"
                    ),
                    maximumPrefixLength: try readInt(
                        from: &reader,
                        field: "maximumPrefixLength"
                    )
                )
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            return .text(fields: fields, mode: mode)
        case 8:
            let location = try readField(&reader)
            let rawEncoding = try reader.readString()
            guard let encoding = SpatialEncoding(rawValue: rawEncoding) else {
                throw SchemaEntityEntryCodecError.invalidEnum(
                    type: "SpatialEncoding",
                    value: rawEncoding
                )
            }
            return .spatial(
                location: location,
                encoding: encoding,
                level: try readInt(from: &reader, field: "level")
            )
        case 9:
            return .rank(score: try readField(&reader))
        case 10:
            let graph: GraphIndexDefinition<FieldReference>
            switch try reader.readUInt8() {
            case 0:
                let source = try readField(&reader)
                let label: PropertyGraphLabel<FieldReference>
                switch try reader.readUInt8() {
                case 0: label = .field(try readField(&reader))
                case 1: label = .implicit
                case let tag:
                    throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
                }
                let target = try readField(&reader)
                let namespace: FieldReference?
                if try reader.readBool() {
                    namespace = try readField(&reader)
                } else {
                    namespace = nil
                }
                let rawStrategy = try reader.readString()
                guard
                    let strategy = PropertyGraphIndexStrategy(
                        rawValue: rawStrategy
                    )
                else {
                    throw SchemaEntityEntryCodecError.invalidEnum(
                        type: "PropertyGraphIndexStrategy",
                        value: rawStrategy
                    )
                }
                graph = .property(
                    source: source,
                    label: label,
                    target: target,
                    graph: namespace,
                    strategy: strategy
                )
            case 1:
                let subject = try readField(&reader)
                let predicate = try readField(&reader)
                let object = try readField(&reader)
                let namespace: FieldReference?
                if try reader.readBool() {
                    namespace = try readField(&reader)
                } else {
                    namespace = nil
                }
                graph = .rdf(
                    subject: subject,
                    predicate: predicate,
                    object: object,
                    graph: namespace
                )
            case 2:
                let individualIRIBase = try reader.readString()
                let graphName: RDFGraphName?
                if try reader.readBool() {
                    let value = try reader.readLengthPrefixed {
                        reader throws(StorageFrameError) in
                        try StorageValueDecoder.read(from: &reader)
                    }
                    guard case .rdfTerm(let term) = value else {
                        throw SchemaEntityEntryCodecError.invalidDefinition(
                            "Ontology projection graph must be an RDF term"
                        )
                    }
                    do {
                        graphName = try RDFGraphName(term)
                    } catch {
                        throw SchemaEntityEntryCodecError.invalidDefinition(
                            "Ontology projection graph is invalid"
                        )
                    }
                } else {
                    graphName = nil
                }
                graph = .ontologyProjection(
                    individualIRIBase: individualIRIBase,
                    graph: graphName
                )
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            return .graph(
                graph,
                includedFields: try readIndexFields(
                    from: &reader,
                    readField: readField
                )
            )
        case 11:
            return .custom(
                CustomIndexDefinition(
                    identifier: try reader.readString(),
                    keys: try readIndexKeys(
                        from: &reader,
                        readField: readField
                    ),
                    includedFields: try readIndexFields(
                        from: &reader,
                        readField: readField
                    ),
                    parameters: try readFieldValueMap(
                        context: "custom index parameters",
                        from: &reader
                    )
                )
            )
        case let tag:
            throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
        }
    }

    private static func readIndexKeys<FieldReference>(
        from reader: inout StorageFrameDecoder,
        readField: (inout StorageFrameDecoder) throws -> FieldReference
    ) throws -> [IndexKey<FieldReference>] {
        let count = try reader.readCount()
        var keys: [IndexKey<FieldReference>] = []
        keys.reserveCapacity(count)
                for _ in 0..<count {
            let field = try readField(&reader)
            let order: IndexFieldOrder
            switch try reader.readUInt8() {
            case 0: order = .ascending
            case 1: order = .descending
            case let tag:
                throw SchemaEntityEntryCodecError.invalidMetadataTag(tag)
            }
            keys.append(IndexKey(field, order: order))
        }
        return keys
    }

    private static func readIndexFields<FieldReference>(
        from reader: inout StorageFrameDecoder,
        readField: (inout StorageFrameDecoder) throws -> FieldReference
    ) throws -> [FieldReference] {
        let count = try reader.readCount()
        var fields: [FieldReference] = []
        fields.reserveCapacity(count)
        for _ in 0..<count {
            fields.append(try readField(&reader))
        }
        return fields
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
