import Core
import DatabaseValue
import DatabaseWire
import Relationship

public struct SchemaDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = SchemaDescribeOperation

    private let schemaVersion: DatabaseSchemaVersion

    public init(schemaVersion: DatabaseSchemaVersion) {
        self.schemaVersion = schemaVersion
    }

    public func handle(
        _ request: DatabaseEmpty,
        context: DatabaseOperationContext
    ) async throws -> SchemaDescribeOperation.Response {
        let entities = try context.container.schema.entities
            .sorted { $0.name < $1.name }
            .map(Self.describe)
        return SchemaDescribeOperation.Response(version: schemaVersion, entities: entities)
    }

    private static func describe(
        _ entity: Schema.Entity
    ) throws -> SchemaDescribeOperation.Entity {
        let fields = try entity.fields
            .sorted { $0.fieldNumber < $1.fieldNumber }
            .map { field -> SchemaDescribeOperation.Field in
                guard field.fieldNumber > 0,
                      let number = UInt32(exactly: field.fieldNumber) else {
                    throw SchemaDescriptionError.invalidFieldNumber(
                        entity: entity.name,
                        field: field.name,
                        number: field.fieldNumber
                    )
                }
                return SchemaDescribeOperation.Field(
                    number: number,
                    name: field.name,
                    type: valueType(for: field),
                    nullable: field.isOptional,
                    reference: try reference(
                        for: field,
                        entity: entity
                    )
                )
            }

        let numbersByName = Dictionary(
            uniqueKeysWithValues: fields.map { ($0.name, $0.number) }
        )
        let indexes = try entity.indexes
            .sorted { $0.name < $1.name }
            .map { index -> SchemaDescribeOperation.Index in
                let fieldNumbers = try index.fieldNames.map { fieldName in
                    guard let number = numbersByName[fieldName] else {
                        throw SchemaDescriptionError.indexFieldNotFound(
                            entity: entity.name,
                            index: index.name,
                            field: fieldName
                        )
                    }
                    return number
                }
                return SchemaDescribeOperation.Index(
                    name: index.name,
                    kind: index.kindIdentifier,
                    fields: fieldNumbers,
                    options: options(for: index)
                )
            }
        return SchemaDescribeOperation.Entity(
            name: entity.name,
            fields: fields,
            indexes: indexes
        )
    }

    private static func valueType(
        for field: FieldSchema
    ) -> SchemaDescribeOperation.ValueType {
        if field.isArray { return .array }
        switch field.type {
        case .bool:
            return .bool
        case .int, .int8, .int16, .int32, .int64:
            return .int64
        case .uint, .uint8, .uint16, .uint32, .uint64:
            return .uint64
        case .double, .float:
            return .double
        case .string, .enum:
            return .string
        case .uuid:
            return .uuid
        case .data:
            return .bytes
        case .date:
            return .timestamp
        case .rdfTerm:
            return .rdfTerm
        case .nested:
            return .object
        case .reference:
            return .reference
        }
    }

    private static func reference(
        for field: FieldSchema,
        entity: Schema.Entity
    ) throws -> SchemaDescribeOperation.Reference? {
        guard field.type == .reference else { return nil }
        guard let ownerType = entity.persistableType,
              let descriptor = ownerType.relationshipDescriptors.first(where: {
                  $0.propertyName == field.name
              }) else {
            throw SchemaDescriptionError.relationshipMetadataNotFound(
                entity: entity.name,
                field: field.name
            )
        }
        let cardinality: SchemaDescribeOperation.ReferenceCardinality
        switch descriptor.cardinality {
        case .requiredToOne: cardinality = .requiredToOne
        case .optionalToOne: cardinality = .optionalToOne
        case .toMany: cardinality = .toMany
        }
        let deleteRule: SchemaDescribeOperation.ReferenceDeleteRule
        switch descriptor.deleteRule {
        case .nullify: deleteRule = .nullify
        case .cascade: deleteRule = .cascade
        case .deny: deleteRule = .deny
        case .noAction: deleteRule = .noAction
        }
        return SchemaDescribeOperation.Reference(
            targetEntity: descriptor.relatedTypeName,
            cardinality: cardinality,
            deleteRule: deleteRule
        )
    }

    private static func options(
        for index: IndexDescriptorMetadata
    ) -> [DatabaseObjectField] {
        var values: [(String, IndexMetadataValue)] = index.kind.metadata.map {
            ("kind.\($0.key)", $0.value)
        }
        values.append(contentsOf: index.commonMetadata.map {
            ("common.\($0.key)", $0.value)
        })
        return values
            .sorted { $0.0 < $1.0 }
            .enumerated()
            .map { offset, entry in
                DatabaseObjectField(
                    number: UInt32(offset + 1),
                    name: entry.0,
                    value: databaseValue(entry.1)
                )
            }
    }

    private static func databaseValue(_ value: IndexMetadataValue) -> DatabaseValue {
        switch value {
        case .string(let scalar):
            return .string(scalar)
        case .int(let scalar):
            return .int64(Int64(scalar))
        case .double(let scalar):
            return .double(scalar)
        case .bool(let scalar):
            return .bool(scalar)
        case .stringArray(let values):
            return .array(values.map(DatabaseValue.string))
        case .intArray(let values):
            return .array(values.map { .int64(Int64($0)) })
        case .rdfTerm(let term):
            return .rdfTerm(term)
        }
    }
}
