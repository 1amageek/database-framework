import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct SchemaDescribeHandler: DatabaseOperationHandler {
    public typealias Operation = SchemaDescribeOperation

    public init() {}

    public func handle(
        _ request: EmptyOperationPayload,
        context: DatabaseOperationContext
    ) async throws -> SchemaDescribeOperation.Response {
        _ = request
        let executor = try context.requireControlExecutor()
        return try await executor.withTransaction(
            requiredAccess: .read,
            configuration: .readOnly
        ) { _ in
            let entities = try context.executor.schema.entities
                .sorted { $0.name < $1.name }
                .map {
                    try Self.describe(
                        $0,
                        runtimeConfiguration: executor.runtimeConfiguration
                    )
                }
            return SchemaDescribeOperation.Response(
                version: context.executor.schema.version,
                entities: entities
            )
        }
    }

    private static func describe(
        _ entity: Schema.Entity,
        runtimeConfiguration: DatabaseRuntimeConfiguration
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
                        entity: entity,
                        runtimeConfiguration: runtimeConfiguration
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
                    options: try options(for: index)
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
        case .int8:
            return .int8
        case .int16:
            return .int16
        case .int32:
            return .int32
        case .int64:
            return .int64
        case .uint8:
            return .uint8
        case .uint16:
            return .uint16
        case .uint32:
            return .uint32
        case .uint64:
            return .uint64
        case .float32:
            return .float32
        case .float64:
            return .float64
        case .decimal:
            return .decimal
        case .string, .enum:
            return .string
        case .uuid:
            return .uuid
        case .bytes:
            return .bytes
        case .date:
            return .date
        case .time:
            return .time
        case .dateTime:
            return .dateTime
        case .timestamp:
            return .timestamp
        case .timeSpan:
            return .timeSpan
        case .calendarPeriod:
            return .calendarPeriod
        case .geographicPoint:
            return .geographicPoint
        case .geographicPosition:
            return .geographicPosition
        case .vector:
            return .vector
        case .rdfTerm:
            return .rdfTerm
        case .nested, .object:
            return .object
        case .reference:
            return .reference
        }
    }

    private static func reference(
        for field: FieldSchema,
        entity: Schema.Entity,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) throws -> SchemaDescribeOperation.Reference? {
        guard field.type == .reference else { return nil }
        guard runtimeConfiguration.entityRuntimes.registration(
            named: entity.name
        ) != nil,
              let descriptor = entity.relationships.first(where: {
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
    ) throws -> FieldObject {
        var values: [(key: String, value: FieldValue)] =
            index.kind.metadata.map {
                (key: "kind.\($0.key)", value: $0.value)
            }
        values.append(
            (
                key: "common.unique",
                value: .bool(index.commonOptions.unique)
            )
        )
        values.append(
            (
                key: "common.sparse",
                value: .bool(index.commonOptions.sparse)
            )
        )
        values.append(contentsOf: index.commonOptions.metadata.map {
            (
                key: "common.metadata.\($0.key)",
                value: .string($0.value)
            )
        })
        values.append(
            (
                key: "storedFields",
                value: .array(index.storedFieldNames.map(FieldValue.string))
            )
        )
        return try FieldObject(
            values.sorted { $0.key < $1.key }
        )
    }
}
