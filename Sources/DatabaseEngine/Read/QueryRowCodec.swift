import DatabaseKit
import DatabaseTypes

public enum QueryRowCodec {
    public static func encode<T: Persistable>(
        _ item: T,
        columns: [QueryColumn],
        annotations: [String: FieldValue] = [:]
    ) throws -> QueryRow {
        try encode(
            PersistedModel(item),
            schemas: T.fieldSchemas,
            columns: columns,
            annotations: annotations
        )
    }

    public static func encode(
        _ item: PersistedModel,
        entity: Schema.Entity,
        columns: [QueryColumn],
        annotations: [String: FieldValue] = [:]
    ) throws -> QueryRow {
        guard item.entity == entity.name else {
            throw QueryRowCodecError.entityMismatch(expected: entity.name, actual: item.entity)
        }
        return try encode(item, schemas: entity.fields, columns: columns, annotations: annotations)
    }

    public static func decode<T: Persistable>(
        _ row: QueryRow,
        columns: [QueryColumn],
        as type: T.Type
    ) throws -> T {
        let fields = try persistedFields(
            from: row, columns: columns, schemas: T.fieldSchemas, entityName: T.persistableType
        )
        return try T.decodePersistedFields(fields)
    }

    public static func persistedModel(
        from row: QueryRow,
        columns: [QueryColumn],
        entity: Schema.Entity
    ) throws -> PersistedModel {
        try PersistedModel(
            entity: entity.name,
            fields: persistedFields(from: row, columns: columns, schemas: entity.fields, entityName: entity.name)
        )
    }

    private static func persistedFields(
        from row: QueryRow,
        columns: [QueryColumn],
        schemas: [FieldSchema],
        entityName: String
    ) throws -> [PersistableField] {
        // Name projection is an explicit destination boundary, never execution storage.
        let values = try row.namedValues(columns: columns)
        let schemasByName = try fieldMap(schemas, entityName: entityName)
        var fields: [PersistableField] = []
        fields.reserveCapacity(schemas.count)
        for (column, value) in zip(columns, row.values) {
            guard let schema = schemasByName[column.name] else {
                throw QueryRowCodecError.unknownField(type: entityName, field: column.name)
            }
            let payload: FieldValue
            if let descriptor = schema.queryType {
                guard descriptor == column.type else {
                    throw QueryRowCodecError.incompatibleField(type: entityName, field: schema.name)
                }
                try QueryFieldStorageAdapter.validateDescriptor(descriptor, fieldType: schema.type, isArray: schema.isArray)
                payload = try QueryFieldStorageAdapter.encode(
                    value, type: descriptor,
                    limits: QueryFieldStorageAdapter.inferredLimits(for: value, type: descriptor)
                )
            } else {
                try validatePrimitiveColumn(column, schema: schema, entityName: entityName)
                guard case .primitive(let primitive) = value else {
                    throw QueryRowCodecError.richValueRequiresDeclaredAdapter(field: schema.name)
                }
                guard QueryValueValidator.accepts(value, type: .primitive(schema.type, isArray: schema.isArray)) else {
                    throw QueryRowCodecError.incompatibleField(type: entityName, field: schema.name)
                }
                payload = primitive
            }
            guard schema.isOptional || !value.isNull else {
                throw QueryRowCodecError.incompatibleField(type: entityName, field: schema.name)
            }
            fields.append(try field(payload, schema: schema, entityName: entityName))
        }
        for schema in schemas where values[schema.name] == nil {
            guard let defaultValue = schema.defaultValue else {
                throw QueryRowCodecError.missingField(type: entityName, field: schema.name)
            }
            try validatePersistedValue(defaultValue, schema: schema, entityName: entityName)
            fields.append(try field(defaultValue, schema: schema, entityName: entityName))
        }
        fields.sort { $0.number < $1.number }
        return fields
    }

    private static func encode(
        _ item: PersistedModel,
        schemas: [FieldSchema],
        columns: [QueryColumn],
        annotations: [String: FieldValue]
    ) throws -> QueryRow {
        try QueryColumn.validate(columns)
        let schemasByName = try fieldMap(schemas, entityName: item.entity)
        for stored in item.fields {
            guard let schema = schemasByName[stored.name] else {
                throw QueryRowCodecError.unknownField(type: item.entity, field: stored.name)
            }
            guard UInt32(exactly: schema.fieldNumber) == stored.number else {
                throw QueryRowCodecError.invalidFieldIdentity(type: item.entity, field: stored.name)
            }
        }
        var names = Set<String>()
        var values: [QueryValue] = []
        values.reserveCapacity(columns.count)
        for column in columns {
            guard names.insert(column.name).inserted else {
                throw QueryRowCodecError.duplicateColumn(column.name)
            }
            guard let schema = schemasByName[column.name] else {
                throw QueryRowCodecError.unknownField(type: item.entity, field: column.name)
            }
            let payload: FieldValue
            if let stored = item.value(forFieldNamed: column.name) {
                payload = stored
            } else if let defaultValue = schema.defaultValue {
                payload = defaultValue
            } else {
                throw QueryRowCodecError.missingField(type: item.entity, field: column.name)
            }
            guard schema.isOptional || payload != .null else {
                throw QueryRowCodecError.incompatibleField(type: item.entity, field: schema.name)
            }
            if let descriptor = schema.queryType {
                guard descriptor == column.type else {
                    throw QueryRowCodecError.incompatibleField(type: item.entity, field: column.name)
                }
                try QueryFieldStorageAdapter.validateDescriptor(descriptor, fieldType: schema.type, isArray: schema.isArray)
                values.append(try QueryFieldStorageAdapter.decode(
                    payload, type: descriptor,
                    limits: QueryFieldStorageAdapter.inferredLimits(for: payload, type: descriptor)
                ))
            } else {
                try validatePrimitiveColumn(column, schema: schema, entityName: item.entity)
                let value = QueryValue.primitive(payload)
                guard QueryValueValidator.accepts(value, type: .primitive(schema.type, isArray: schema.isArray)) else {
                    throw QueryRowCodecError.incompatibleField(type: item.entity, field: column.name)
                }
                values.append(value)
            }
        }
        var fields: [String: FieldValue] = [:]
        fields.reserveCapacity(item.fields.count)
        for field in item.fields { fields[field.name] = field.value }
        return try QueryRow(
            values: values, columns: columns, annotations: annotations,
            version: PersistableVersionTokenCodec.token(for: fields)
        )
    }

    private static func fieldMap(
        _ schemas: [FieldSchema], entityName: String
    ) throws -> [String: FieldSchema] {
        var result: [String: FieldSchema] = [:]
        var numbers = Set<Int>()
        result.reserveCapacity(schemas.count)
        for schema in schemas {
            guard schema.fieldNumber > 0, UInt32(exactly: schema.fieldNumber) != nil,
                  numbers.insert(schema.fieldNumber).inserted else {
                throw QueryRowCodecError.invalidFieldIdentity(type: entityName, field: schema.name)
            }
            guard result.updateValue(schema, forKey: schema.name) == nil else {
                throw QueryRowCodecError.duplicateField(type: entityName, field: schema.name)
            }
        }
        return result
    }

    private static func validatePrimitiveColumn(
        _ column: QueryColumn, schema: FieldSchema, entityName: String
    ) throws {
        do {
            try QueryFieldStorageAdapter.validateDescriptor(column.type, fieldType: schema.type, isArray: schema.isArray)
        } catch {
            throw QueryRowCodecError.incompatibleField(type: entityName, field: schema.name)
        }
    }

    private static func validatePersistedValue(
        _ payload: FieldValue, schema: FieldSchema, entityName: String
    ) throws {
        guard schema.isOptional || payload != .null else {
            throw QueryRowCodecError.incompatibleField(type: entityName, field: schema.name)
        }
        if let descriptor = schema.queryType {
            try QueryFieldStorageAdapter.validateDescriptor(descriptor, fieldType: schema.type, isArray: schema.isArray)
            _ = try QueryFieldStorageAdapter.decode(
                payload, type: descriptor,
                limits: QueryFieldStorageAdapter.inferredLimits(for: payload, type: descriptor)
            )
        } else if !QueryValueValidator.accepts(.primitive(payload), type: .primitive(schema.type, isArray: schema.isArray)) {
            throw QueryRowCodecError.incompatibleField(type: entityName, field: schema.name)
        }
    }

    private static func field(
        _ value: FieldValue, schema: FieldSchema, entityName: String
    ) throws -> PersistableField {
        guard let number = UInt32(exactly: schema.fieldNumber), number > 0 else {
            throw QueryRowCodecError.invalidFieldIdentity(type: entityName, field: schema.name)
        }
        return try PersistableField(number: number, name: schema.name, value: value)
    }
}
