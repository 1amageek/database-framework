import DatabaseKit
import DatabaseTypes

public enum QueryRowCodec {
    public static func encode<T: Persistable>(
        _ item: T,
        annotations: [String: FieldValue] = [:]
    ) throws -> QueryRow {
        let fields = try canonicalFields(PersistedModel(item))
        return QueryRow(
            fields: fields,
            annotations: annotations,
            version: try PersistableVersionTokenCodec.token(for: fields)
        )
    }

    public static func decode<T: Persistable>(
        _ row: QueryRow,
        as type: T.Type
    ) throws -> T {
        let schemas = Dictionary(uniqueKeysWithValues: T.fieldSchemas.map { ($0.name, $0) })
        var fields: [PersistableField] = []
        fields.reserveCapacity(row.fields.count)
        for (name, value) in row.fields {
            guard let schema = schemas[name],
                  schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw QueryRowCodecError.unknownField(type: T.persistableType, field: name)
            }
            fields.append(
                try PersistableField(
                    number: number,
                    name: name,
                    value: value
                )
            )
        }
        return try T.decodePersistedFields(fields)
    }

    public static func encode(
        _ item: PersistedModel,
        annotations: [String: FieldValue] = [:]
    ) throws -> QueryRow {
        let fields = try canonicalFields(item)
        return QueryRow(
            fields: fields,
            annotations: annotations,
            version: try PersistableVersionTokenCodec.token(for: fields)
        )
    }

    public static func persistedModel(
        from row: QueryRow,
        entity: Schema.Entity
    ) throws -> PersistedModel {
        var fields: [PersistableField] = []
        fields.reserveCapacity(row.fields.count)
        for (name, value) in row.fields {
            guard let schema = entity.fieldMapByName[name],
                  schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw QueryRowCodecError.unknownField(
                    type: entity.name,
                    field: name
                )
            }
            fields.append(
                try PersistableField(
                    number: number,
                    name: name,
                    value: value
                )
            )
        }
        fields.sort { $0.number < $1.number }
        return try PersistedModel(
            entity: entity.name,
            fields: fields
        )
    }

    private static func canonicalFields(
        _ item: PersistedModel
    ) throws -> [String: FieldValue] {
        var fields: [String: FieldValue] = [:]
        fields.reserveCapacity(item.fields.count)
        for field in item.fields {
            guard fields.updateValue(field.value, forKey: field.name) == nil else {
                throw QueryRowCodecError.duplicateField(
                    type: item.entity,
                    field: field.name
                )
            }
        }
        return fields
    }
}
