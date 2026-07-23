#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue

public enum QueryRowCodec {
    public static func encode<T: Persistable>(
        _ item: T,
        annotations: [String: DatabaseValue] = [:]
    ) throws -> QueryRow {
        let fields = try canonicalFields(item)
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
        var fields: [DatabaseObjectField] = []
        fields.reserveCapacity(row.fields.count)
        for (name, value) in row.fields {
            guard let schema = schemas[name],
                  schema.fieldNumber > 0,
                  let number = UInt32(exactly: schema.fieldNumber) else {
                throw QueryRowCodecError.unknownField(type: T.persistableType, field: name)
            }
            fields.append(
                DatabaseObjectField(
                    number: number,
                    name: name,
                    value: value
                )
            )
        }
        return try T.decodePersistedFields(fields)
    }

    public static func encodeAny(
        _ item: any Persistable,
        annotations: [String: DatabaseValue] = [:]
    ) throws -> QueryRow {
        let fields = try canonicalFields(item)
        return QueryRow(
            fields: fields,
            annotations: annotations,
            version: try PersistableVersionTokenCodec.token(for: fields)
        )
    }

    private static func canonicalFields(
        _ item: any Persistable
    ) throws -> [String: DatabaseValue] {
        let encoded = try PersistableFieldEncoder.encode(item)
        var fields: [String: DatabaseValue] = [:]
        fields.reserveCapacity(encoded.count)
        for field in encoded {
            guard fields.updateValue(field.value, forKey: field.name) == nil else {
                throw QueryRowCodecError.duplicateField(
                    type: type(of: item).persistableType,
                    field: field.name
                )
            }
        }
        return fields
    }
}
