import Foundation
import Core
import DatabaseClientProtocol

public enum QueryRowCodec {
    public static func encode<T: Persistable>(
        _ item: T,
        annotations: [String: FieldValue] = [:]
    ) throws -> QueryRow {
        let fields = Dictionary(
            uniqueKeysWithValues: T.allFields.map { fieldName in
                (fieldName, FieldReader.readFieldValue(from: item, fieldName: fieldName))
            }
        )
        return QueryRow(fields: fields, annotations: annotations)
    }

    public static func decode<T: Persistable>(
        _ row: QueryRow,
        as type: T.Type
    ) throws -> T {
        let jsonObject = try Dictionary(
            uniqueKeysWithValues: row.fields.map { key, value in
                (key, try jsonValue(for: value))
            }
        )
        let data = try JSONSerialization.data(withJSONObject: jsonObject)
        return try JSONDecoder().decode(T.self, from: data)
    }

    public static func encodeAny(
        _ item: any Persistable,
        annotations: [String: FieldValue] = [:]
    ) -> QueryRow {
        let itemType = type(of: item)
        let fields = Dictionary(
            uniqueKeysWithValues: itemType.allFields.map { fieldName in
                (fieldName, FieldReader.readFieldValue(from: item, fieldName: fieldName))
            }
        )
        return QueryRow(fields: fields, annotations: annotations)
    }

    private static func jsonValue(for value: FieldValue) throws -> Any {
        switch value {
        case .int64(let raw):
            return NSNumber(value: raw)
        case .double(let raw):
            return NSNumber(value: raw)
        case .string(let raw):
            return raw
        case .bool(let raw):
            return raw
        case .data(let raw):
            return raw.base64EncodedString()
        case .null:
            return NSNull()
        case .array(let values):
            return try values.map(jsonValue(for:))
        }
    }
}
