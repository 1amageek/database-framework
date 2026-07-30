import DatabaseKit
import DatabaseTypes
import StorageKit

internal enum TupleElementSemanticName {
    static func describe(_ element: any TupleElement) -> String {
        switch element.tupleValue {
        case .null: return "null"
        case .string: return "string"
        case .boolean: return "boolean"
        case .signedInteger: return "signed integer"
        case .unsignedInteger: return "unsigned integer"
        case .float32: return "32-bit floating point"
        case .float64: return "64-bit floating point"
        case .bytes(let bytes):
            return FieldValueTupleCodec.isCanonicalEncoding(bytes)
                ? "field value"
                : "byte string"
        case .uuid: return "UUID"
        case .nested: return "tuple"
        case .versionstamp: return "versionstamp"
        case nil: return "unsupported tuple element"
        }
    }

    static func describe(_ value: FieldValue) -> String {
        switch value {
        case .null: return "null"
        case .bool: return "boolean"
        case .int8: return "int8"
        case .int16: return "int16"
        case .int32: return "int32"
        case .int64: return "int64"
        case .uint8: return "uint8"
        case .uint16: return "uint16"
        case .uint32: return "uint32"
        case .uint64: return "uint64"
        case .float32: return "float32"
        case .float64: return "float64"
        case .decimal: return "decimal"
        case .string: return "string"
        case .bytes: return "bytes"
        case .uuid: return "UUID"
        case .timestamp: return "timestamp"
        case .date: return "date"
        case .time: return "time"
        case .dateTime: return "date-time"
        case .timeSpan: return "time span"
        case .calendarPeriod: return "calendar period"
        case .reference: return "entity reference"
        case .rdfTerm: return "RDF term"
        case .vector: return "vector"
        case .geographicPoint: return "geographic point"
        case .geographicPosition: return "geographic position"
        case .array: return "array"
        case .object: return "object"
        }
    }
}
