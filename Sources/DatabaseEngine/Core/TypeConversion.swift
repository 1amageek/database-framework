import DatabaseKit
import DatabaseTypes
import StorageKit

/// Canonical conversions used by execution and index code.
///
/// Values enter the framework through `FieldValueRepresentable`; this layer
/// never discovers model meaning through reflection or Foundation type casts.
public enum TypeConversion {
    public static func asInt64(_ value: FieldValue) -> Int64? {
        switch value {
        case .int8(let value): return Int64(value)
        case .int16(let value): return Int64(value)
        case .int32(let value): return Int64(value)
        case .int64(let value): return value
        case .uint8(let value): return Int64(value)
        case .uint16(let value): return Int64(value)
        case .uint32(let value): return Int64(value)
        case .uint64(let value): return Int64(exactly: value)
        case .bool(let value): return value ? 1 : 0
        default: return nil
        }
    }

    public static func asDouble(_ value: FieldValue) -> Double? {
        switch value {
        case .int8(let value): return Double(value)
        case .int16(let value): return Double(value)
        case .int32(let value): return Double(value)
        case .int64(let value): return Double(value)
        case .uint8(let value): return Double(value)
        case .uint16(let value): return Double(value)
        case .uint32(let value): return Double(value)
        case .uint64(let value): return Double(value)
        case .float32(let value): return Double(value)
        case .float64(let value): return value
        default: return nil
        }
    }

    public static func asFloat(_ value: FieldValue) -> Float? {
        asDouble(value).map(Float.init)
    }

    public static func asString(_ value: FieldValue) -> String? {
        switch value {
        case .string(let value): return value
        case .uuid(let value): return value.description
        default: return nil
        }
    }

    public static func toFieldValue<Value: FieldValueRepresentable>(
        _ value: Value
    ) -> FieldValue {
        value.fieldValue
    }

    public static func toFieldValue(_ value: FieldValue) -> FieldValue {
        value
    }

    public static func toTupleElement<Value: FieldValueRepresentable>(
        _ value: Value
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try TupleEncoder.encode(value)
    }

    public static func toTupleElement(
        _ value: FieldValue
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try TupleEncoder.encode(value)
    }

    public static func int64(
        from element: any TupleElement
    ) throws -> Int64 {
        try TupleDecoder.decodeInt64(element)
    }

    public static func double(
        from element: any TupleElement
    ) throws -> Double {
        try TupleDecoder.decodeDouble(element)
    }

    public static func string(
        from element: any TupleElement
    ) throws -> String {
        try TupleDecoder.decodeString(element)
    }
}
