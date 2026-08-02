import DatabaseTypes
import DatabaseKit
import StorageKit

public enum CanonicalTupleElementCodec {
    public static func encode(_ element: any TupleElement & Sendable) throws -> FieldValue {
        guard let value = element.tupleValue else {
            throw CanonicalTupleElementCodecError.unsupportedType(
                TupleElementSemanticName.describe(element)
            )
        }

        switch value {
        case .null:
            return .null
        case .bytes(let value):
            return .bytes(value)
        case .string(let value):
            return .string(value)
        case .boolean(let value):
            return .bool(value)
        case .signedInteger(let value):
            return .int64(value)
        case .unsignedInteger(let value):
            return .uint64(value)
        case .float32(let value):
            return .float32(value)
        case .float64(let value):
            return .float64(value)
        case .uuid(let value):
            return .uuid(value)
        case .nested, .versionstamp:
            throw CanonicalTupleElementCodecError.unsupportedType(
                TupleElementSemanticName.describe(element)
            )
        }
    }

    public static func decode(_ value: FieldValue) throws -> (any TupleElement & Sendable) {
        switch value {
        case .null:
            return TupleNil()
        case .bytes(let bytes):
            return bytes
        case .string(let string):
            return string
        case .bool(let bool):
            return bool
        case .int64(let int):
            return int
        case .uint64(let unsigned):
            return unsigned
        case .float32(let float):
            return float
        case .float64(let double):
            return double
        case .uuid(let uuid):
            return uuid
        default:
            throw CanonicalTupleElementCodecError.invalidValue
        }
    }
}

enum CanonicalTupleElementCodecError: Error, Sendable {
    case unsupportedType(String)
    case invalidValue
}
