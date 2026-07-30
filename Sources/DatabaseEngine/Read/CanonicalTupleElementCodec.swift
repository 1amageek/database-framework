import DatabaseTypes
import DatabaseKit
import StorageKit

public enum CanonicalTupleElementCodec {
    private enum Key {
        static let type = "type"
        static let value = "value"
        static let uuid = "uuid"
        static let float = "float"
    }

    public static func encode(_ element: any TupleElement & Sendable) throws -> FieldValue {
        guard let value = element.tupleValue else {
            throw CanonicalTupleElementCodecError.unsupportedType(
                TupleElementSemanticName.describe(element)
            )
        }

        switch value {
        case .string(let value):
            return .string(value)
        case .boolean(let value):
            return .bool(value)
        case .signedInteger(let value):
            return .int64(value)
        case .unsignedInteger(let value):
            return .uint64(value)
        case .float32(let value):
            return try typedObject(type: Key.float, value: .float64(Double(value)))
        case .float64(let value):
            return .float64(value)
        case .uuid(let value):
            return .uuid(value)
        case .null, .bytes, .nested, .versionstamp:
            throw CanonicalTupleElementCodecError.unsupportedType(
                TupleElementSemanticName.describe(element)
            )
        }
    }

    public static func decode(_ value: FieldValue) throws -> (any TupleElement & Sendable) {
        switch value {
        case .string(let string):
            return string
        case .bool(let bool):
            return bool
        case .int64(let int):
            return int
        case .uint64(let unsigned):
            return unsigned
        case .float64(let double):
            return double
        case .object(let object):
            guard let typeValue = objectValue(named: Key.type, in: object),
                  case .string(let type) = typeValue else {
                throw CanonicalTupleElementCodecError.invalidValue
            }
            switch type {
            case Key.uuid:
                guard let rawValue = objectValue(named: Key.value, in: object),
                      case .string(let raw) = rawValue,
                      let uuid = DatabaseTypes.UUID(canonicalString: raw) else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return uuid
            case Key.float:
                guard let scalarValue = objectValue(named: Key.value, in: object),
                      case .float64(let scalar) = scalarValue else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return Float(scalar)
            default:
                throw CanonicalTupleElementCodecError.unsupportedType(type)
            }
        case .uuid(let uuid):
            return uuid
        default:
            throw CanonicalTupleElementCodecError.invalidValue
        }
    }

    private static func typedObject(
        type: String,
        value: FieldValue
    ) throws -> FieldValue {
        do {
            return .object(
                try FieldObject([
                (key: Key.type, value: .string(type)),
                (key: Key.value, value: value),
                ])
            )
        } catch {
            throw CanonicalTupleElementCodecError.invalidValue
        }
    }

    private static func objectValue(
        named name: String,
        in object: FieldObject
    ) -> FieldValue? {
        object[name]
    }
}

enum CanonicalTupleElementCodecError: Error, Sendable {
    case unsupportedType(String)
    case invalidValue
}
