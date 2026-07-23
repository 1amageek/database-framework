#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseValue
import QueryIR
import StorageKit

public enum CanonicalTupleElementCodec {
    private enum Key {
        static let type = "type"
        static let value = "value"
        static let uuid = "uuid"
        static let date = "date"
        static let float = "float"
    }

    public static func encode(_ element: any TupleElement & Sendable) throws -> QueryParameterValue {
        switch element {
        case let value as String:
            return .string(value)
        case let value as Bool:
            return .bool(value)
        case let value as Int:
            return .int64(Int64(value))
        case let value as Int32:
            return .int64(Int64(value))
        case let value as Int64:
            return .int64(value)
        case let value as UInt64:
            return .uint64(value)
        case let value as Float:
            return typedObject(type: Key.float, value: .double(Double(value)))
        case let value as Double:
            return .double(value)
        case let value as UUID:
            return typedObject(type: Key.uuid, value: .string(value.uuidString))
        case let value as Date:
            return typedObject(type: Key.date, value: .double(value.timeIntervalSince1970))
        default:
            throw CanonicalTupleElementCodecError.unsupportedType(String(describing: type(of: element)))
        }
    }

    public static func decode(_ value: QueryParameterValue) throws -> (any TupleElement & Sendable) {
        switch value {
        case .string(let string):
            return string
        case .bool(let bool):
            return bool
        case .int64(let int):
            return int
        case .uint64(let unsigned):
            return unsigned
        case .double(let double):
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
                      let uuid = UUID(uuidString: raw) else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return uuid
            case Key.date:
                guard let intervalValue = objectValue(named: Key.value, in: object),
                      case .double(let interval) = intervalValue else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return Date(timeIntervalSince1970: interval)
            case Key.float:
                guard let scalarValue = objectValue(named: Key.value, in: object),
                      case .double(let scalar) = scalarValue else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return Float(scalar)
            default:
                throw CanonicalTupleElementCodecError.unsupportedType(type)
            }
        default:
            throw CanonicalTupleElementCodecError.invalidValue
        }
    }

    private static func typedObject(type: String, value: DatabaseValue) -> DatabaseValue {
        .object([
            DatabaseObjectField(number: 1, name: Key.type, value: .string(type)),
            DatabaseObjectField(number: 2, name: Key.value, value: value),
        ])
    }

    private static func objectValue(
        named name: String,
        in fields: [DatabaseObjectField]
    ) -> DatabaseValue? {
        fields.first { $0.name == name }?.value
    }
}

enum CanonicalTupleElementCodecError: Error, Sendable {
    case unsupportedType(String)
    case invalidValue
}
