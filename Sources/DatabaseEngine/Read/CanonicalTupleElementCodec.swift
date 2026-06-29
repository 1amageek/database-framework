import Foundation
import QueryIR
import StorageKit

public enum CanonicalTupleElementCodec {
    private enum Key {
        static let type = "type"
        static let value = "value"
        static let uuid = "uuid"
        static let date = "date"
        static let float = "float"
        static let uint64 = "uint64"
    }

    public static func encode(_ element: any TupleElement & Sendable) throws -> QueryParameterValue {
        switch element {
        case let value as String:
            return .string(value)
        case let value as Bool:
            return .bool(value)
        case let value as Int:
            return .int(Int64(value))
        case let value as Int32:
            return .int(Int64(value))
        case let value as Int64:
            return .int(value)
        case let value as UInt64:
            guard let exact = Int64(exactly: value) else {
                return .object([
                    Key.type: .string(Key.uint64),
                    Key.value: .string(String(value))
                ])
            }
            return .int(exact)
        case let value as Float:
            return .object([
                Key.type: .string(Key.float),
                Key.value: .double(Double(value))
            ])
        case let value as Double:
            return .double(value)
        case let value as UUID:
            return .object([
                Key.type: .string(Key.uuid),
                Key.value: .string(value.uuidString)
            ])
        case let value as Date:
            return .object([
                Key.type: .string(Key.date),
                Key.value: .double(value.timeIntervalSince1970)
            ])
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
        case .int(let int):
            return int
        case .double(let double):
            return double
        case .object(let object):
            guard let type = object[Key.type]?.stringValue else {
                throw CanonicalTupleElementCodecError.invalidValue
            }
            switch type {
            case Key.uuid:
                guard let raw = object[Key.value]?.stringValue,
                      let uuid = UUID(uuidString: raw) else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return uuid
            case Key.date:
                guard let interval = object[Key.value]?.doubleValue else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return Date(timeIntervalSince1970: interval)
            case Key.float:
                guard let scalar = object[Key.value]?.doubleValue else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return Float(scalar)
            case Key.uint64:
                guard let raw = object[Key.value]?.stringValue,
                      let unsigned = UInt64(raw) else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return unsigned
            default:
                throw CanonicalTupleElementCodecError.unsupportedType(type)
            }
        default:
            throw CanonicalTupleElementCodecError.invalidValue
        }
    }
}

enum CanonicalTupleElementCodecError: Error, Sendable {
    case unsupportedType(String)
    case invalidValue
}
