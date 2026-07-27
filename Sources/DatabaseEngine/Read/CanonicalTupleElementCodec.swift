#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseTypes
import DatabaseKit
import StorageKit

#if canImport(FoundationEssentials)
private typealias PlatformUUID = FoundationEssentials.UUID
private typealias PlatformDate = FoundationEssentials.Date
#else
private typealias PlatformUUID = Foundation.UUID
private typealias PlatformDate = Foundation.Date
#endif

public enum CanonicalTupleElementCodec {
    private enum Key {
        static let type = "type"
        static let value = "value"
        static let uuid = "uuid"
        static let date = "date"
        static let float = "float"
    }

    public static func encode(_ element: any TupleElement & Sendable) throws -> FieldValue {
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
            return try typedObject(type: Key.float, value: .float64(Double(value)))
        case let value as Double:
            return .float64(value)
        case let value as PlatformUUID:
            return try typedObject(type: Key.uuid, value: .string(value.uuidString))
        case let value as PlatformDate:
            return try typedObject(
                type: Key.date,
                value: .float64(value.timeIntervalSince1970)
            )
        default:
            throw CanonicalTupleElementCodecError.unsupportedType(String(describing: type(of: element)))
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
                      let uuid = PlatformUUID(uuidString: raw) else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return uuid
            case Key.date:
                guard let intervalValue = objectValue(named: Key.value, in: object),
                      case .float64(let interval) = intervalValue else {
                    throw CanonicalTupleElementCodecError.invalidValue
                }
                return PlatformDate(timeIntervalSince1970: interval)
            case Key.float:
                guard let scalarValue = objectValue(named: Key.value, in: object),
                      case .float64(let scalar) = scalarValue else {
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
