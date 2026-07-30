// TupleDecoder.swift
// DatabaseEngine - Decode FoundationDB TupleElement to Swift types
//
// Single responsibility: Convert TupleElement to Swift types for index operations.

import DatabaseTypes
import StorageKit

/// A Swift value that can be decoded from the database tuple representation.
public protocol TupleDecodable {
    static func decodeTupleElement(_ element: any TupleElement) throws -> Self
}

// MARK: - TupleDecoder

/// Decodes FoundationDB TupleElement to Swift types
///
/// **MANDATORY**: All Index modules MUST use this decoder.
/// Custom decoding implementations are PROHIBITED.
///
/// **Type Mapping**:
/// | TupleElement | Decodable Types |
/// |--------------|-----------------|
/// | Int64/UInt64 | Signed and unsigned integer types (range checked) |
/// | Double | Double, Float |
/// | String | String |
/// | Bool | Bool |
/// | [UInt8] | [UInt8], Data |
/// | UUID | UUID |
/// | Date | Date |
///
/// **Reference**: FoundationDB Tuple Layer Specification
///
/// **Usage**:
/// ```swift
/// let score = try TupleDecoder.decodeInt64(element)
/// let name = try TupleDecoder.decode(element, as: String.self)
/// ```
public struct TupleDecoder: Sendable {

    // Private init to prevent instantiation
    private init() {}

    // MARK: - Integer Decoding

    /// Decode as Int64
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Int64 value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeInt64(_ element: any TupleElement) throws -> Int64 {
        if let value = try canonicalFieldValue(element) {
            switch value {
            case .int8(let value): return Int64(value)
            case .int16(let value): return Int64(value)
            case .int32(let value): return Int64(value)
            case .int64(let value): return value
            case .uint8(let value): return Int64(value)
            case .uint16(let value): return Int64(value)
            case .uint32(let value): return Int64(value)
            case .uint64(let value):
                guard let exact = Int64(exactly: value) else {
                    throw TupleDecodingError.unsignedIntegerOverflow(
                        value: value,
                        targetType: "Int64"
                    )
                }
                return exact
            case .float32(let value):
                return try exactInt64(Double(value))
            case .float64(let value):
                return try exactInt64(value)
            default:
                throw canonicalTypeMismatch(
                    expected: "Int64",
                    value: value
                )
            }
        }
        switch element.tupleValue {
        case .signedInteger(let value):
            return value
        case .unsignedInteger(let value):
            guard let exact = Int64(exactly: value) else {
                throw TupleDecodingError.unsignedIntegerOverflow(
                    value: value,
                    targetType: "Int64"
                )
            }
            return exact
        case .float64(let value):
            guard !value.isNaN, !value.isInfinite,
                  let exact = Int64(exactly: value) else {
                throw TupleDecodingError.integerOverflow(
                    value: 0,
                    targetType: "Int64 (exact from Double \(value))"
                )
            }
            return exact
        case .float32(let value):
            guard !value.isNaN, !value.isInfinite,
                  let exact = Int64(exactly: value) else {
                throw TupleDecodingError.integerOverflow(
                    value: 0,
                    targetType: "Int64 (exact from Float \(value))"
                )
            }
            return exact
        default:
            break
        }
        throw TupleDecodingError.typeMismatch(
            expected: "Int64",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    /// Decode as Int with range check
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Int value
    /// - Throws: TupleDecodingError on type mismatch or overflow
    public static func decodeInt(_ element: any TupleElement) throws -> Int {
        let i64 = try decodeInt64(element)
        guard i64 >= Int64(Int.min) && i64 <= Int64(Int.max) else {
            throw TupleDecodingError.integerOverflow(value: i64, targetType: "Int")
        }
        return Int(i64)
    }

    /// Decode as Int32 with range check
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Int32 value
    /// - Throws: TupleDecodingError on type mismatch or overflow
    public static func decodeInt32(_ element: any TupleElement) throws -> Int32 {
        let i64 = try decodeInt64(element)
        guard i64 >= Int64(Int32.min) && i64 <= Int64(Int32.max) else {
            throw TupleDecodingError.integerOverflow(value: i64, targetType: "Int32")
        }
        return Int32(i64)
    }

    /// Decode as Int16 with range check
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Int16 value
    /// - Throws: TupleDecodingError on type mismatch or overflow
    public static func decodeInt16(_ element: any TupleElement) throws -> Int16 {
        let i64 = try decodeInt64(element)
        guard i64 >= Int64(Int16.min) && i64 <= Int64(Int16.max) else {
            throw TupleDecodingError.integerOverflow(value: i64, targetType: "Int16")
        }
        return Int16(i64)
    }

    /// Decode as Int8 with range check
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Int8 value
    /// - Throws: TupleDecodingError on type mismatch or overflow
    public static func decodeInt8(_ element: any TupleElement) throws -> Int8 {
        let i64 = try decodeInt64(element)
        guard i64 >= Int64(Int8.min) && i64 <= Int64(Int8.max) else {
            throw TupleDecodingError.integerOverflow(value: i64, targetType: "Int8")
        }
        return Int8(i64)
    }

    // MARK: - Unsigned Integer Decoding

    /// Decode as UInt64 without passing through a signed or floating value.
    public static func decodeUInt64(_ element: any TupleElement) throws -> UInt64 {
        if let value = try canonicalFieldValue(element) {
            switch value {
            case .uint8(let value): return UInt64(value)
            case .uint16(let value): return UInt64(value)
            case .uint32(let value): return UInt64(value)
            case .uint64(let value): return value
            case .int8(let value): return try exactUInt64(Int64(value))
            case .int16(let value): return try exactUInt64(Int64(value))
            case .int32(let value): return try exactUInt64(Int64(value))
            case .int64(let value): return try exactUInt64(value)
            case .float32(let value):
                return try exactUInt64(Double(value))
            case .float64(let value):
                return try exactUInt64(value)
            default:
                throw canonicalTypeMismatch(
                    expected: "UInt64",
                    value: value
                )
            }
        }
        switch element.tupleValue {
        case .unsignedInteger(let value):
            return value
        case .signedInteger(let value):
            guard let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.integerOverflow(
                    value: value,
                    targetType: "UInt64"
                )
            }
            return exact
        case .float64(let value):
            guard value.isFinite, let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.typeMismatch(
                    expected: "UInt64 (exact)",
                    actual: "Double(\(value))"
                )
            }
            return exact
        case .float32(let value):
            guard value.isFinite, let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.typeMismatch(
                    expected: "UInt64 (exact)",
                    actual: "Float(\(value))"
                )
            }
            return exact
        default:
            break
        }
        throw TupleDecodingError.typeMismatch(
            expected: "UInt64",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    /// Decode as UInt with a platform-width range check.
    public static func decodeUInt(_ element: any TupleElement) throws -> UInt {
        let value = try decodeUInt64(element)
        guard let exact = UInt(exactly: value) else {
            throw TupleDecodingError.unsignedIntegerOverflow(
                value: value,
                targetType: "UInt"
            )
        }
        return exact
    }

    /// Decode as UInt32 with a range check.
    public static func decodeUInt32(_ element: any TupleElement) throws -> UInt32 {
        let value = try decodeUInt64(element)
        guard let exact = UInt32(exactly: value) else {
            throw TupleDecodingError.unsignedIntegerOverflow(
                value: value,
                targetType: "UInt32"
            )
        }
        return exact
    }

    /// Decode as UInt16 with a range check.
    public static func decodeUInt16(_ element: any TupleElement) throws -> UInt16 {
        let value = try decodeUInt64(element)
        guard let exact = UInt16(exactly: value) else {
            throw TupleDecodingError.unsignedIntegerOverflow(
                value: value,
                targetType: "UInt16"
            )
        }
        return exact
    }

    /// Decode as UInt8 with a range check.
    public static func decodeUInt8(_ element: any TupleElement) throws -> UInt8 {
        let value = try decodeUInt64(element)
        guard let exact = UInt8(exactly: value) else {
            throw TupleDecodingError.unsignedIntegerOverflow(
                value: value,
                targetType: "UInt8"
            )
        }
        return exact
    }

    // MARK: - Floating Point Decoding

    /// Decode as Double
    ///
    /// Also accepts Int64/Int for numeric type coercion.
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Double value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeDouble(_ element: any TupleElement) throws -> Double {
        if let value = try canonicalFieldValue(element) {
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
            default:
                throw canonicalTypeMismatch(
                    expected: "Double",
                    value: value
                )
            }
        }
        switch element.tupleValue {
        case .float64(let value): return value
        case .float32(let value): return Double(value)
        case .signedInteger(let value): return Double(value)
        case .unsignedInteger(let value): return Double(value)
        default: break
        }
        throw TupleDecodingError.typeMismatch(
            expected: "Double",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    /// Decode as Float
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Float value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeFloat(_ element: any TupleElement) throws -> Float {
        let d = try decodeDouble(element)
        return Float(d)
    }

    // MARK: - String Decoding

    /// Decode as String
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: String value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeString(_ element: any TupleElement) throws -> String {
        if let value = try canonicalFieldValue(element) {
            guard case .string(let string) = value else {
                throw canonicalTypeMismatch(
                    expected: "String",
                    value: value
                )
            }
            return string
        }
        if case .string(let value) = element.tupleValue { return value }
        throw TupleDecodingError.typeMismatch(
            expected: "String",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    // MARK: - Bool Decoding

    /// Decode as Bool
    /// - Parameter element: TupleElement to decode
    /// - Returns: Bool value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeBool(_ element: any TupleElement) throws -> Bool {
        if let value = try canonicalFieldValue(element) {
            guard case .bool(let bool) = value else {
                throw canonicalTypeMismatch(
                    expected: "Bool",
                    value: value
                )
            }
            return bool
        }
        if case .boolean(let value) = element.tupleValue { return value }
        throw TupleDecodingError.typeMismatch(
            expected: "Bool",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    // MARK: - Binary Decoding

    /// Decode as owned storage bytes
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Owned byte value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeBytes(_ element: any TupleElement) throws -> ByteString {
        if let value = try canonicalFieldValue(element) {
            guard case .bytes(let bytes) = value else {
                throw canonicalTypeMismatch(
                    expected: "ByteString",
                    value: value
                )
            }
            return bytes
        }
        if case .bytes(let value) = element.tupleValue {
            return value
        }
        throw TupleDecodingError.typeMismatch(
            expected: "ByteString",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    // MARK: - UUID Decoding

    /// Decode the canonical database UUID representation.
    public static func decodeUUID(
        _ element: any TupleElement
    ) throws -> DatabaseTypes.UUID {
        if case .uuid(let value) = element.tupleValue { return value }
        if let value = try canonicalFieldValue(element), case .uuid(let uuid) = value {
            return uuid
        }
        throw TupleDecodingError.typeMismatch(
            expected: "UUID",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    /// Decode the canonical timestamp representation.
    public static func decodeTimestamp(
        _ element: any TupleElement
    ) throws -> Timestamp {
        if let value = try canonicalFieldValue(element), case .timestamp(let timestamp) = value {
            return timestamp
        }
        throw TupleDecodingError.typeMismatch(
            expected: "Timestamp",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    /// Decode the canonical civil-date representation.
    public static func decodeCivilDate(
        _ element: any TupleElement
    ) throws -> CivilDate {
        if let value = try canonicalFieldValue(element), case .date(let date) = value {
            return date
        }
        throw TupleDecodingError.typeMismatch(
            expected: "CivilDate",
            actual: TupleElementSemanticName.describe(element)
        )
    }

    // MARK: - Generic Decoding

    /// Decode as a specified type
    ///
    /// - Parameters:
    ///   - element: TupleElement to decode
    ///   - type: Target type
    /// - Returns: Decoded value of type T
    /// - Throws: TupleDecodingError on failure
    public static func decode<T: TupleDecodable>(
        _ element: any TupleElement,
        as type: T.Type
    ) throws -> T {
        try T.decodeTupleElement(element)
    }

    private static func canonicalFieldValue(
        _ element: any TupleElement
    ) throws -> FieldValue? {
        guard case .bytes(let bytes) = element.tupleValue,
              FieldValueTupleCodec.isCanonicalEncoding(bytes) else {
            return nil
        }
        return try FieldValueTupleCodec.decode(bytes)
    }

    private static func canonicalTypeMismatch(
        expected: String,
        value: FieldValue
    ) -> TupleDecodingError {
        .typeMismatch(
            expected: expected,
            actual: TupleElementSemanticName.describe(value)
        )
    }

    private static func exactInt64(
        _ value: Double
    ) throws -> Int64 {
        guard value.isFinite, let exact = Int64(exactly: value) else {
            throw TupleDecodingError.typeMismatch(
                expected: "Int64 (exact)",
                actual: "Double(\(value))"
            )
        }
        return exact
    }

    private static func exactUInt64(
        _ value: Int64
    ) throws -> UInt64 {
        guard let exact = UInt64(exactly: value) else {
            throw TupleDecodingError.integerOverflow(
                value: value,
                targetType: "UInt64"
            )
        }
        return exact
    }

    private static func exactUInt64(
        _ value: Double
    ) throws -> UInt64 {
        guard value.isFinite, let exact = UInt64(exactly: value) else {
            throw TupleDecodingError.typeMismatch(
                expected: "UInt64 (exact)",
                actual: "Double(\(value))"
            )
        }
        return exact
    }

}

extension Int64: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Int64 {
        try TupleDecoder.decodeInt64(element)
    }
}

extension Int: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Int {
        try TupleDecoder.decodeInt(element)
    }
}

extension Int32: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Int32 {
        try TupleDecoder.decodeInt32(element)
    }
}

extension Int16: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Int16 {
        try TupleDecoder.decodeInt16(element)
    }
}

extension Int8: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Int8 {
        try TupleDecoder.decodeInt8(element)
    }
}

extension UInt64: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> UInt64 {
        try TupleDecoder.decodeUInt64(element)
    }
}

extension UInt: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> UInt {
        try TupleDecoder.decodeUInt(element)
    }
}

extension UInt32: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> UInt32 {
        try TupleDecoder.decodeUInt32(element)
    }
}

extension UInt16: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> UInt16 {
        try TupleDecoder.decodeUInt16(element)
    }
}

extension UInt8: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> UInt8 {
        try TupleDecoder.decodeUInt8(element)
    }
}

extension Double: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Double {
        try TupleDecoder.decodeDouble(element)
    }
}

extension Float: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Float {
        try TupleDecoder.decodeFloat(element)
    }
}

extension String: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> String {
        try TupleDecoder.decodeString(element)
    }
}

extension Bool: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Bool {
        try TupleDecoder.decodeBool(element)
    }
}

extension ByteString: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> ByteString {
        try TupleDecoder.decodeBytes(element)
    }
}

extension DatabaseTypes.UUID: TupleDecodable {
    public static func decodeTupleElement(
        _ element: any TupleElement
    ) throws -> DatabaseTypes.UUID {
        try TupleDecoder.decodeUUID(element)
    }
}

extension Timestamp: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> Timestamp {
        try TupleDecoder.decodeTimestamp(element)
    }
}

extension CivilDate: TupleDecodable {
    public static func decodeTupleElement(_ element: any TupleElement) throws -> CivilDate {
        try TupleDecoder.decodeCivilDate(element)
    }
}

// MARK: - TupleDecodingError

/// Errors that can occur during tuple element decoding
public enum TupleDecodingError: Error, Sendable, Equatable, CustomStringConvertible {
    /// Element type does not match expected type
    case typeMismatch(expected: String, actual: String)

    /// Integer value exceeds target type range
    case integerOverflow(value: Int64, targetType: String)

    /// Unsigned integer value exceeds target type range
    case unsignedIntegerOverflow(value: UInt64, targetType: String)

    /// Target type is not supported for decoding
    case unsupportedType(String)

    /// Tuple format is invalid (wrong element count, missing bitmap, etc.)
    case invalidFormat(String)

    public var description: String {
        switch self {
        case .typeMismatch(let expected, let actual):
            return "Type mismatch: expected \(expected), got \(actual)"
        case .integerOverflow(let value, let targetType):
            return "Integer overflow: \(value) cannot fit in \(targetType)"
        case .unsignedIntegerOverflow(let value, let targetType):
            return "Unsigned integer overflow: \(value) cannot fit in \(targetType)"
        case .unsupportedType(let type):
            return "Unsupported type for decoding: \(type)"
        case .invalidFormat(let message):
            return "Invalid tuple format: \(message)"
        }
    }
}
