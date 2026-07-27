// TupleDecoder.swift
// DatabaseEngine - Decode FoundationDB TupleElement to Swift types
//
// Single responsibility: Convert TupleElement to Swift types for index operations.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseTypes
import DatabaseTypesFoundation
import StorageKit
import StorageKitFoundation

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
        if let value = canonicalFieldValue(element) {
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
        if let v = element as? Int64 { return v }
        if let v = element as? Int { return Int64(v) }
        if let v = element as? UInt64 {
            guard let exact = Int64(exactly: v) else {
                throw TupleDecodingError.unsignedIntegerOverflow(
                    value: v,
                    targetType: "Int64"
                )
            }
            return exact
        }
        // Numeric coercion from floating point (exact integers only)
        // Symmetric with decodeDouble accepting Int64→Double
        if let v = element as? Double {
            guard !v.isNaN, !v.isInfinite,
                  let exact = Int64(exactly: v) else {
                throw TupleDecodingError.integerOverflow(
                    value: 0,
                    targetType: "Int64 (exact from Double \(v))"
                )
            }
            return exact
        }
        if let v = element as? Float {
            guard !v.isNaN, !v.isInfinite,
                  let exact = Int64(exactly: v) else {
                throw TupleDecodingError.integerOverflow(
                    value: 0,
                    targetType: "Int64 (exact from Float \(v))"
                )
            }
            return exact
        }
        throw TupleDecodingError.typeMismatch(expected: "Int64", actual: String(describing: type(of: element)))
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
        if let value = canonicalFieldValue(element) {
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
        if let value = element as? UInt64 { return value }
        if let value = element as? Int64 {
            guard let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.integerOverflow(
                    value: value,
                    targetType: "UInt64"
                )
            }
            return exact
        }
        if let value = element as? Int {
            guard let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.integerOverflow(
                    value: Int64(value),
                    targetType: "UInt64"
                )
            }
            return exact
        }
        if let value = element as? Double {
            guard value.isFinite, let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.typeMismatch(
                    expected: "UInt64 (exact)",
                    actual: "Double(\(value))"
                )
            }
            return exact
        }
        if let value = element as? Float {
            guard value.isFinite, let exact = UInt64(exactly: value) else {
                throw TupleDecodingError.typeMismatch(
                    expected: "UInt64 (exact)",
                    actual: "Float(\(value))"
                )
            }
            return exact
        }
        throw TupleDecodingError.typeMismatch(
            expected: "UInt64",
            actual: String(describing: type(of: element))
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
        if let value = canonicalFieldValue(element) {
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
        if let v = element as? Double { return v }
        if let v = element as? Float { return Double(v) }
        // Allow numeric coercion from integers
        if let v = element as? Int64 { return Double(v) }
        if let v = element as? Int { return Double(v) }
        throw TupleDecodingError.typeMismatch(expected: "Double", actual: String(describing: type(of: element)))
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
        if let value = canonicalFieldValue(element) {
            guard case .string(let string) = value else {
                throw canonicalTypeMismatch(
                    expected: "String",
                    value: value
                )
            }
            return string
        }
        if let v = element as? String { return v }
        throw TupleDecodingError.typeMismatch(expected: "String", actual: String(describing: type(of: element)))
    }

    // MARK: - Bool Decoding

    /// Decode as Bool
    /// - Parameter element: TupleElement to decode
    /// - Returns: Bool value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeBool(_ element: any TupleElement) throws -> Bool {
        if let value = canonicalFieldValue(element) {
            guard case .bool(let bool) = value else {
                throw canonicalTypeMismatch(
                    expected: "Bool",
                    value: value
                )
            }
            return bool
        }
        if let v = element as? Bool { return v }
        throw TupleDecodingError.typeMismatch(expected: "Bool", actual: String(describing: type(of: element)))
    }

    // MARK: - Binary Decoding

    /// Decode as Data
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Data value
    /// - Throws: TupleDecodingError on type mismatch
#if canImport(FoundationEssentials)
    public static func decodeData(
        _ element: any TupleElement
    ) throws -> FoundationEssentials.Data {
        if let value = element as? ByteString {
            // Data is the requested Foundation ownership boundary; materializing
            // independent storage prevents a borrowed tuple slice from escaping.
            return FoundationEssentials.Data(value)
        }
        throw TupleDecodingError.typeMismatch(expected: "Data", actual: String(describing: type(of: element)))
    }
#else
    public static func decodeData(
        _ element: any TupleElement
    ) throws -> Foundation.Data {
        if let value = element as? ByteString {
            return Foundation.Data(value)
        }
        throw TupleDecodingError.typeMismatch(
            expected: "Data",
            actual: String(describing: type(of: element))
        )
    }
#endif

    /// Decode as owned storage bytes
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Owned byte value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeBytes(_ element: any TupleElement) throws -> ByteString {
        if let value = element as? ByteString {
            return value
        }
        throw TupleDecodingError.typeMismatch(expected: "ByteString", actual: String(describing: type(of: element)))
    }

    // MARK: - UUID Decoding

    /// Decode as UUID
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: UUID value
    /// - Throws: TupleDecodingError on type mismatch
#if canImport(FoundationEssentials)
    public static func decodeUUID(
        _ element: any TupleElement
    ) throws -> FoundationEssentials.UUID {
        if let v = element as? FoundationEssentials.UUID { return v }
        if let v = element as? DatabaseTypes.UUID {
            return FoundationEssentials.UUID(v)
        }
        throw TupleDecodingError.typeMismatch(expected: "UUID", actual: String(describing: type(of: element)))
    }
#else
    public static func decodeUUID(
        _ element: any TupleElement
    ) throws -> Foundation.UUID {
        if let v = element as? Foundation.UUID { return v }
        if let v = element as? DatabaseTypes.UUID {
            return Foundation.UUID(v)
        }
        throw TupleDecodingError.typeMismatch(
            expected: "UUID",
            actual: String(describing: type(of: element))
        )
    }
#endif

    // MARK: - Date Decoding

    /// Decode as Date
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Date value
    /// - Throws: TupleDecodingError on type mismatch
#if canImport(FoundationEssentials)
    public static func decodeDate(
        _ element: any TupleElement
    ) throws -> FoundationEssentials.Date {
        if let v = element as? FoundationEssentials.Date { return v }
        if let v = element as? Double {
            return FoundationEssentials.Date(timeIntervalSince1970: v)
        }
        throw TupleDecodingError.typeMismatch(expected: "Date", actual: String(describing: type(of: element)))
    }
#else
    public static func decodeDate(
        _ element: any TupleElement
    ) throws -> Foundation.Date {
        if let v = element as? Foundation.Date { return v }
        if let v = element as? Double {
            return Foundation.Date(timeIntervalSince1970: v)
        }
        throw TupleDecodingError.typeMismatch(
            expected: "Date",
            actual: String(describing: type(of: element))
        )
    }
#endif

    // MARK: - Generic Decoding

    /// Decode as a specified type
    ///
    /// - Parameters:
    ///   - element: TupleElement to decode
    ///   - type: Target type
    /// - Returns: Decoded value of type T
    /// - Throws: TupleDecodingError on failure
    public static func decode<T>(_ element: any TupleElement, as type: T.Type) throws -> T {
        switch type {
        case is Int64.Type:
            return try decodeInt64(element) as! T
        case is Int.Type:
            return try decodeInt(element) as! T
        case is Int32.Type:
            return try decodeInt32(element) as! T
        case is Int16.Type:
            return try decodeInt16(element) as! T
        case is Int8.Type:
            return try decodeInt8(element) as! T
        case is UInt64.Type:
            return try decodeUInt64(element) as! T
        case is UInt.Type:
            return try decodeUInt(element) as! T
        case is UInt32.Type:
            return try decodeUInt32(element) as! T
        case is UInt16.Type:
            return try decodeUInt16(element) as! T
        case is UInt8.Type:
            return try decodeUInt8(element) as! T
        case is Double.Type:
            return try decodeDouble(element) as! T
        case is Float.Type:
            return try decodeFloat(element) as! T
        case is String.Type:
            return try decodeString(element) as! T
        case is Bool.Type:
            return try decodeBool(element) as! T
#if canImport(FoundationEssentials)
        case is FoundationEssentials.Data.Type:
            return try decodeData(element) as! T
        case is ByteString.Type:
            return try decodeBytes(element) as! T
        case is FoundationEssentials.UUID.Type:
            return try decodeUUID(element) as! T
        case is FoundationEssentials.Date.Type:
            return try decodeDate(element) as! T
#else
        case is Foundation.Data.Type:
            return try decodeData(element) as! T
        case is ByteString.Type:
            return try decodeBytes(element) as! T
        case is Foundation.UUID.Type:
            return try decodeUUID(element) as! T
        case is Foundation.Date.Type:
            return try decodeDate(element) as! T
#endif
        default:
            throw TupleDecodingError.unsupportedType(String(describing: type))
        }
    }

    private static func canonicalFieldValue(
        _ element: any TupleElement
    ) -> FieldValue? {
        (element as? CanonicalFieldValueTupleElement)?.prepared.value
    }

    private static func canonicalTypeMismatch(
        expected: String,
        value: FieldValue
    ) -> TupleDecodingError {
        .typeMismatch(
            expected: expected,
            actual: String(describing: value)
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
