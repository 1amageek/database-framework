// TupleDecoder.swift
// DatabaseEngine - Decode FoundationDB TupleElement to Swift types
//
// Single responsibility: Convert TupleElement to Swift types for index operations.

import Foundation
import FoundationDB

// MARK: - TupleDecoder

/// Decodes FoundationDB TupleElement to Swift types
///
/// **MANDATORY**: All Index modules MUST use this decoder.
/// Custom decoding implementations are PROHIBITED.
///
/// **Type Mapping**:
/// | TupleElement | Decodable Types |
/// |--------------|-----------------|
/// | Int64 | Int64, Int, Int32, Int16, Int8 (range check) |
/// | Double | Double, Float |
/// | String | String |
/// | Bool | Bool, also Int64 0/1 |
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
        if let v = element as? Int64 { return v }
        if let v = element as? Int { return Int64(v) }
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

    // MARK: - Floating Point Decoding

    /// Decode as Double
    ///
    /// Also accepts Int64/Int for numeric type coercion.
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Double value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeDouble(_ element: any TupleElement) throws -> Double {
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
        if let v = element as? String { return v }
        throw TupleDecodingError.typeMismatch(expected: "String", actual: String(describing: type(of: element)))
    }

    // MARK: - Bool Decoding

    /// Decode as Bool
    ///
    /// Also accepts Int64 (0 = false, non-zero = true) for compatibility.
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Bool value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeBool(_ element: any TupleElement) throws -> Bool {
        if let v = element as? Bool { return v }
        // Int64 0/1 can be interpreted as Bool
        if let v = element as? Int64 { return v != 0 }
        if let v = element as? Int { return v != 0 }
        throw TupleDecodingError.typeMismatch(expected: "Bool", actual: String(describing: type(of: element)))
    }

    // MARK: - Binary Decoding

    /// Decode as Data
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Data value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeData(_ element: any TupleElement) throws -> Data {
        if let v = element as? [UInt8] { return Data(v) }
        throw TupleDecodingError.typeMismatch(expected: "Data", actual: String(describing: type(of: element)))
    }

    /// Decode as [UInt8]
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Byte array
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeBytes(_ element: any TupleElement) throws -> [UInt8] {
        if let v = element as? [UInt8] { return v }
        throw TupleDecodingError.typeMismatch(expected: "[UInt8]", actual: String(describing: type(of: element)))
    }

    // MARK: - UUID Decoding

    /// Decode as UUID
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: UUID value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeUUID(_ element: any TupleElement) throws -> UUID {
        if let v = element as? UUID { return v }
        throw TupleDecodingError.typeMismatch(expected: "UUID", actual: String(describing: type(of: element)))
    }

    // MARK: - Date Decoding

    /// Decode as Date
    ///
    /// - Parameter element: TupleElement to decode
    /// - Returns: Date value
    /// - Throws: TupleDecodingError on type mismatch
    public static func decodeDate(_ element: any TupleElement) throws -> Date {
        if let v = element as? Date { return v }
        throw TupleDecodingError.typeMismatch(expected: "Date", actual: String(describing: type(of: element)))
    }

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
        case is Double.Type:
            return try decodeDouble(element) as! T
        case is Float.Type:
            return try decodeFloat(element) as! T
        case is String.Type:
            return try decodeString(element) as! T
        case is Bool.Type:
            return try decodeBool(element) as! T
        case is Data.Type:
            return try decodeData(element) as! T
        case is [UInt8].Type:
            return try decodeBytes(element) as! T
        case is UUID.Type:
            return try decodeUUID(element) as! T
        case is Date.Type:
            return try decodeDate(element) as! T
        default:
            throw TupleDecodingError.unsupportedType(String(describing: type))
        }
    }

    /// Decode as a specified type, returning nil on failure
    ///
    /// - Parameters:
    ///   - element: TupleElement to decode
    ///   - type: Target type
    /// - Returns: Decoded value or nil
    public static func decodeOrNil<T>(_ element: any TupleElement, as type: T.Type) -> T? {
        try? decode(element, as: type)
    }
}

// MARK: - TupleDecodingError

/// Errors that can occur during tuple element decoding
public enum TupleDecodingError: Error, Sendable, CustomStringConvertible {
    /// Element type does not match expected type
    case typeMismatch(expected: String, actual: String)

    /// Integer value exceeds target type range
    case integerOverflow(value: Int64, targetType: String)

    /// Target type is not supported for decoding
    case unsupportedType(String)

    public var description: String {
        switch self {
        case .typeMismatch(let expected, let actual):
            return "Type mismatch: expected \(expected), got \(actual)"
        case .integerOverflow(let value, let targetType):
            return "Integer overflow: \(value) cannot fit in \(targetType)"
        case .unsupportedType(let type):
            return "Unsupported type for decoding: \(type)"
        }
    }
}
