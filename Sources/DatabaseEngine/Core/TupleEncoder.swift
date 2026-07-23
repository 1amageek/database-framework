// TupleEncoder.swift
// DatabaseEngine - Encode Swift types to FoundationDB TupleElement
//
// Single responsibility: Convert Swift types to TupleElement for index operations.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import Core
import DatabaseValue

// MARK: - TupleEncoder

/// Encodes Swift types to FoundationDB TupleElement for index operations
///
/// **MANDATORY**: All Index modules MUST use this encoder.
/// Custom encoding implementations are PROHIBITED.
///
/// **Type Mapping**:
/// | Swift Type | TupleElement | Notes |
/// |------------|--------------|-------|
/// | String | String | Direct |
/// | Int, Int8-64 | Int64 | Widened |
/// | UInt, UInt8-64 | Int64 or UInt64 | Canonical positive integer encoding |
/// | Double | Double | IEEE 754 (FDB preserves order) |
/// | Float | Double | Widened |
/// | Bool | Bool | Direct |
/// | Date | Date | fdb-swift-bindings handles encoding |
/// | UUID | UUID | Direct |
/// | Data | [UInt8] | Converted to byte array |
///
/// **Reference**: FoundationDB Tuple Layer Specification
///
/// **Usage**:
/// ```swift
/// let element = try TupleEncoder.encode(42)
/// let elements = try TupleEncoder.encodeAll([name, age])
/// ```
public struct TupleEncoder: Sendable {

    // Private init to prevent instantiation
    private init() {}

    // MARK: - Public API

    /// Encode a single value to TupleElement
    ///
    /// - Parameter value: The value to encode
    /// - Returns: TupleElement
    /// - Throws: TupleEncodingError on failure
    public static func encode(_ value: Any) throws -> any TupleElement {
        // Handle Optional types - nil values cannot be indexed
        if let optional = value as? (any _OptionalProtocol) {
            if optional._isNil {
                throw TupleEncodingError.nilValueCannotBeEncoded
            }
            // Unwrap and recursively process the wrapped value
            if let unwrapped = optional._unwrappedAny {
                return try encode(unwrapped)
            }
            throw TupleEncodingError.nilValueCannotBeEncoded
        }

        // Handle common types
        switch value {
        case let fieldValue as FieldValue:
            return try fieldValue.toTupleElement()
        case let rdfTerm as DatabaseRDFTerm:
            return try FieldValue.rdfTerm(rdfTerm).toTupleElement()
        case let stringValue as String:
            return stringValue

        // Signed integers -> Int64
        case let intValue as Int:
            return Int64(intValue)
        case let int64Value as Int64:
            return int64Value
        case let int32Value as Int32:
            return Int64(int32Value)
        case let int16Value as Int16:
            return Int64(int16Value)
        case let int8Value as Int8:
            return Int64(int8Value)

        // Unsigned integers use the canonical positive integer tuple encoding.
        // Values through Int64.max retain the historical Int64 representation;
        // larger values use UInt64's positiveInt9-capable representation.
        case let uintValue as UInt:
            if UInt64(uintValue) <= UInt64(Int64.max) {
                return Int64(uintValue)
            }
            return UInt64(uintValue)
        case let uint64Value as UInt64:
            if uint64Value <= UInt64(Int64.max) {
                return Int64(uint64Value)
            }
            return uint64Value
        case let uint32Value as UInt32:
            return Int64(uint32Value)
        case let uint16Value as UInt16:
            return Int64(uint16Value)
        case let uint8Value as UInt8:
            return Int64(uint8Value)

        // Floating point - FDB Tuple Layer preserves Double ordering
        case let doubleValue as Double:
            return doubleValue
        case let floatValue as Float:
            return Double(floatValue)

        // Other types
        case let boolValue as Bool:
            return boolValue
        case let dateValue as Date:
            // Return Date directly - fdb-swift-bindings encodes it as
            // timeIntervalSince1970 (Double) in Tuple.encodeTuple()
            return dateValue
        case let uuidValue as UUID:
            return uuidValue
        case let databaseBytes as DatabaseBytes:
            return Bytes(retaining: databaseBytes)
        case let storageBytes as Bytes:
            return storageBytes
        case let dataValue as Data:
            return Bytes(
                retaining: DatabaseBytes(retaining: dataValue)
            )
        case let bytesValue as [UInt8]:
            return Bytes(bytesValue)
        case let tupleValue as Tuple:
            return tupleValue

        default:
            throw TupleEncodingError.unsupportedType(actualType: String(describing: type(of: value)))
        }
    }

    /// Encode a value to one or more TupleElements
    ///
    /// This method handles array types (vectors) that need to be encoded
    /// as multiple TupleElements.
    ///
    /// - Parameter value: The value to encode
    /// - Returns: Array of TupleElements
    /// - Throws: TupleEncodingError on failure
    public static func encodeToArray(_ value: Any) throws -> [any TupleElement] {
        // Handle Optional types
        if let optional = value as? (any _OptionalProtocol) {
            if optional._isNil {
                throw TupleEncodingError.nilValueCannotBeEncoded
            }
            if let unwrapped = optional._unwrappedAny {
                return try encodeToArray(unwrapped)
            }
            throw TupleEncodingError.nilValueCannotBeEncoded
        }

        // Handle array types (vectors) that produce multiple elements
        switch value {
        case let floatArray as [Float]:
            return floatArray.map { Double($0) as any TupleElement }
        case let doubleArray as [Double]:
            return doubleArray.map { $0 as any TupleElement }
        case let intArray as [Int]:
            return intArray.map { Int64($0) as any TupleElement }
        case let int64Array as [Int64]:
            return int64Array.map { $0 as any TupleElement }
        case let stringArray as [String]:
            return stringArray.map { $0 as any TupleElement }
        case let dateArray as [Date]:
            return dateArray.map { $0 as any TupleElement }
        case let arrayValue as [any TupleElement]:
            return arrayValue
        default:
            // For non-array types, wrap single element in array
            return [try encode(value)]
        }
    }

    /// Encode multiple values to TupleElements
    ///
    /// - Parameter values: Array of values to encode
    /// - Returns: Array of TupleElements
    /// - Throws: TupleEncodingError on failure
    public static func encodeAll(_ values: [Any]) throws -> [any TupleElement] {
        try values.map { try encode($0) }
    }

}

// MARK: - TupleEncodingError

/// Errors that can occur during tuple element encoding
public enum TupleEncodingError: Error, Sendable, CustomStringConvertible {
    /// Nil/Optional.none values cannot be encoded to TupleElement
    case nilValueCannotBeEncoded

    /// Type is not supported for TupleElement encoding
    case unsupportedType(actualType: String)

    public var description: String {
        switch self {
        case .nilValueCannotBeEncoded:
            return "Nil value cannot be encoded to TupleElement"
        case .unsupportedType(let actualType):
            return "Unsupported type for TupleElement encoding: \(actualType)"
        }
    }
}

// MARK: - _OptionalProtocol

/// Internal protocol for handling Optional types uniformly
///
/// This allows checking and unwrapping Optional values without
/// knowing the concrete Wrapped type at compile time.
internal protocol _OptionalProtocol {
    var _isNil: Bool { get }
    var _unwrappedAny: Any? { get }
}

extension Optional: _OptionalProtocol {
    internal var _isNil: Bool { self == nil }
    internal var _unwrappedAny: Any? {
        switch self {
        case .some(let value): return value
        case .none: return nil
        }
    }
}
