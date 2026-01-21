// TupleElementConverter.swift
// DatabaseEngine - Type conversion for FoundationDB Tuple elements
//
// Single responsibility: Convert Swift types to TupleElement for index operations.

import Foundation
import FoundationDB

// MARK: - TupleElementConverter

/// Converts Swift types to FoundationDB TupleElement for index operations
///
/// This is the canonical type conversion utility for all index operations.
/// Index modules should use this instead of implementing their own conversion.
///
/// **Design**: Stateless utility with static methods (single responsibility)
///
/// **Supported Types**:
/// - String, Int (all sizes), UInt (all sizes with overflow check)
/// - Double, Float, Bool, UUID, Data, [UInt8], Tuple, Date
/// - Arrays of the above types
///
/// **Type Mapping**:
/// | Swift Type | TupleElement Type | Notes |
/// |------------|-------------------|-------|
/// | Int, Int8-64 | Int64 | Widened to Int64 |
/// | UInt, UInt8-64 | Int64 | Overflow check for values > Int64.max |
/// | Float | Double | Widened to Double |
/// | Date | Date | Encoded as timeIntervalSince1970 by fdb-swift-bindings |
/// | Data | [UInt8] | Converted to byte array |
///
/// **Reference**: FoundationDB Tuple Layer specification
///
/// **Usage**:
/// ```swift
/// // Single value conversion
/// let elements = try TupleElementConverter.convert(someValue)
///
/// // First element extraction (for index key building)
/// let element = try TupleElementConverter.convertSingle(someValue)
/// ```
public struct TupleElementConverter: Sendable {

    // Private init to prevent instantiation
    private init() {}

    // MARK: - Public API

    /// Convert any value to TupleElements
    ///
    /// - Parameter value: The value to convert
    /// - Returns: Array of TupleElements
    /// - Throws: TupleConversionError on failure
    public static func convert(_ value: Any) throws -> [any TupleElement] {
        // Handle Optional types - nil values cannot be indexed
        if let optional = value as? (any _OptionalProtocol) {
            if optional._isNil {
                throw TupleConversionError.nilValueCannotBeConverted
            }
            // Unwrap and recursively process the wrapped value
            if let unwrapped = optional._unwrappedAny {
                return try convert(unwrapped)
            }
            throw TupleConversionError.nilValueCannotBeConverted
        }

        // Handle common types
        switch value {
        case let stringValue as String:
            return [stringValue]
        case let intValue as Int:
            return [Int64(intValue)]
        case let int64Value as Int64:
            return [int64Value]
        case let int32Value as Int32:
            return [Int64(int32Value)]
        case let int16Value as Int16:
            return [Int64(int16Value)]
        case let int8Value as Int8:
            return [Int64(int8Value)]
        // Unsigned integers: keep as Int64 but check for overflow
        case let uintValue as UInt:
            guard uintValue <= UInt(Int64.max) else {
                throw TupleConversionError.integerOverflow(value: UInt64(uintValue), targetType: "Int64")
            }
            return [Int64(uintValue)]
        case let uint64Value as UInt64:
            guard uint64Value <= UInt64(Int64.max) else {
                throw TupleConversionError.integerOverflow(value: uint64Value, targetType: "Int64")
            }
            return [Int64(uint64Value)]
        case let uint32Value as UInt32:
            return [Int64(uint32Value)]
        case let uint16Value as UInt16:
            return [Int64(uint16Value)]
        case let uint8Value as UInt8:
            return [Int64(uint8Value)]
        case let doubleValue as Double:
            return [doubleValue]
        case let floatValue as Float:
            return [Double(floatValue)]
        case let boolValue as Bool:
            return [boolValue]
        case let dateValue as Date:
            // Return Date directly - fdb-swift-bindings encodes it as
            // timeIntervalSince1970 (Double) in Tuple.encodeTuple()
            return [dateValue]
        case let uuidValue as UUID:
            return [uuidValue]
        case let dataValue as Data:
            return [Array(dataValue)]
        case let bytesValue as [UInt8]:
            return [bytesValue]
        case let tupleValue as Tuple:
            return [tupleValue]
        // Vector/numeric arrays - convert each element to TupleElement
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
            throw TupleConversionError.unsupportedType(actualType: String(describing: type(of: value)))
        }
    }

    /// Convert a single value to a single TupleElement
    ///
    /// Convenience method for index key building where exactly one element is expected.
    ///
    /// - Parameter value: The value to convert
    /// - Returns: Single TupleElement
    /// - Throws: TupleConversionError on failure or if conversion produces multiple elements
    public static func convertSingle(_ value: Any) throws -> any TupleElement {
        let elements = try convert(value)
        guard let first = elements.first else {
            throw TupleConversionError.emptyConversionResult
        }
        if elements.count > 1 {
            throw TupleConversionError.multipleElementsNotAllowed(count: elements.count)
        }
        return first
    }

    /// Convert a single value, returning the first element if multiple are produced
    ///
    /// Use this when building index keys where only the first element matters.
    ///
    /// - Parameter value: The value to convert
    /// - Returns: First TupleElement
    /// - Throws: TupleConversionError on failure
    public static func convertFirst(_ value: Any) throws -> any TupleElement {
        let elements = try convert(value)
        guard let first = elements.first else {
            throw TupleConversionError.emptyConversionResult
        }
        return first
    }
}

// MARK: - TupleConversionError

/// Errors that can occur during tuple element conversion
public enum TupleConversionError: Error, Sendable {
    /// Nil/Optional.none values cannot be converted to TupleElement
    case nilValueCannotBeConverted

    /// Integer value exceeds target type range
    case integerOverflow(value: UInt64, targetType: String)

    /// Type is not supported for TupleElement conversion
    case unsupportedType(actualType: String)

    /// Conversion produced no elements
    case emptyConversionResult

    /// Expected single element but got multiple
    case multipleElementsNotAllowed(count: Int)
}

// MARK: - _OptionalProtocol

/// Internal protocol for handling Optional types uniformly
///
/// This allows checking and unwrapping Optional values without
/// knowing the concrete Wrapped type at compile time.
///
/// **Note**: Internal visibility to allow sharing within DatabaseEngine module.
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
