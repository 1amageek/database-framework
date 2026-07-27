// TypeConversion.swift
// DatabaseEngine - Unified type conversion utilities
//
// This file provides a single entry point for all type conversions
// between Swift native types, FieldValue, and TupleElement.
//
// Reference: Consolidates duplicate conversion logic from Filter.swift,
// AggregationExecution.swift, and index maintainers.

#if canImport(FoundationEssentials)
import FoundationEssentials
private typealias PlatformData = FoundationEssentials.Data
private typealias PlatformDate = FoundationEssentials.Date
private typealias PlatformUUID = FoundationEssentials.UUID
#else
import Foundation
private typealias PlatformData = Foundation.Data
private typealias PlatformDate = Foundation.Date
private typealias PlatformUUID = Foundation.UUID
#endif
import StorageKit
import DatabaseKit
import DatabaseTypes

/// Canonical value conversion utilities.
///
/// All framework modules use this implementation so conversion semantics stay
/// consistent across query, index, and statistics paths.
///
/// ## Type Mapping
///
/// | Swift Type       | Int64 | Double | String | FieldValue      |
/// |------------------|-------|--------|--------|-----------------|
/// | Int, Int8-64     | ✓     | ✓      | -      | .int64          |
/// | UInt, UInt8-64   | ✓*    | ✓      | -      | .uint64         |
/// | Double           | -     | ✓      | -      | .double         |
/// | Float            | -     | ✓      | -      | .double         |
/// | String           | -     | -      | ✓      | .string         |
/// | Bool             | ✓**   | -      | -      | .bool           |
/// | Date             | -     | ✓***   | -      | .double***      |
/// | UUID             | -     | -      | ✓****  | .string****     |
///
/// * A UInt64 greater than Int64.max overflows and returns nil.
/// ** Bool → Int64: true=1, false=0
/// *** Date → Double: timeIntervalSince1970
/// **** UUID → String: uuidString
///
public struct TypeConversion: Sendable {

    private init() {}

    // MARK: - Comparison and Calculation Values

    /// Extracts an Int64 for comparison or calculation.
    ///
    /// - Returns: Nil when the value has no exact supported conversion.
    public static func asInt64(_ value: Any) -> Int64? {
        switch value {
        case let v as Int64: return v
        case let v as Int: return Int64(v)
        case let v as Int32: return Int64(v)
        case let v as Int16: return Int64(v)
        case let v as Int8: return Int64(v)
        case let v as UInt: return v <= UInt(Int64.max) ? Int64(v) : nil
        case let v as UInt64: return v <= UInt64(Int64.max) ? Int64(v) : nil
        case let v as UInt32: return Int64(v)
        case let v as UInt16: return Int64(v)
        case let v as UInt8: return Int64(v)
        case let v as Bool: return v ? 1 : 0
        default: return nil
        }
    }

    /// Extracts a Double for comparison or calculation.
    ///
    /// - Returns: Nil when the value has no supported conversion.
    public static func asDouble(_ value: Any) -> Double? {
        switch value {
        case let v as Double: return v
        case let v as Float: return Double(v)
        case let v as Int64: return Double(v)
        case let v as Int: return Double(v)
        case let v as Int32: return Double(v)
        case let v as Int16: return Double(v)
        case let v as Int8: return Double(v)
        case let v as UInt64: return Double(v)
        case let v as UInt: return Double(v)
        case let v as UInt32: return Double(v)
        case let v as UInt16: return Double(v)
        case let v as UInt8: return Double(v)
        case let v as PlatformDate: return v.timeIntervalSince1970
        default: return nil
        }
    }

    /// Extracts a Float for vector calculations.
    ///
    /// - Returns: Nil when the value has no supported conversion.
    public static func asFloat(_ value: Any) -> Float? {
        switch value {
        case let v as Float: return v
        case let v as Double: return Float(v)
        case let v as Int64: return Float(v)
        case let v as Int: return Float(v)
        case let v as Int32: return Float(v)
        case let v as Int16: return Float(v)
        case let v as Int8: return Float(v)
        case let v as UInt64: return Float(v)
        case let v as UInt: return Float(v)
        case let v as UInt32: return Float(v)
        case let v as UInt16: return Float(v)
        case let v as UInt8: return Float(v)
        default: return nil
        }
    }

    /// Extracts a String for comparison.
    ///
    /// - Returns: Nil when the value has no supported conversion.
    public static func asString(_ value: Any) -> String? {
        switch value {
        case let v as String: return v
        case let v as PlatformUUID: return v.uuidString
        default: return nil
        }
    }

    // MARK: - Storage Conversion

    /// Converts a tuple element to FieldValue.
    ///
    /// Used by query execution, statistics, and HyperLogLog.
    /// - Throws: `TypeConversionError` when the value has no exact representation.
    public static func toFieldValue(
        _ value: Any
    ) throws(TypeConversionError) -> FieldValue {
        let mirror = Mirror(reflecting: value)
        if mirror.displayStyle == .optional {
            guard let wrapped = mirror.children.first?.value else {
                return .null
            }
            return try toFieldValue(wrapped)
        }

        if let fieldValue = value as? FieldValue {
            return fieldValue
        }
        if let bytes = value as? ByteString {
            return .bytes(bytes)
        }
        if let data = value as? PlatformData {
            return .bytes(
                ByteString.copying(count: data.count) { output in
                    data.withUnsafeBytes { source in
                        output.copyMemory(from: source)
                    }
                }
            )
        }
        if let bytes = value as? [UInt8] {
            return .bytes(ByteString(bytes))
        }
        if let encodable = value as? any FieldValueEncodable {
            do {
                return try encodable.encodeFieldValue()
            } catch let error {
                throw .invalidFieldValue(error)
            }
        }
        if mirror.displayStyle == .collection {
            var elements: [FieldValue] = []
            elements.reserveCapacity(mirror.children.count)
            for (index, child) in mirror.children.enumerated() {
                do {
                    elements.append(try toFieldValue(child.value))
                } catch let error {
                    throw .invalidCollectionElement(
                        index: index,
                        reason: error
                    )
                }
            }
            return .array(elements)
        }
        throw .unsupportedType(String(reflecting: type(of: value)))
    }

    /// Converts FieldValue to TupleElement.
    ///
    /// Used to construct physical index keys.
    /// Throws TupleEncodingError for unsupported values.
    public static func toTupleElement(_ value: Any) throws -> any TupleElement {
        return try TupleEncoder.encode(value)
    }

    // MARK: - Tuple Element Extraction

    /// Extracts Int64 from a tuple element.
    public static func int64(from element: any TupleElement) throws -> Int64 {
        return try TupleDecoder.decodeInt64(element)
    }

    /// Extracts Double from a tuple element.
    public static func double(from element: any TupleElement) throws -> Double {
        return try TupleDecoder.decodeDouble(element)
    }

    /// Extracts String from a tuple element.
    public static func string(from element: any TupleElement) throws -> String {
        return try TupleDecoder.decodeString(element)
    }

    /// Extracts a requested scalar type from a tuple element.
    public static func value<T>(from element: any TupleElement, as type: T.Type) throws -> T {
        return try TupleDecoder.decode(element, as: type)
    }

}
