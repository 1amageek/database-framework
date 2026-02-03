// CoveringValueBuilder.swift
// DatabaseEngine - Shared utility for covering index value construction
//
// Builds covering index values that are stored directly in the main index
// entry's value bytes. Indexes use this to embed additional field data
// for index-only scans.

import Foundation
import Core
import FoundationDB

/// Shared utility for building covering index values
///
/// Covering indexes store additional field values directly in the index entry's
/// value bytes, enabling index-only scans without primary key lookups.
///
/// **Format**: `[presenceBitmap: UInt64, value1, value2, ...]`
/// - First element is a UInt64 bitmap indicating which fields are present (non-nil)
/// - Only present fields are stored in subsequent elements
/// - Supports up to 64 fields (bitmap bits)
/// - Properly distinguishes nil from empty string ("")
///
/// **Example**:
/// ```swift
/// // Fields: ["name", "age", "status"]
/// // Values: name="Alice", age=30, status=nil
/// // Bitmap: 0b011 (bits 0,1 set for name,age; bit 2 unset for status)
/// // Tuple: [3, "Alice", 30]  // status not included
/// ```
///
/// **Usage**:
/// ```swift
/// // In IndexMaintainer.updateIndex():
/// let value = try CoveringValueBuilder.build(
///     for: newItem, storedFieldNames: index.storedFieldNames
/// )
/// transaction.setValue(value, for: key)
///
/// // Clearing: just clear the key (no suffix to clean up)
/// transaction.clear(key: key)
/// ```
public enum CoveringValueBuilder {

    /// Build covering value bytes from item's storedFieldNames
    ///
    /// Returns empty array if storedFieldNames is empty.
    ///
    /// - Parameters:
    ///   - item: Item to extract field values from
    ///   - storedFieldNames: Field names to store in covering value
    /// - Returns: FDB Tuple bytes with presence bitmap and values
    /// - Throws: `TupleEncodingError` if a field value cannot be encoded
    public static func build<Item: Persistable>(
        for item: Item,
        storedFieldNames: [String]
    ) throws -> [UInt8] {
        guard !storedFieldNames.isEmpty else { return [] }

        // Check field count limit (UInt64 has 64 bits)
        guard storedFieldNames.count <= 64 else {
            throw TupleEncodingError.unsupportedType(
                actualType: "Too many fields: \(storedFieldNames.count) (max 64)"
            )
        }

        // Build presence bitmap and collect present values
        var presenceBitmap: UInt64 = 0
        var presentValues: [any TupleElement] = []

        for (index, fieldName) in storedFieldNames.enumerated() {
            if let rawValue = item[dynamicMember: fieldName] {
                // Field has a value (including empty string)
                presenceBitmap |= (1 << index)

                if let tupleElement = rawValue as? any TupleElement {
                    presentValues.append(tupleElement)
                } else if let converted = TupleEncoder.encodeOrNil(rawValue) {
                    presentValues.append(converted)
                } else {
                    throw TupleEncodingError.unsupportedType(
                        actualType: String(describing: type(of: rawValue)))
                }
            }
            // If rawValue is nil, bit remains 0 and value is not added
        }

        // Build final tuple: [bitmap, value1, value2, ...]
        var elements: [any TupleElement] = [Int64(presenceBitmap)]
        elements.append(contentsOf: presentValues)

        return Tuple(elements).pack()
    }

    /// Decode covering value bytes to property dictionary
    ///
    /// - Parameters:
    ///   - value: FDB bytes containing bitmap and values
    ///   - storedFieldNames: Field names in the same order as build()
    /// - Returns: Dictionary mapping field names to values (nil for absent fields)
    /// - Throws: `TupleDecodingError` if value format is invalid
    public static func decode(
        _ value: [UInt8],
        storedFieldNames: [String]
    ) throws -> [String: any Sendable] {
        guard !value.isEmpty else { return [:] }

        let tuple = try Tuple.unpack(from: value)
        guard tuple.count >= 1 else {
            throw TupleDecodingError.invalidFormat("Expected at least 1 element (bitmap)")
        }

        // Extract presence bitmap (tuple[0] is guaranteed to exist due to count check)
        // Note: Tuple may store small Int64 values as Int, so we need to handle both
        let firstElement: (any TupleElement)? = tuple[0]
        let bitmap: Int64
        if let int64Value = firstElement as? Int64 {
            bitmap = int64Value
        } else if let intValue = firstElement as? Int {
            bitmap = Int64(intValue)
        } else {
            let actualType = firstElement.map { String(describing: type(of: $0)) } ?? "nil"
            throw TupleDecodingError.invalidFormat("First element must be Int64 or Int bitmap, got \(actualType)")
        }
        let presenceBitmap = UInt64(bitmap)

        // Decode values based on bitmap
        var properties: [String: any Sendable] = [:]
        var valueIndex = 1  // Start after bitmap

        for (fieldIndex, fieldName) in storedFieldNames.enumerated() {
            let bitMask: UInt64 = 1 << fieldIndex

            if presenceBitmap & bitMask != 0 {
                // Field is present
                guard valueIndex < tuple.count else {
                    throw TupleDecodingError.invalidFormat(
                        "Bitmap indicates field '\(fieldName)' present but no value at index \(valueIndex)"
                    )
                }
                properties[fieldName] = tuple[valueIndex]
                valueIndex += 1
            } else {
                // Field is nil
                properties[fieldName] = nil
            }
        }

        return properties
    }
}
