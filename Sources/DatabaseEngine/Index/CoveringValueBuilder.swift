// CoveringValueBuilder.swift
// DatabaseEngine - Shared utility for covering index value construction
//
// Builds covering index values that are stored directly in the main index
// entry's value bytes. Empty-value indexes use this to embed additional
// field data for index-only scans without a separate suffix subkey.

import Foundation
import Core
import FoundationDB

/// Shared utility for building covering index values
///
/// Covering indexes store additional field values directly in the index entry's
/// value bytes, enabling index-only scans without primary key lookups.
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
    public static func build<Item: Persistable>(
        for item: Item,
        storedFieldNames: [String]
    ) throws -> [UInt8] {
        guard !storedFieldNames.isEmpty else { return [] }
        var elements: [any TupleElement] = []
        for fieldName in storedFieldNames {
            if let rawValue = item[dynamicMember: fieldName] {
                if let tupleElement = rawValue as? any TupleElement {
                    elements.append(tupleElement)
                } else if let converted = TupleEncoder.encodeOrNil(rawValue) {
                    elements.append(converted)
                } else {
                    throw TupleEncodingError.unsupportedType(
                        actualType: String(describing: type(of: rawValue)))
                }
            } else {
                elements.append("")
            }
        }
        return Tuple(elements).pack()
    }
}
