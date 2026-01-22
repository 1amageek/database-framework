// ModelDiffBuilder.swift
// DatabaseEngine - Advanced model diff builder
//
// Provides detailed diff computation with array element support and custom comparators.

import Foundation
import Core

// MARK: - ModelDiffBuilder

/// Advanced model diff builder with array element support and custom comparators
///
/// **Design**: Stateless namespace following `DataAccess` pattern
/// - Supports element-level array diffing
/// - Supports custom comparators for specific fields
/// - Custom comparator results are reflected in `changeType`
///
/// **Comparison with `Persistable.diff(from:)`**:
/// - `Persistable.diff(from:)`: Basic diff, Core module
/// - `ModelDiffBuilder.diff()`: Advanced diff with options, DatabaseEngine
///
/// **Usage**:
/// ```swift
/// var options = DiffOptions()
/// options.excludeFields = ["updatedAt"]
/// options.detailedArrayDiff = true
///
/// let diff = try ModelDiffBuilder.diff(old: oldUser, new: newUser, options: options)
///
/// // Check array element changes
/// if let tagChange = diff.change(for: "tags.0") {
///     print("First tag changed from \(tagChange.oldValue) to \(tagChange.newValue)")
/// }
///
/// // Custom comparator for timestamp tolerance
/// options.customComparators["timestamp"] = { old, new in
///     guard let o = old.asDouble, let n = new.asDouble else { return old == new }
///     return abs(o - n) < 1.0  // Within 1 second
/// }
/// ```
public struct ModelDiffBuilder: Sendable {

    // Private init to prevent instantiation
    private init() {}

    // MARK: - Public API

    /// Compute diff between two model instances
    ///
    /// This method provides advanced diffing capabilities:
    /// - Element-level array diffing (optional)
    /// - Custom comparators for specific fields (results reflected in changeType)
    /// - Field exclusion
    ///
    /// - Parameters:
    ///   - old: The older model (base)
    ///   - new: The newer model (current)
    ///   - options: Diff computation options
    /// - Returns: ModelDiff containing all field changes
    /// - Throws: DiffError if comparison fails
    public static func diff<T: Persistable>(
        old: T,
        new: T,
        options: DiffOptions = DiffOptions()
    ) throws -> ModelDiff {
        var changes: [FieldChange] = []

        for fieldName in T.allFields {
            // Skip excluded fields
            if options.excludeFields.contains(fieldName) {
                continue
            }

            // Extract field values
            let oldValue = extractFieldValue(from: old, fieldPath: fieldName)
            let newValue = extractFieldValue(from: new, fieldPath: fieldName)

            // Apply custom comparator if available
            if let comparator = options.customComparators[fieldName] {
                let areEqual = comparator(oldValue, newValue)
                if !areEqual || options.includeUnchanged {
                    // When custom comparator says "equal", override changeType to .unchanged
                    // This ensures changeType reflects the custom comparison result
                    let changeTypeOverride: ChangeType? = areEqual ? .unchanged : nil
                    changes.append(FieldChange(
                        fieldPath: fieldName,
                        oldValue: oldValue,
                        newValue: newValue,
                        changeTypeOverride: changeTypeOverride
                    ))
                }
                continue
            }

            // Handle arrays with optional element-level diffing
            if case .array(let oldArray) = oldValue,
               case .array(let newArray) = newValue {
                let arrayChanges = diffArrays(
                    old: oldArray,
                    new: newArray,
                    fieldPath: fieldName,
                    options: options
                )
                changes.append(contentsOf: arrayChanges)
                continue
            }

            // Standard comparison
            if oldValue != newValue || options.includeUnchanged {
                changes.append(FieldChange(
                    fieldPath: fieldName,
                    oldValue: oldValue,
                    newValue: newValue
                ))
            }
        }

        return ModelDiff(
            typeName: T.persistableType,
            idString: "\(new.id)",
            changes: changes,
            timestamp: Date(),
            oldVersion: nil,
            newVersion: nil
        )
    }

    /// Check if two models have any differences
    ///
    /// More efficient than computing full diff when you only need to know
    /// whether changes exist.
    ///
    /// - Parameters:
    ///   - old: The older model
    ///   - new: The newer model
    ///   - excludeFields: Fields to exclude from comparison
    /// - Returns: True if any field differs
    public static func hasChanges<T: Persistable>(
        old: T,
        new: T,
        excludeFields: Set<String> = []
    ) -> Bool {
        for fieldName in T.allFields {
            if excludeFields.contains(fieldName) {
                continue
            }

            let oldValue = extractFieldValue(from: old, fieldPath: fieldName)
            let newValue = extractFieldValue(from: new, fieldPath: fieldName)

            if oldValue != newValue {
                return true
            }
        }
        return false
    }

    /// Get list of changed field names
    ///
    /// - Parameters:
    ///   - old: The older model
    ///   - new: The newer model
    ///   - excludeFields: Fields to exclude from comparison
    /// - Returns: Array of field names that differ
    public static func changedFields<T: Persistable>(
        old: T,
        new: T,
        excludeFields: Set<String> = []
    ) -> [String] {
        var changed: [String] = []

        for fieldName in T.allFields {
            if excludeFields.contains(fieldName) {
                continue
            }

            let oldValue = extractFieldValue(from: old, fieldPath: fieldName)
            let newValue = extractFieldValue(from: new, fieldPath: fieldName)

            if oldValue != newValue {
                changed.append(fieldName)
            }
        }

        return changed
    }

    // MARK: - Private Implementation

    /// Extract field value as FieldValue using dynamicMember
    private static func extractFieldValue<T: Persistable>(
        from item: T,
        fieldPath: String
    ) -> FieldValue {
        guard let value = item[dynamicMember: fieldPath] else {
            return .null
        }
        return convertToFieldValue(value)
    }

    /// Convert any Sendable value to FieldValue
    private static func convertToFieldValue(_ value: any Sendable) -> FieldValue {
        // Try direct FieldValue conversion
        if let fieldValue = FieldValue(value) {
            return fieldValue
        }

        // Try FieldValueConvertible
        if let convertible = value as? any FieldValueConvertible {
            return convertible.toFieldValue()
        }

        // Handle arrays
        if let array = value as? [any Sendable] {
            let elements = array.map { convertToFieldValue($0) }
            return .array(elements)
        }

        // Handle Optional
        if let optional = value as? (any OptionalProtocol) {
            if optional.isNil {
                return .null
            }
            if let unwrapped = optional.wrappedAny {
                return convertAnyToFieldValue(unwrapped)
            }
        }

        // Fall back to string description
        return .string(String(describing: value))
    }


    /// Diff arrays with optional element-level detail
    private static func diffArrays(
        old: [FieldValue],
        new: [FieldValue],
        fieldPath: String,
        options: DiffOptions
    ) -> [FieldChange] {
        // Skip if arrays are equal
        if old == new {
            if options.includeUnchanged {
                return [FieldChange(
                    fieldPath: fieldPath,
                    oldValue: .array(old),
                    newValue: .array(new)
                )]
            }
            return []
        }

        // Use whole-array comparison if:
        // 1. Element-level diff is disabled
        // 2. Arrays are too large
        let shouldUseWholeArray = !options.detailedArrayDiff ||
            old.count > options.maxArrayDiffSize ||
            new.count > options.maxArrayDiffSize

        if shouldUseWholeArray {
            return [FieldChange(
                fieldPath: fieldPath,
                oldValue: .array(old),
                newValue: .array(new)
            )]
        }

        // Element-level diff
        var changes: [FieldChange] = []
        let maxIndex = max(old.count, new.count)

        for i in 0..<maxIndex {
            let oldElement = i < old.count ? old[i] : .null
            let newElement = i < new.count ? new[i] : .null

            if oldElement != newElement || options.includeUnchanged {
                changes.append(FieldChange(
                    fieldPath: "\(fieldPath).\(i)",
                    oldValue: oldElement,
                    newValue: newElement
                ))
            }
        }

        return changes
    }

    /// Convert Any value to FieldValue (for Mirror-based extraction)
    private static func convertAnyToFieldValue(_ value: Any?) -> FieldValue {
        guard let value = value else {
            return .null
        }

        // Handle Optional wrapper
        let mirror = Mirror(reflecting: value)
        if mirror.displayStyle == .optional {
            if let child = mirror.children.first {
                return convertAnyToFieldValue(child.value)
            }
            return .null
        }

        // Try FieldValue conversion
        if let fieldValue = FieldValue(value) {
            return fieldValue
        }

        // Fall back to string
        return .string(String(describing: value))
    }
}

// MARK: - OptionalProtocol

/// Protocol to detect and unwrap Optional values at runtime
private protocol OptionalProtocol {
    var isNil: Bool { get }
    var wrappedAny: Any? { get }
}

extension Optional: OptionalProtocol {
    var isNil: Bool {
        self == nil
    }

    var wrappedAny: Any? {
        switch self {
        case .some(let value):
            return value
        case .none:
            return nil
        }
    }
}
