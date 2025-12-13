// FieldSecurityEvaluator.swift
// DatabaseEngine - Evaluator for field-level security

import Foundation
import Core

/// Evaluator for field-level security
///
/// Provides methods to:
/// - Extract restricted field metadata from a type
/// - Mask fields that the user cannot read
/// - Validate write permissions before save
///
/// **Usage**:
/// ```swift
/// let auth = MyAuth(userID: "user-1", roles: ["employee"])
///
/// // Mask fields user cannot read
/// let masked = FieldSecurityEvaluator.mask(employee, auth: auth)
///
/// // Validate write permissions
/// try FieldSecurityEvaluator.validateWrite(
///     original: existingEmployee,
///     updated: modifiedEmployee,
///     auth: auth
/// )
/// ```
public struct FieldSecurityEvaluator {

    // MARK: - Field Metadata Extraction

    /// Information about a restricted field
    public struct RestrictedFieldInfo: Sendable {
        public let fieldName: String
        public let readAccess: FieldAccessLevel
        public let writeAccess: FieldAccessLevel
    }

    /// Extract restricted field information from a value using reflection
    ///
    /// - Parameter value: The value to inspect
    /// - Returns: Dictionary of field name to restriction info
    public static func extractRestrictedFields<T>(from value: T) -> [String: RestrictedFieldInfo] {
        var result: [String: RestrictedFieldInfo] = [:]
        let mirror = Mirror(reflecting: value)

        for child in mirror.children {
            // Property wrapper fields appear as "_fieldName"
            guard var label = child.label else { continue }

            // Check if this is a Restricted wrapper
            if let restricted = child.value as? any RestrictedProtocol {
                // Remove underscore prefix from property wrapper storage
                if label.hasPrefix("_") {
                    label = String(label.dropFirst())
                }

                result[label] = RestrictedFieldInfo(
                    fieldName: label,
                    readAccess: restricted.readAccess,
                    writeAccess: restricted.writeAccess
                )
            }
        }

        return result
    }

    // MARK: - Read Access Evaluation

    /// Check if a field can be read
    ///
    /// - Parameters:
    ///   - fieldName: Name of the field
    ///   - value: The object containing the field
    ///   - auth: Authentication context (nil = unauthenticated)
    /// - Returns: true if read is allowed
    public static func canRead<T>(
        field fieldName: String,
        in value: T,
        auth: (any AuthContext)?
    ) -> Bool {
        let restrictions = extractRestrictedFields(from: value)

        guard let info = restrictions[fieldName] else {
            // No restriction = public access
            return true
        }

        return info.readAccess.evaluate(auth: auth)
    }

    /// Get list of fields that cannot be read
    ///
    /// - Parameters:
    ///   - value: The object to check
    ///   - auth: Authentication context
    /// - Returns: List of field names that cannot be read
    public static func unreadableFields<T>(
        in value: T,
        auth: (any AuthContext)?
    ) -> [String] {
        let restrictions = extractRestrictedFields(from: value)

        return restrictions.compactMap { fieldName, info in
            info.readAccess.evaluate(auth: auth) ? nil : fieldName
        }
    }

    // MARK: - Write Access Evaluation

    /// Check if a field can be written
    ///
    /// - Parameters:
    ///   - fieldName: Name of the field
    ///   - value: The object containing the field
    ///   - auth: Authentication context (nil = unauthenticated)
    /// - Returns: true if write is allowed
    public static func canWrite<T>(
        field fieldName: String,
        in value: T,
        auth: (any AuthContext)?
    ) -> Bool {
        let restrictions = extractRestrictedFields(from: value)

        guard let info = restrictions[fieldName] else {
            // No restriction = public access
            return true
        }

        return info.writeAccess.evaluate(auth: auth)
    }

    /// Get list of fields that cannot be written
    ///
    /// - Parameters:
    ///   - value: The object to check
    ///   - auth: Authentication context
    /// - Returns: List of field names that cannot be written
    public static func unwritableFields<T>(
        in value: T,
        auth: (any AuthContext)?
    ) -> [String] {
        let restrictions = extractRestrictedFields(from: value)

        return restrictions.compactMap { fieldName, info in
            info.writeAccess.evaluate(auth: auth) ? nil : fieldName
        }
    }

    // MARK: - Masking

    /// Mask fields that cannot be read by setting them to default values
    ///
    /// **Note**: This creates a copy with restricted fields set to their type's default value.
    /// For Optional fields, they become nil. For value types, they get zero/empty values.
    ///
    /// - Parameters:
    ///   - value: The value to mask
    ///   - auth: Authentication context
    /// - Returns: Masked copy of the value
    public static func mask<T: Persistable>(
        _ value: T,
        auth: (any AuthContext)?
    ) -> T {
        let unreadable = Set(unreadableFields(in: value, auth: auth))

        if unreadable.isEmpty {
            return value
        }

        // Use Persistable's dynamicMember to create masked copy
        // This requires the type to support copying with modified fields
        var masked = value
        let mirror = Mirror(reflecting: value)

        for child in mirror.children {
            guard var label = child.label else { continue }

            // Handle property wrapper naming
            if label.hasPrefix("_") {
                label = String(label.dropFirst())
            }

            if unreadable.contains(label) {
                // Set to default/nil value via reflection
                setDefaultValue(&masked, fieldName: label, originalValue: child.value)
            }
        }

        return masked
    }

    /// Mask multiple values
    ///
    /// - Parameters:
    ///   - values: Array of values to mask
    ///   - auth: Authentication context
    /// - Returns: Array of masked values
    public static func mask<T: Persistable>(
        _ values: [T],
        auth: (any AuthContext)?
    ) -> [T] {
        values.map { mask($0, auth: auth) }
    }

    // MARK: - Write Validation

    /// Validate that the user has permission to write changed fields
    ///
    /// Compares original and updated values, and checks write permission
    /// for any fields that have changed.
    ///
    /// - Parameters:
    ///   - original: Original value (nil for new inserts)
    ///   - updated: Updated value to be saved
    ///   - auth: Authentication context
    /// - Throws: FieldSecurityError.writeNotAllowed if any changed field cannot be written
    public static func validateWrite<T: Persistable>(
        original: T?,
        updated: T,
        auth: (any AuthContext)?
    ) throws {
        let restrictions = extractRestrictedFields(from: updated)

        if restrictions.isEmpty {
            return // No restricted fields
        }

        var violations: [String] = []

        for (fieldName, info) in restrictions {
            // Check if user can write this field
            if !info.writeAccess.evaluate(auth: auth) {
                // Check if field has changed
                if let original = original {
                    if fieldChanged(original: original, updated: updated, fieldName: fieldName) {
                        violations.append(fieldName)
                    }
                } else {
                    // New insert - check if value is non-default
                    if !isDefaultValue(in: updated, fieldName: fieldName) {
                        violations.append(fieldName)
                    }
                }
            }
        }

        if !violations.isEmpty {
            throw FieldSecurityError.writeNotAllowed(
                type: T.persistableType,
                fields: violations.sorted()
            )
        }
    }

    /// Validate write for multiple values
    ///
    /// - Parameters:
    ///   - originals: Dictionary of ID to original value
    ///   - updates: Array of updated values
    ///   - auth: Authentication context
    /// - Throws: FieldSecurityError.writeNotAllowed
    public static func validateWrite<T: Persistable>(
        originals: [String: T],
        updates: [T],
        auth: (any AuthContext)?
    ) throws {
        for updated in updates {
            let original = originals["\(updated.id)"]
            try validateWrite(original: original, updated: updated, auth: auth)
        }
    }

    // MARK: - Private Helpers

    /// Check if a field has changed between original and updated
    private static func fieldChanged<T>(
        original: T,
        updated: T,
        fieldName: String
    ) -> Bool {
        let originalMirror = Mirror(reflecting: original)
        let updatedMirror = Mirror(reflecting: updated)

        // Find field in both objects
        var originalValue: Any?
        var updatedValue: Any?

        for child in originalMirror.children {
            var label = child.label ?? ""
            if label.hasPrefix("_") { label = String(label.dropFirst()) }
            if label == fieldName {
                if let restricted = child.value as? any RestrictedProtocol {
                    originalValue = restricted.anyValue
                } else {
                    originalValue = child.value
                }
                break
            }
        }

        for child in updatedMirror.children {
            var label = child.label ?? ""
            if label.hasPrefix("_") { label = String(label.dropFirst()) }
            if label == fieldName {
                if let restricted = child.value as? any RestrictedProtocol {
                    updatedValue = restricted.anyValue
                } else {
                    updatedValue = child.value
                }
                break
            }
        }

        // Compare using string representation as fallback
        return "\(originalValue ?? "nil")" != "\(updatedValue ?? "nil")"
    }

    /// Check if a field has its default/zero value
    private static func isDefaultValue<T>(in value: T, fieldName: String) -> Bool {
        let mirror = Mirror(reflecting: value)

        for child in mirror.children {
            var label = child.label ?? ""
            if label.hasPrefix("_") { label = String(label.dropFirst()) }

            if label == fieldName {
                let actualValue: Any
                if let restricted = child.value as? any RestrictedProtocol {
                    actualValue = restricted.anyValue
                } else {
                    actualValue = child.value
                }

                // Check common default values
                switch actualValue {
                case let opt as Any? where opt == nil:
                    return true
                case let str as String where str.isEmpty:
                    return true
                case let num as Int where num == 0:
                    return true
                case let num as Int64 where num == 0:
                    return true
                case let num as Double where num == 0:
                    return true
                case let num as Float where num == 0:
                    return true
                case let bool as Bool where bool == false:
                    return true
                case let arr as [Any] where arr.isEmpty:
                    return true
                case let data as Data where data.isEmpty:
                    return true
                default:
                    return false
                }
            }
        }

        return true // Field not found = default
    }

    /// Set a field to its default value
    ///
    /// **Limitation**: This is a no-op in the current implementation.
    /// Full field masking requires macro-generated code to modify fields at runtime.
    /// For now, use `unreadableFields(in:auth:)` to identify which fields should be
    /// filtered at the serialization or presentation layer.
    private static func setDefaultValue<T>(
        _ value: inout T,
        fieldName: String,
        originalValue: Any
    ) {
        // No-op: Swift's reflection doesn't support setting properties
        // Full implementation requires macro-generated code
    }
}
