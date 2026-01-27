// FieldReader.swift
// DatabaseEngine - Non-throwing field access utility

import Foundation
import Core

/// Non-throwing field reader for Persistable models
///
/// Layer 2 in the 3-layer value access architecture:
/// - Layer 1: FieldComparison.evaluate(on:) / SortDescriptor.orderedComparison — zero-copy via typed closures
/// - Layer 2: FieldReader — non-throwing, returns nil for unknown fields
/// - Layer 3: DataAccess — throwing, for index key construction and serialization
///
/// FieldReader provides non-throwing field access using Persistable's dynamicMember subscript.
/// Used as a fallback when typed KeyPath closures are unavailable (e.g., after type erasure
/// in QueryRewriter/DNFConverter).
public struct FieldReader: Sendable {
    private init() {}

    /// Read a field value from a model using AnyKeyPath with fieldName fallback
    ///
    /// Attempts PartialKeyPath direct access first (O(1)), then falls back
    /// to dynamicMember string-based access.
    ///
    /// - Parameters:
    ///   - model: The model to read from
    ///   - keyPath: The AnyKeyPath to try first
    ///   - fieldName: The field name for dynamicMember fallback
    /// - Returns: The field value, or nil if not found
    public static func read<T: Persistable>(
        from model: T, keyPath: AnyKeyPath, fieldName: String
    ) -> Any? {
        if let typedKeyPath = keyPath as? PartialKeyPath<T> {
            return model[keyPath: typedKeyPath]
        }
        return read(from: model, fieldName: fieldName)
    }

    /// Read a field value from a model using field name
    ///
    /// Supports nested fields via dot notation (e.g., "address.city").
    /// First level uses Persistable's dynamicMember subscript;
    /// nested levels fall back to Mirror reflection.
    ///
    /// - Parameters:
    ///   - model: The model to read from
    ///   - fieldName: The field name (supports dot notation for nested fields)
    /// - Returns: The field value, or nil if not found
    public static func read<T: Persistable>(
        from model: T, fieldName: String
    ) -> Any? {
        // Fast path: no nested fields (common case) — avoids split/map allocation
        guard fieldName.contains(".") else {
            return model[dynamicMember: fieldName]
        }
        // Nested field path: first level via dynamicMember, rest via Mirror
        let components = fieldName.split(separator: ".").map(String.init)
        guard let first = components.first,
              let firstValue = model[dynamicMember: first] else { return nil }
        var current: Any = firstValue
        for component in components.dropFirst() {
            let mirror = Mirror(reflecting: current)
            guard let child = mirror.children.first(where: { $0.label == component }) else { return nil }
            current = child.value
        }
        return current
    }

    /// Read a field value and convert to FieldValue
    ///
    /// - Parameters:
    ///   - model: The model to read from
    ///   - fieldName: The field name
    /// - Returns: The field value as FieldValue, or .null if not found/convertible
    public static func readFieldValue<T: Persistable>(
        from model: T, fieldName: String
    ) -> FieldValue {
        guard let raw = read(from: model, fieldName: fieldName) else { return .null }
        return FieldValue(raw) ?? .null
    }
}
