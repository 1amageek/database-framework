// VariableBinding.swift
// GraphIndex - SPARQL-like variable bindings
//
// Represents a single solution (row) in a SPARQL result set.

import Foundation
import Core

/// A single binding row: variable name → typed value
///
/// Represents one solution in a SPARQL result set.
/// Uses `FieldValue` from Core to preserve type information (int64, double, string, etc.).
///
/// **Nullability**: Missing bindings are represented by absence from the dictionary,
/// not by nil values. This distinction matters for OPTIONAL patterns where a variable
/// may be unbound in some solutions.
///
/// **Usage**:
/// ```swift
/// var binding = VariableBinding()
/// binding = binding.binding("?person", to: .string("Alice"))
/// binding = binding.binding("?age", to: .int64(30))
///
/// if let person = binding.string("?person") {
///     print("Person: \(person)")
/// }
/// ```
public struct VariableBinding: Sendable, Hashable {

    /// The bound values for each variable
    private var bindings: [String: FieldValue]

    // MARK: - Initialization

    /// Create an empty binding
    public init() {
        self.bindings = [:]
    }

    /// Create a binding with initial values
    public init(_ bindings: [String: FieldValue]) {
        self.bindings = bindings
    }

    // MARK: - Access

    /// Get the typed value bound to a variable
    ///
    /// Returns `nil` if the variable is not bound (either not in query or OPTIONAL not matched).
    public subscript(variable: String) -> FieldValue? {
        bindings[variable]
    }

    /// All variable names that have bindings
    public var boundVariables: Set<String> {
        Set(bindings.keys)
    }

    /// Number of bound variables
    public var count: Int {
        bindings.count
    }

    /// Whether this binding is empty (no variables bound)
    public var isEmpty: Bool {
        bindings.isEmpty
    }

    /// Check if a variable is bound
    public func isBound(_ variable: String) -> Bool {
        bindings[variable] != nil
    }

    /// Get all bindings as a dictionary
    public var asDictionary: [String: FieldValue] {
        bindings
    }

    // MARK: - Type Extraction

    /// Get value as String representation
    ///
    /// Used for hexastore lookups and display. Converts all FieldValue types to string.
    public func string(_ variable: String) -> String? {
        guard let value = bindings[variable] else { return nil }
        switch value {
        case .string(let s): return s
        case .int64(let i): return String(i)
        case .double(let d): return String(d)
        case .bool(let b): return String(b)
        case .null: return nil
        case .data, .array: return nil
        }
    }

    /// Get value as Int
    public func int(_ variable: String) -> Int? {
        guard let value = bindings[variable] else { return nil }
        if let i = value.int64Value {
            return Int(i)
        }
        // Fallback: try parsing string
        if let s = value.stringValue {
            return Int(s)
        }
        return nil
    }

    /// Get value as Int64
    public func int64(_ variable: String) -> Int64? {
        guard let value = bindings[variable] else { return nil }
        if let i = value.int64Value {
            return i
        }
        // Fallback: try parsing string
        if let s = value.stringValue {
            return Int64(s)
        }
        return nil
    }

    /// Get value as Double
    public func double(_ variable: String) -> Double? {
        guard let value = bindings[variable] else { return nil }
        if let d = value.asDouble {
            return d
        }
        // Fallback: try parsing string
        if let s = value.stringValue {
            return Double(s)
        }
        return nil
    }

    /// Get value as Bool
    public func bool(_ variable: String) -> Bool? {
        guard let value = bindings[variable] else { return nil }
        if let b = value.boolValue {
            return b
        }
        // Fallback: try parsing string
        if let s = value.stringValue {
            switch s.lowercased() {
            case "true", "1", "yes": return true
            case "false", "0", "no": return false
            default: return nil
            }
        }
        return nil
    }

    // MARK: - Modification

    /// Create a new binding with an additional variable bound to a FieldValue
    ///
    /// Does not modify the original binding (immutable pattern).
    ///
    /// - Parameters:
    ///   - variable: Variable name (e.g., "?person")
    ///   - value: Typed value to bind
    /// - Returns: New binding with the variable bound
    public func binding(_ variable: String, to value: FieldValue) -> VariableBinding {
        var copy = self
        copy.bindings[variable] = value
        return copy
    }

    /// Create a new binding with an additional variable bound to a String
    ///
    /// Convenience method that wraps the string in `.string()`.
    public func binding(_ variable: String, toString value: String) -> VariableBinding {
        binding(variable, to: .string(value))
    }

    /// Create a new binding with multiple variables bound
    public func binding(_ newBindings: [String: FieldValue]) -> VariableBinding {
        var copy = self
        for (key, value) in newBindings {
            copy.bindings[key] = value
        }
        return copy
    }

    // MARK: - Merging (for joins)

    /// Merge two bindings (for joins)
    ///
    /// Returns `nil` if there's a conflict (same variable, different values).
    /// This implements the merge-join semantics: shared variables must have equal values.
    ///
    /// - Parameter other: Binding to merge with
    /// - Returns: Merged binding, or `nil` if there's a conflict
    public func merged(with other: VariableBinding) -> VariableBinding? {
        var result = self.bindings
        for (key, value) in other.bindings {
            if let existing = result[key] {
                // Conflict check: same variable must have same value
                if existing != value {
                    return nil
                }
            } else {
                result[key] = value
            }
        }
        return VariableBinding(result)
    }

    /// Check if this binding is compatible with another (for joins)
    ///
    /// Compatible means: shared variables have the same values.
    /// Used to filter candidate bindings before full merge.
    ///
    /// - Parameter other: Binding to check compatibility with
    /// - Returns: `true` if bindings can be merged without conflict
    public func isCompatible(with other: VariableBinding) -> Bool {
        for key in bindings.keys {
            if let otherValue = other.bindings[key] {
                if bindings[key] != otherValue {
                    return false
                }
            }
        }
        return true
    }

    // MARK: - Projection

    /// Project to only the specified variables
    ///
    /// Creates a new binding containing only the specified variables.
    /// Variables not in the original binding are omitted from the result.
    ///
    /// - Parameter variables: Variables to keep
    /// - Returns: New binding with only the specified variables
    public func project(_ variables: Set<String>) -> VariableBinding {
        var projected: [String: FieldValue] = [:]
        for variable in variables {
            if let value = bindings[variable] {
                projected[variable] = value
            }
        }
        return VariableBinding(projected)
    }

    /// Project to only the specified variables (array version)
    public func project(_ variables: [String]) -> VariableBinding {
        project(Set(variables))
    }
}

// MARK: - CustomStringConvertible

extension VariableBinding: CustomStringConvertible {
    public var description: String {
        let pairs = bindings
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ", ")
        return "{\(pairs)}"
    }
}

// MARK: - Sequence Conformance

extension VariableBinding: Sequence {
    public func makeIterator() -> Dictionary<String, FieldValue>.Iterator {
        bindings.makeIterator()
    }
}

// MARK: - ExpressibleByDictionaryLiteral

extension VariableBinding: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, FieldValue)...) {
        var bindings: [String: FieldValue] = [:]
        for (key, value) in elements {
            bindings[key] = value
        }
        self.bindings = bindings
    }
}

// MARK: - GroupValue

/// GROUP BY key value wrapper that distinguishes null/unbound from bound values
///
/// SPARQL 1.1 Section 11.2 requires that unbound values be distinct from any bound value,
/// including the empty string. This enum provides type-safe handling of nullability
/// in GROUP BY keys.
///
/// **Reference**: https://www.w3.org/TR/sparql11-query/#aggregateExample
public enum GroupValue: Sendable, Hashable, Comparable {

    /// Variable is bound to a typed value
    case bound(FieldValue)

    /// Variable is unbound (NULL in SPARQL semantics)
    case unbound

    // MARK: - Initialization

    /// Create from an optional FieldValue
    ///
    /// - Parameter optional: The optional value from VariableBinding subscript
    public init(from optional: FieldValue?) {
        if let value = optional {
            self = .bound(value)
        } else {
            self = .unbound
        }
    }

    // MARK: - Access

    /// Get the FieldValue if bound, nil if unbound
    public var fieldValue: FieldValue? {
        switch self {
        case .bound(let value):
            return value
        case .unbound:
            return nil
        }
    }

    /// Get the string representation if bound, nil if unbound
    public var stringValue: String? {
        switch self {
        case .bound(let value):
            switch value {
            case .string(let s): return s
            case .int64(let i): return String(i)
            case .double(let d): return String(d)
            case .bool(let b): return String(b)
            default: return nil
            }
        case .unbound:
            return nil
        }
    }

    /// Whether this value is bound
    public var isBound: Bool {
        if case .bound = self {
            return true
        }
        return false
    }

    // MARK: - Comparable

    /// Comparison: unbound sorts after all bound values
    ///
    /// This ordering ensures deterministic GROUP BY result ordering:
    /// - bound values use FieldValue.Comparable
    /// - unbound sorts after all bound values
    public static func < (lhs: GroupValue, rhs: GroupValue) -> Bool {
        switch (lhs, rhs) {
        case (.bound(let l), .bound(let r)):
            return l < r
        case (.unbound, .bound):
            return false  // unbound sorts after bound
        case (.bound, .unbound):
            return true   // bound sorts before unbound
        case (.unbound, .unbound):
            return false
        }
    }
}

// MARK: - GroupValue CustomStringConvertible

extension GroupValue: CustomStringConvertible {
    public var description: String {
        switch self {
        case .bound(let value):
            return "\(value)"
        case .unbound:
            return "UNBOUND"
        }
    }
}
