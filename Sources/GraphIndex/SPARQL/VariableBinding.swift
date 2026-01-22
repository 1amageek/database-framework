// VariableBinding.swift
// GraphIndex - SPARQL-like variable bindings
//
// Represents a single solution (row) in a SPARQL result set.

import Foundation

/// A single binding row: variable name → value
///
/// Represents one solution in a SPARQL result set.
/// Uses String keys and String values for simplicity (matching GraphEdge pattern).
///
/// **Nullability**: Missing bindings are represented by absence from the dictionary,
/// not by nil values. This distinction matters for OPTIONAL patterns where a variable
/// may be unbound in some solutions.
///
/// **Usage**:
/// ```swift
/// var binding = VariableBinding()
/// binding = binding.binding("?person", to: "Alice")
/// binding = binding.binding("?age", to: "30")
///
/// if let person = binding["?person"] {
///     print("Person: \(person)")
/// }
/// ```
public struct VariableBinding: Sendable, Hashable {

    /// The bound values for each variable
    private var bindings: [String: String]

    // MARK: - Initialization

    /// Create an empty binding
    public init() {
        self.bindings = [:]
    }

    /// Create a binding with initial values
    public init(_ bindings: [String: String]) {
        self.bindings = bindings
    }

    // MARK: - Access

    /// Get the value bound to a variable
    ///
    /// Returns `nil` if the variable is not bound (either not in query or OPTIONAL not matched).
    public subscript(variable: String) -> String? {
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
    public var asDictionary: [String: String] {
        bindings
    }

    // MARK: - Modification

    /// Create a new binding with an additional variable bound
    ///
    /// Does not modify the original binding (immutable pattern).
    ///
    /// - Parameters:
    ///   - variable: Variable name (e.g., "?person")
    ///   - value: Value to bind
    /// - Returns: New binding with the variable bound
    public func binding(_ variable: String, to value: String) -> VariableBinding {
        var copy = self
        copy.bindings[variable] = value
        return copy
    }

    /// Create a new binding with multiple variables bound
    public func binding(_ newBindings: [String: String]) -> VariableBinding {
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
        var projected: [String: String] = [:]
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

    // MARK: - Type Conversion Helpers

    /// Get value as Int
    public func int(_ variable: String) -> Int? {
        bindings[variable].flatMap { Int($0) }
    }

    /// Get value as Int64
    public func int64(_ variable: String) -> Int64? {
        bindings[variable].flatMap { Int64($0) }
    }

    /// Get value as Double
    public func double(_ variable: String) -> Double? {
        bindings[variable].flatMap { Double($0) }
    }

    /// Get value as Bool
    public func bool(_ variable: String) -> Bool? {
        guard let value = bindings[variable] else { return nil }
        switch value.lowercased() {
        case "true", "1", "yes":
            return true
        case "false", "0", "no":
            return false
        default:
            return nil
        }
    }
}

// MARK: - CustomStringConvertible

extension VariableBinding: CustomStringConvertible {
    public var description: String {
        let pairs = bindings
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\"\($0.value)\"" }
            .joined(separator: ", ")
        return "{\(pairs)}"
    }
}

// MARK: - Collection Conformance

extension VariableBinding: Sequence {
    public func makeIterator() -> Dictionary<String, String>.Iterator {
        bindings.makeIterator()
    }
}

// MARK: - ExpressibleByDictionaryLiteral

extension VariableBinding: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, String)...) {
        var bindings: [String: String] = [:]
        for (key, value) in elements {
            bindings[key] = value
        }
        self.bindings = bindings
    }
}
