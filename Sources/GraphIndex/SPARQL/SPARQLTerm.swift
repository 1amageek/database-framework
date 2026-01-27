// SPARQLTerm.swift
// GraphIndex - SPARQL-like term representation
//
// Represents terms in triple patterns following SPARQL semantics.

import Foundation
import Core

/// Represents a term in a SPARQL-like triple pattern
///
/// **Design**: Terms can be:
/// - `.variable("?name")` - Named variable that binds to values
/// - `.value("literal")` - Exact value to match
/// - `.wildcard` - Match anything but don't bind (anonymous variable)
///
/// **Usage**:
/// ```swift
/// // Using string literals (auto-detection)
/// let term1: SPARQLTerm = "?person"  // → .variable("?person")
/// let term2: SPARQLTerm = "Alice"    // → .value("Alice")
///
/// // Explicit construction
/// let term3 = SPARQLTerm.wildcard
/// ```
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 4.1
public enum SPARQLTerm: Sendable, Hashable {
    /// Named variable (e.g., "?person", "?predicate")
    /// Variables are bound during pattern matching and can be projected in results
    case variable(String)

    /// Exact value to match (typed)
    case value(FieldValue)

    /// Anonymous wildcard - matches anything but doesn't bind
    /// Equivalent to an unnamed variable that isn't referenced elsewhere
    case wildcard

    /// Whether this term is a named variable that creates bindings
    ///
    /// - `.variable` → true (creates binding during pattern matching)
    /// - `.value` → false (concrete value, no binding)
    /// - `.wildcard` → false (matches anything but doesn't create binding)
    ///
    /// Use `isBound` to check if a term has a concrete value for index optimization.
    public var isVariable: Bool {
        if case .variable = self {
            return true
        }
        return false
    }

    /// Get the variable name if this is a named variable
    public var variableName: String? {
        if case .variable(let name) = self {
            return name
        }
        return nil
    }

    /// Get the value if this is a literal
    public var literalValue: FieldValue? {
        if case .value(let v) = self {
            return v
        }
        return nil
    }

    /// Whether this term is bound (has a concrete value)
    ///
    /// - `.value` → true (concrete value for index prefix)
    /// - `.variable` → false (needs to scan and bind)
    /// - `.wildcard` → false (needs to scan, no binding)
    ///
    /// Use this for index optimization: bound terms can be used as
    /// prefix keys for efficient range scans.
    public var isBound: Bool {
        if case .value = self {
            return true
        }
        return false
    }

    /// Whether this term is a wildcard
    public var isWildcard: Bool {
        if case .wildcard = self {
            return true
        }
        return false
    }

    /// Substitute this term using a variable binding
    ///
    /// If this is a variable and the binding contains a value for it,
    /// returns a value term. Otherwise returns self unchanged.
    ///
    /// - Parameter binding: The variable binding to substitute from
    /// - Returns: Substituted term or self if not substitutable
    public func substitute(_ binding: VariableBinding) -> SPARQLTerm {
        switch self {
        case .variable(let name):
            if let value = binding[name] {
                return .value(value)
            }
            return self
        case .value, .wildcard:
            return self
        }
    }
}

// MARK: - ExpressibleByStringLiteral

extension SPARQLTerm: ExpressibleByStringLiteral {
    /// Create a term from a string literal
    ///
    /// Strings starting with "?" are interpreted as variables.
    /// All other strings are interpreted as literal values.
    ///
    /// **Example**:
    /// ```swift
    /// let person: SPARQLTerm = "?person"  // → .variable("?person")
    /// let alice: SPARQLTerm = "Alice"     // → .value("Alice")
    /// ```
    public init(stringLiteral value: String) {
        if value.hasPrefix("?") {
            self = .variable(value)
        } else {
            self = .value(.string(value))
        }
    }
}

// MARK: - CustomStringConvertible

extension SPARQLTerm: CustomStringConvertible {
    public var description: String {
        switch self {
        case .variable(let name):
            return name
        case .value(let v):
            switch v {
            case .string(let s): return "\"\(s)\""
            case .int64(let i): return String(i)
            case .double(let d): return String(d)
            case .bool(let b): return String(b)
            default: return "\"\(v)\""
            }
        case .wildcard:
            return "_"
        }
    }
}

// MARK: - Convenience Initializers

extension SPARQLTerm {
    /// Create a variable term
    public static func `var`(_ name: String) -> SPARQLTerm {
        // Ensure variable names start with "?"
        if name.hasPrefix("?") {
            return .variable(name)
        } else {
            return .variable("?\(name)")
        }
    }

    /// Create a literal value term (string)
    public static func literal(_ value: String) -> SPARQLTerm {
        .value(.string(value))
    }

    /// Create a literal value term (integer)
    public static func literal(_ value: Int) -> SPARQLTerm {
        .value(.int64(Int64(value)))
    }

    /// Create a literal value term (Int64)
    public static func literal(_ value: Int64) -> SPARQLTerm {
        .value(.int64(value))
    }

    /// Create a literal value term (double)
    public static func literal(_ value: Double) -> SPARQLTerm {
        .value(.double(value))
    }

    /// Create a literal value term (bool)
    public static func literal(_ value: Bool) -> SPARQLTerm {
        .value(.bool(value))
    }

    /// Create a literal value term (FieldValue)
    public static func literal(_ value: FieldValue) -> SPARQLTerm {
        .value(value)
    }

    /// The wildcard term (matches anything, doesn't bind)
    public static let any = SPARQLTerm.wildcard
}
