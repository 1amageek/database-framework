// VariableBinding.swift
// GraphIndex - SPARQL-like variable bindings
//
// Represents a single solution (row) in a SPARQL result set.

import DatabaseTypes
import DatabaseKit

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

    /// Query-local identity for expression functions whose semantics are scoped
    /// to one solution occurrence. It is deliberately excluded from RDF
    /// solution equality and hashing.
    private var expressionScope: UInt64?

    // MARK: - Initialization

    /// Create an empty binding
    public init() {
        self.bindings = [:]
        self.expressionScope = nil
    }

    /// Create a binding with initial values
    public init(_ bindings: [String: FieldValue]) {
        self.bindings = bindings
        self.expressionScope = nil
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

    /// Provides scoped read access to the owned binding storage.
    func withBindings<Result>(
        _ body: ([String: FieldValue]) throws -> Result
    ) rethrows -> Result {
        try body(bindings)
    }

    var expressionScopeIdentifier: UInt64? {
        expressionScope
    }

    func assigningExpressionScope(_ identifier: UInt64) -> VariableBinding {
        guard expressionScope == nil else { return self }
        var copy = self
        copy.expressionScope = identifier
        return copy
    }

    // MARK: - Type Extraction

    /// Get value as String representation
    ///
    /// Used for hexastore lookups and display. Converts all FieldValue types to string.
    public func string(_ variable: String) -> String? {
        bindings[variable]?.displayString
    }

    /// Get value as Int
    public func int(_ variable: String) -> Int? {
        guard let value = bindings[variable] else { return nil }
        if let i = value.int64Value {
            return Int(exactly: i)
        }
        if let numeric = SPARQLNumericValue(value),
           let integer = numeric.exactInteger {
            return Int(exactly: integer)
        }
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
        if let numeric = SPARQLNumericValue(value) {
            return numeric.exactInteger
        }
        if let s = value.stringValue {
            return Int64(s)
        }
        return nil
    }

    /// Get value as Double
    public func double(_ variable: String) -> Double? {
        guard let value = bindings[variable] else { return nil }
        if let number = SPARQLNumericValue(value) {
            return number.doubleValue
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

    /// Merge one value into this solution without materializing a second row.
    ///
    /// Returns `false` when the variable is already bound to a different value.
    /// Existing expression-scope identity remains attached to the solution.
    package mutating func merge(
        variable: String,
        value: FieldValue
    ) -> Bool {
        if let existing = bindings[variable] {
            return existing == value
        }
        bindings[variable] = value
        return true
    }

    /// Matches one execution term while mutating the uniquely owned binding
    /// Dictionary in place. Callers that retain the result must admit its
    /// prospective footprint before invoking this method.
    package mutating func match(
        _ term: ExecutionTerm,
        against value: FieldValue
    ) -> Bool {
        switch term {
        case .variable(let name):
            return merge(variable: name, value: value)
        case .value(let expected):
            return expected == value
        case .wildcard:
            return true
        case .tripleTerm(let subject, let predicate, let object):
            guard case .rdfTerm(
                .tripleTerm(
                    let storedSubject,
                    let storedPredicate,
                    let storedObject
                )
            ) = value else {
                return false
            }
            return match(
                subject,
                against: .rdfTerm(storedSubject.term)
            ) && match(
                predicate,
                against: .rdfTerm(storedPredicate.term)
            ) && match(
                object,
                against: .rdfTerm(storedObject)
            )
        }
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
        // Join constructs a new solution occurrence. Its expression scope is
        // assigned lazily if a later expression requires one.
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

    /// Returns whether MINUS must exclude this left solution for `other`.
    /// The check is allocation-free: it detects a non-empty shared domain and
    /// verifies equality for every shared variable in one Dictionary scan.
    package func isMinusCompatible(
        with other: borrowing VariableBinding
    ) -> Bool {
        var hasSharedVariable = false
        for (variable, value) in bindings {
            guard let otherValue = other.bindings[variable] else {
                continue
            }
            hasSharedVariable = true
            guard value == otherValue else { return false }
        }
        return hasSharedVariable
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
        var projectedEntryCount = 0
        for variable in variables where bindings[variable] != nil {
            projectedEntryCount += 1
        }
        var projected: [String: FieldValue] = [:]
        projected.reserveCapacity(projectedEntryCount)
        for variable in variables {
            if let value = bindings[variable] {
                projected[variable] = value
            }
        }
        var result = VariableBinding(projected)
        result.expressionScope = expressionScope
        return result
    }

    /// Project to only the specified variables (array version)
    public func project(_ variables: [String]) -> VariableBinding {
        var projectedEntryCount = 0
        for variable in variables where bindings[variable] != nil {
            projectedEntryCount += 1
        }
        var projected: [String: FieldValue] = [:]
        projected.reserveCapacity(projectedEntryCount)
        for variable in variables {
            if let value = bindings[variable] {
                projected[variable] = value
            }
        }
        var result = VariableBinding(projected)
        result.expressionScope = expressionScope
        return result
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
        self.expressionScope = nil
    }
}

extension VariableBinding {
    public static func == (lhs: VariableBinding, rhs: VariableBinding) -> Bool {
        lhs.bindings == rhs.bindings
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(bindings)
    }
}

// MARK: - FieldValue String Conversion

extension FieldValue {
    /// Convert to a display string representation
    ///
    /// Returns `nil` for values that do not have a SPARQL lexical form.
    /// Shared by `VariableBinding.string()` and `GroupValue.stringValue`.
    var displayString: String? {
        switch self {
        case .string(let s): return s
        case .int8(let i): return String(i)
        case .int16(let i): return String(i)
        case .int32(let i): return String(i)
        case .int64(let i): return String(i)
        case .uint8(let i): return String(i)
        case .uint16(let i): return String(i)
        case .uint32(let i): return String(i)
        case .uint64(let i): return String(i)
        case .float32(let value): return String(value)
        case .float64(let value): return String(value)
        case .decimal(let decimal):
            do {
                return try decimal.decimalLexicalForm(
                    maximumUTF8Count:
                        SPARQLExecutionLimits.maximumLiteralUTF8Count
                )
            } catch {
                return nil
            }
        case .bool(let b): return String(b)
        case .rdfTerm(let term):
            switch term {
            case .iri(let iri): return iri.rawValue
            case .blankNode(let identifier):
                return "_:\(identifier.rawValue)"
            case .literal(let literal): return literal.lexicalForm
            case .tripleTerm: return term.description
            }
        case .null: return nil
        case .bytes, .timestamp, .date, .time, .dateTime, .timeSpan,
             .calendarPeriod, .uuid, .geographicPoint,
             .geographicPosition, .vector, .array, .object, .reference:
            return nil
        }
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
        fieldValue?.displayString
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
