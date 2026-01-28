// BindingSorter.swift
// GraphIndex - ORDER BY sorting for SPARQL query results
//
// Sorts VariableBinding arrays by multiple keys with configurable direction
// and null ordering. Follows SPARQL 1.1 Section 15 ordering semantics.
//
// Reference: W3C SPARQL 1.1 Query Language, Section 15 (Solution Sequences and Modifiers)

import Foundation
import Core

/// A single ORDER BY sort key for VariableBinding sorting
///
/// Encapsulates the evaluation function, sort direction, and null ordering
/// for one component of a multi-key sort.
///
/// **Usage**:
/// ```swift
/// // Simple variable sort
/// let key = BindingSortKey.variable("?name")
///
/// // Descending with nulls last
/// let key = BindingSortKey.variable("?age", ascending: false, nullsLast: true)
///
/// // Custom evaluation
/// let key = BindingSortKey(ascending: true) { binding in binding["?score"] }
/// ```
public struct BindingSortKey: Sendable {

    /// Evaluates a binding to produce the sort value
    public let evaluate: @Sendable (VariableBinding) -> FieldValue?

    /// Sort direction: true = ascending (ASC), false = descending (DESC)
    public let ascending: Bool

    /// Whether null/unbound values sort last (true) or first (false)
    ///
    /// SPARQL 1.1 default: nulls sort as smallest (first in ASC, last in DESC).
    /// This property overrides the default behavior.
    public let nullsLast: Bool

    // MARK: - Initialization

    /// Create a sort key with a custom evaluation function
    ///
    /// - Parameters:
    ///   - ascending: Sort direction (default: true = ASC)
    ///   - nullsLast: Whether nulls sort last (default: false = nulls sort first)
    ///   - evaluate: Function to extract the sort value from a binding
    public init(
        ascending: Bool = true,
        nullsLast: Bool = false,
        evaluate: @escaping @Sendable (VariableBinding) -> FieldValue?
    ) {
        self.ascending = ascending
        self.nullsLast = nullsLast
        self.evaluate = evaluate
    }

    // MARK: - Convenience Constructors

    /// Create a sort key from a variable name
    ///
    /// - Parameters:
    ///   - name: Variable name (e.g., "?person")
    ///   - ascending: Sort direction (default: true = ASC)
    ///   - nullsLast: Whether nulls sort last (default: false)
    /// - Returns: A BindingSortKey that extracts the named variable's value
    public static func variable(
        _ name: String,
        ascending: Bool = true,
        nullsLast: Bool = false
    ) -> BindingSortKey {
        BindingSortKey(
            ascending: ascending,
            nullsLast: nullsLast,
            evaluate: { binding in binding[name] }
        )
    }
}

/// Sorts VariableBinding arrays by multiple sort keys
///
/// Implements multi-key sorting following SPARQL 1.1 ORDER BY semantics:
/// 1. Compare by first key; if equal, compare by second key, etc.
/// 2. Null/unbound values are ordered according to `nullsLast` setting.
/// 3. Incomparable types use `FieldValue.Comparable` type ordering.
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 15.1
public struct BindingSorter: Sendable {

    /// Sort bindings by multiple keys
    ///
    /// - Parameters:
    ///   - bindings: The bindings to sort
    ///   - keys: Ordered list of sort keys (primary key first)
    /// - Returns: Sorted array of bindings
    public static func sort(
        _ bindings: [VariableBinding],
        by keys: [BindingSortKey]
    ) -> [VariableBinding] {
        guard !keys.isEmpty, bindings.count > 1 else {
            return bindings
        }

        return bindings.sorted { lhs, rhs in
            for key in keys {
                let lVal = key.evaluate(lhs)
                let rVal = key.evaluate(rhs)

                let result = compareValues(lVal, rVal, nullsLast: key.nullsLast)

                switch result {
                case .orderedSame:
                    continue // Tie on this key, try next
                case .orderedAscending:
                    return key.ascending
                case .orderedDescending:
                    return !key.ascending
                }
            }
            return false // All keys equal, maintain relative order
        }
    }

    // MARK: - Private

    /// Compare two optional FieldValues with null handling
    ///
    /// - Parameters:
    ///   - lhs: Left value (nil = unbound/null)
    ///   - rhs: Right value (nil = unbound/null)
    ///   - nullsLast: Whether nulls sort after all non-null values
    /// - Returns: Comparison result
    private static func compareValues(
        _ lhs: FieldValue?,
        _ rhs: FieldValue?,
        nullsLast: Bool
    ) -> ComparisonResult {
        switch (lhs, rhs) {
        case (.none, .none):
            return .orderedSame
        case (.none, .some):
            return nullsLast ? .orderedDescending : .orderedAscending
        case (.some, .none):
            return nullsLast ? .orderedAscending : .orderedDescending
        case (.some(.null), .some(.null)):
            return .orderedSame
        case (.some(.null), .some):
            return nullsLast ? .orderedDescending : .orderedAscending
        case (.some, .some(.null)):
            return nullsLast ? .orderedAscending : .orderedDescending
        case (.some(let l), .some(let r)):
            // Use FieldValue.compare(to:) for type-aware comparison
            if let cmp = l.compare(to: r) {
                return cmp
            }
            // Fallback: use Comparable conformance for cross-type ordering
            if l < r { return .orderedAscending }
            if r < l { return .orderedDescending }
            return .orderedSame
        }
    }
}
