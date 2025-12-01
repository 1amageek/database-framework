// FieldCondition.swift
// QueryPlanner - Field condition and reference types

import Core

// MARK: - Field Reference

/// Reference to a field in a model
public struct FieldReference<T: Persistable>: @unchecked Sendable, Hashable {
    /// The KeyPath to the field
    public let keyPath: AnyKeyPath

    /// The field name (dot notation for nested fields)
    public let fieldName: String

    /// The field's value type
    public let fieldType: Any.Type

    /// Create a field reference from a typed KeyPath
    public init<V>(_ keyPath: KeyPath<T, V>) {
        self.keyPath = keyPath
        self.fieldName = T.fieldName(for: keyPath)
        self.fieldType = V.self
    }

    /// Create from AnyKeyPath (internal use)
    init(anyKeyPath: AnyKeyPath, fieldName: String, fieldType: Any.Type) {
        self.keyPath = anyKeyPath
        self.fieldName = fieldName
        self.fieldType = fieldType
    }

    // MARK: - Hashable

    public func hash(into hasher: inout Hasher) {
        hasher.combine(fieldName)
    }

    public static func == (lhs: FieldReference<T>, rhs: FieldReference<T>) -> Bool {
        lhs.fieldName == rhs.fieldName
    }
}

// MARK: - Field Condition

/// Represents a condition on a single field
public struct FieldCondition<T: Persistable>: @unchecked Sendable {
    /// The field being compared
    public let field: FieldReference<T>

    /// The type of constraint
    public let constraint: FieldConstraint

    /// Original predicate for post-filtering if needed
    public let sourcePredicate: Predicate<T>?

    /// Create a field condition
    public init(
        field: FieldReference<T>,
        constraint: FieldConstraint,
        sourcePredicate: Predicate<T>? = nil
    ) {
        self.field = field
        self.constraint = constraint
        self.sourcePredicate = sourcePredicate
    }

    /// Convenience initializer with KeyPath
    public init<V>(
        keyPath: KeyPath<T, V>,
        constraint: FieldConstraint,
        sourcePredicate: Predicate<T>? = nil
    ) {
        self.field = FieldReference(keyPath)
        self.constraint = constraint
        self.sourcePredicate = sourcePredicate
    }

    /// Get the constraint value
    public var constraintValue: AnySendable {
        constraint.constraintValue ?? AnySendable(Optional<Any>.none as Any)
    }

    /// Generate a unique identifier for this condition
    ///
    /// This identifier is used for tracking satisfied conditions without requiring full Hashable conformance.
    /// Format: "fieldName:constraintType:constraintValueDescription"
    public var identifier: String {
        let constraintDesc: String
        switch constraint {
        case .equals(let value):
            constraintDesc = "eq:\(value.value)"
        case .notEquals(let value):
            constraintDesc = "ne:\(value.value)"
        case .range(let bound):
            let lower = bound.lower.map { "\($0.inclusive ? ">=" : ">")\($0.value.value)" } ?? ""
            let upper = bound.upper.map { "\($0.inclusive ? "<=" : "<")\($0.value.value)" } ?? ""
            constraintDesc = "range:\(lower),\(upper)"
        case .in(let values):
            constraintDesc = "in:[\(values.map { "\($0.value)" }.joined(separator: ","))]"
        case .notIn(let values):
            constraintDesc = "notIn:[\(values.map { "\($0.value)" }.joined(separator: ","))]"
        case .isNull(let isNull):
            constraintDesc = isNull ? "isNull" : "isNotNull"
        case .textSearch(let search):
            constraintDesc = "text:\(search.terms.joined(separator: ",")):\(search.matchMode)"
        case .spatial(let spatial):
            constraintDesc = "spatial:\(spatial.type)"
        case .vectorSimilarity(let vector):
            constraintDesc = "vector:k=\(vector.k)"
        case .stringPattern(let pattern):
            constraintDesc = "pattern:\(pattern.type):\(pattern.pattern)"
        }
        return "\(field.fieldName):\(constraintDesc)"
    }
}

// MARK: - Field Requirement

/// Requirements for a specific field in a query
public struct FieldRequirement: Sendable {
    /// Field name
    public let fieldName: String

    /// Types of access needed
    public let accessTypes: Set<FieldAccessType>

    /// Constraints on this field
    public let constraints: [FieldConstraint]

    /// Whether this field is used in ordering
    public let usedInOrdering: Bool

    /// Order direction if used in ordering
    public let orderDirection: SortOrder?

    public init(
        fieldName: String,
        accessTypes: Set<FieldAccessType>,
        constraints: [FieldConstraint],
        usedInOrdering: Bool = false,
        orderDirection: SortOrder? = nil
    ) {
        self.fieldName = fieldName
        self.accessTypes = accessTypes
        self.constraints = constraints
        self.usedInOrdering = usedInOrdering
        self.orderDirection = orderDirection
    }
}

/// Types of field access in a query
public enum FieldAccessType: Sendable, Hashable {
    /// Equality comparison (=)
    case equality
    /// Inequality comparison (!=)
    case inequality
    /// Range comparison (<, <=, >, >=)
    case range
    /// Membership check (IN)
    case membership
    /// String pattern (LIKE, CONTAINS, PREFIX)
    case pattern
    /// Sort ordering (ORDER BY)
    case ordering
    /// Full-text search
    case textSearch
    /// Spatial queries
    case spatial
    /// Vector similarity
    case vector
}
