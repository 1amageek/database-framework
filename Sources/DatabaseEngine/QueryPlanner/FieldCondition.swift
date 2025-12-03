// FieldCondition.swift
// QueryPlanner - Field condition types
//
// Design:
// - FieldConditionProtocol<T> for type-erased condition storage
// - ScalarFieldCondition<T> for scalar constraints (equals, range, in)
// - Specialized types for text/spatial/vector
// - Values stored as [any TupleElement] for FDB compatibility

import Core
import FoundationDB

// MARK: - Field Reference

/// Reference to a field in a model
public struct FieldReference<T: Persistable>: @unchecked Sendable, Hashable {
    /// The KeyPath to the field (type-erased)
    public let keyPath: AnyKeyPath

    /// The field name (dot notation for nested fields)
    public let fieldName: String

    /// Create a field reference from a typed KeyPath
    public init<V>(_ keyPath: KeyPath<T, V>) {
        self.keyPath = keyPath
        self.fieldName = T.fieldName(for: keyPath)
    }

    /// Create a field reference from type-erased components
    public init(anyKeyPath: AnyKeyPath, fieldName: String) {
        self.keyPath = anyKeyPath
        self.fieldName = fieldName
    }

    // MARK: - Hashable

    public func hash(into hasher: inout Hasher) {
        hasher.combine(fieldName)
    }

    public static func == (lhs: FieldReference<T>, rhs: FieldReference<T>) -> Bool {
        lhs.fieldName == rhs.fieldName
    }
}

// MARK: - FieldConditionProtocol

/// Protocol for type-erased field conditions
///
/// Different condition types (ScalarFieldCondition<T>, TextSearchFieldCondition<T>, etc.)
/// can be stored together using `any FieldConditionProtocol<T>`.
public protocol FieldConditionProtocol<T>: Sendable {
    associatedtype T: Persistable

    /// The field name
    var fieldName: String { get }

    /// The field's KeyPath
    var keyPath: AnyKeyPath { get }

    /// Whether this is an equality constraint
    var isEquality: Bool { get }

    /// Whether this is a range constraint
    var isRange: Bool { get }

    /// Whether this is an IN constraint
    var isIn: Bool { get }

    /// Whether this is a null check
    var isNullCheck: Bool { get }

    /// Convert constraint values to TupleElements
    func constraintToTupleElements() -> [any TupleElement]

    /// Get range bounds as TupleElements for index scans
    /// Returns (lower: (element, inclusive)?, upper: (element, inclusive)?)
    func rangeBoundsAsTupleElements() -> (lower: (any TupleElement, Bool)?, upper: (any TupleElement, Bool)?)?

    /// Number of values for IN constraint (0 if not IN)
    var inValuesCount: Int { get }

    /// Unique identifier for condition matching
    var identifier: String { get }

    /// Original predicate for post-filtering
    var predicate: Predicate<T>? { get }

    /// Create a negated version of this condition
    func negated() -> any FieldConditionProtocol<T>
}

// MARK: - ScalarFieldCondition

/// Condition for scalar constraints (equals, range, in, etc.)
///
/// Values are stored as TupleElements for FDB compatibility.
public struct ScalarFieldCondition<T: Persistable>: FieldConditionProtocol, Sendable {
    /// The field being compared
    public let field: FieldReference<T>

    /// The type of constraint
    public let constraintType: ScalarConstraintType

    /// Values as TupleElements
    /// - For equals/notEquals/comparisons: single value
    /// - For between: [lower, upper]
    /// - For in/notIn: multiple values
    public let values: [any TupleElement]

    /// Bounds information for range constraints
    public let bounds: ScalarConstraintBounds?

    /// Original predicate for post-filtering
    public let sourcePredicate: Predicate<T>?

    public init(
        field: FieldReference<T>,
        constraintType: ScalarConstraintType,
        values: [any TupleElement],
        bounds: ScalarConstraintBounds? = nil,
        sourcePredicate: Predicate<T>? = nil
    ) {
        self.field = field
        self.constraintType = constraintType
        self.values = values
        self.bounds = bounds
        self.sourcePredicate = sourcePredicate
    }

    // MARK: - Convenience Initializers

    /// Create an equality condition
    public static func equals(
        field: FieldReference<T>,
        value: any TupleElement,
        predicate: Predicate<T>? = nil
    ) -> ScalarFieldCondition<T> {
        ScalarFieldCondition(
            field: field,
            constraintType: .equals,
            values: [value],
            sourcePredicate: predicate
        )
    }

    /// Create a range condition
    public static func range(
        field: FieldReference<T>,
        type: ScalarConstraintType,
        value: any TupleElement,
        inclusive: Bool = false,
        predicate: Predicate<T>? = nil
    ) -> ScalarFieldCondition<T> {
        let bounds: ScalarConstraintBounds
        switch type {
        case .lessThan, .lessThanOrEqual:
            bounds = ScalarConstraintBounds(upper: value, upperInclusive: type == .lessThanOrEqual)
        case .greaterThan, .greaterThanOrEqual:
            bounds = ScalarConstraintBounds(lower: value, lowerInclusive: type == .greaterThanOrEqual)
        default:
            bounds = ScalarConstraintBounds()
        }
        return ScalarFieldCondition(
            field: field,
            constraintType: type,
            values: [value],
            bounds: bounds,
            sourcePredicate: predicate
        )
    }

    /// Create a between condition
    public static func between(
        field: FieldReference<T>,
        lower: any TupleElement,
        upper: any TupleElement,
        lowerInclusive: Bool = true,
        upperInclusive: Bool = true,
        predicate: Predicate<T>? = nil
    ) -> ScalarFieldCondition<T> {
        ScalarFieldCondition(
            field: field,
            constraintType: .between,
            values: [lower, upper],
            bounds: ScalarConstraintBounds(
                lower: lower,
                upper: upper,
                lowerInclusive: lowerInclusive,
                upperInclusive: upperInclusive
            ),
            sourcePredicate: predicate
        )
    }

    /// Create an IN condition
    public static func `in`(
        field: FieldReference<T>,
        values: [any TupleElement],
        predicate: Predicate<T>? = nil
    ) -> ScalarFieldCondition<T> {
        ScalarFieldCondition(
            field: field,
            constraintType: .in,
            values: values,
            sourcePredicate: predicate
        )
    }

    /// Create an IS NULL condition
    public static func isNull(
        field: FieldReference<T>,
        predicate: Predicate<T>? = nil
    ) -> ScalarFieldCondition<T> {
        ScalarFieldCondition(
            field: field,
            constraintType: .isNull,
            values: [],
            sourcePredicate: predicate
        )
    }

    // MARK: - FieldConditionProtocol

    public var fieldName: String { field.fieldName }
    public var keyPath: AnyKeyPath { field.keyPath }
    public var isEquality: Bool { constraintType.isEquality }
    public var isRange: Bool { constraintType.isRange }
    public var isIn: Bool { constraintType.isIn }
    public var isNullCheck: Bool { constraintType.isNullCheck }

    public func constraintToTupleElements() -> [any TupleElement] {
        values
    }

    public func rangeBoundsAsTupleElements() -> (lower: (any TupleElement, Bool)?, upper: (any TupleElement, Bool)?)? {
        guard let bounds = bounds else {
            // For simple equals, treat as both bounds
            if constraintType == .equals, let value = values.first {
                return ((value, true), (value, true))
            }
            return nil
        }
        let lower = bounds.lower.map { ($0, bounds.lowerInclusive) }
        let upper = bounds.upper.map { ($0, bounds.upperInclusive) }
        return (lower, upper)
    }

    public var inValuesCount: Int {
        constraintType.isIn ? values.count : 0
    }

    public var identifier: String {
        let desc: String
        switch constraintType {
        case .equals: desc = "eq:\(values.first.map { "\($0)" } ?? "nil")"
        case .notEquals: desc = "ne:\(values.first.map { "\($0)" } ?? "nil")"
        case .lessThan: desc = "lt:\(values.first.map { "\($0)" } ?? "nil")"
        case .lessThanOrEqual: desc = "le:\(values.first.map { "\($0)" } ?? "nil")"
        case .greaterThan: desc = "gt:\(values.first.map { "\($0)" } ?? "nil")"
        case .greaterThanOrEqual: desc = "ge:\(values.first.map { "\($0)" } ?? "nil")"
        case .between:
            let li = bounds?.lowerInclusive ?? true
            let ui = bounds?.upperInclusive ?? true
            desc = "between:\(li ? "[" : "(")\(values.first.map { "\($0)" } ?? "nil"),\(values.dropFirst().first.map { "\($0)" } ?? "nil")\(ui ? "]" : ")")"
        case .in: desc = "in:[\(values.count)]"
        case .notIn: desc = "notIn:[\(values.count)]"
        case .isNull: desc = "isNull"
        case .isNotNull: desc = "isNotNull"
        }
        return "\(fieldName):\(desc)"
    }

    public var predicate: Predicate<T>? { sourcePredicate }

    public func negated() -> any FieldConditionProtocol<T> {
        ScalarFieldCondition(
            field: field,
            constraintType: constraintType.negated,
            values: values,
            bounds: bounds.map { b in
                // Swap bounds for range negation
                ScalarConstraintBounds(
                    lower: b.upper,
                    upper: b.lower,
                    lowerInclusive: !b.upperInclusive,
                    upperInclusive: !b.lowerInclusive
                )
            },
            sourcePredicate: sourcePredicate
        )
    }
}

// MARK: - TextSearchFieldCondition

/// Condition for full-text search
public struct TextSearchFieldCondition<T: Persistable>: FieldConditionProtocol, Sendable {
    public let field: FieldReference<T>
    public let constraint: TextSearchConstraint
    public let sourcePredicate: Predicate<T>?

    public init(field: FieldReference<T>, constraint: TextSearchConstraint, sourcePredicate: Predicate<T>? = nil) {
        self.field = field
        self.constraint = constraint
        self.sourcePredicate = sourcePredicate
    }

    public var fieldName: String { field.fieldName }
    public var keyPath: AnyKeyPath { field.keyPath }
    public var isEquality: Bool { false }
    public var isRange: Bool { false }
    public var isIn: Bool { false }
    public var isNullCheck: Bool { false }
    public func constraintToTupleElements() -> [any TupleElement] { constraint.terms.map { $0 as any TupleElement } }
    public func rangeBoundsAsTupleElements() -> (lower: (any TupleElement, Bool)?, upper: (any TupleElement, Bool)?)? { nil }
    public var inValuesCount: Int { 0 }
    public var identifier: String { "\(fieldName):text:\(constraint.terms.joined(separator: ","))" }
    public var predicate: Predicate<T>? { sourcePredicate }
    public func negated() -> any FieldConditionProtocol<T> { self }  // Text search doesn't have simple negation
}

// MARK: - SpatialFieldCondition

/// Condition for spatial queries
public struct SpatialFieldCondition<T: Persistable>: FieldConditionProtocol, Sendable {
    public let field: FieldReference<T>
    public let constraint: SpatialConstraint
    public let sourcePredicate: Predicate<T>?

    public init(field: FieldReference<T>, constraint: SpatialConstraint, sourcePredicate: Predicate<T>? = nil) {
        self.field = field
        self.constraint = constraint
        self.sourcePredicate = sourcePredicate
    }

    public var fieldName: String { field.fieldName }
    public var keyPath: AnyKeyPath { field.keyPath }
    public var isEquality: Bool { false }
    public var isRange: Bool { false }
    public var isIn: Bool { false }
    public var isNullCheck: Bool { false }
    public func constraintToTupleElements() -> [any TupleElement] { [] }
    public func rangeBoundsAsTupleElements() -> (lower: (any TupleElement, Bool)?, upper: (any TupleElement, Bool)?)? { nil }
    public var inValuesCount: Int { 0 }
    public var identifier: String { "\(fieldName):spatial" }
    public var predicate: Predicate<T>? { sourcePredicate }
    public func negated() -> any FieldConditionProtocol<T> { self }  // Spatial doesn't have simple negation
}

// MARK: - VectorFieldCondition

/// Condition for vector similarity search
public struct VectorFieldCondition<T: Persistable>: FieldConditionProtocol, Sendable {
    public let field: FieldReference<T>
    public let constraint: VectorConstraint
    public let sourcePredicate: Predicate<T>?

    public init(field: FieldReference<T>, constraint: VectorConstraint, sourcePredicate: Predicate<T>? = nil) {
        self.field = field
        self.constraint = constraint
        self.sourcePredicate = sourcePredicate
    }

    public var fieldName: String { field.fieldName }
    public var keyPath: AnyKeyPath { field.keyPath }
    public var isEquality: Bool { false }
    public var isRange: Bool { false }
    public var isIn: Bool { false }
    public var isNullCheck: Bool { false }
    public func constraintToTupleElements() -> [any TupleElement] { [] }
    public func rangeBoundsAsTupleElements() -> (lower: (any TupleElement, Bool)?, upper: (any TupleElement, Bool)?)? { nil }
    public var inValuesCount: Int { 0 }
    public var identifier: String { "\(fieldName):vector:k=\(constraint.k)" }
    public var predicate: Predicate<T>? { sourcePredicate }
    public func negated() -> any FieldConditionProtocol<T> { self }  // Vector search doesn't have simple negation
}

// MARK: - StringPatternFieldCondition

/// Condition for string pattern matching
public struct StringPatternFieldCondition<T: Persistable>: FieldConditionProtocol, Sendable {
    public let field: FieldReference<T>
    public let constraint: StringPatternConstraint
    public let sourcePredicate: Predicate<T>?

    public init(field: FieldReference<T>, constraint: StringPatternConstraint, sourcePredicate: Predicate<T>? = nil) {
        self.field = field
        self.constraint = constraint
        self.sourcePredicate = sourcePredicate
    }

    public var fieldName: String { field.fieldName }
    public var keyPath: AnyKeyPath { field.keyPath }
    public var isEquality: Bool { false }
    public var isRange: Bool { false }
    public var isIn: Bool { false }
    public var isNullCheck: Bool { false }
    public func constraintToTupleElements() -> [any TupleElement] { [constraint.pattern as any TupleElement] }
    public func rangeBoundsAsTupleElements() -> (lower: (any TupleElement, Bool)?, upper: (any TupleElement, Bool)?)? { nil }
    public var inValuesCount: Int { 0 }
    public var identifier: String { "\(fieldName):pattern:\(constraint.type):\(constraint.pattern)" }
    public var predicate: Predicate<T>? { sourcePredicate }
    public func negated() -> any FieldConditionProtocol<T> { self }  // Pattern doesn't have simple negation
}

// MARK: - Field Requirement

/// Requirements for a specific field in a query
public struct FieldRequirement: Sendable {
    /// Field name
    public let fieldName: String

    /// Types of access needed
    public let accessTypes: Set<FieldAccessType>

    /// Whether this field is used in ordering
    public let usedInOrdering: Bool

    /// Order direction if used in ordering
    public let orderDirection: SortOrder?

    public init(
        fieldName: String,
        accessTypes: Set<FieldAccessType>,
        usedInOrdering: Bool = false,
        orderDirection: SortOrder? = nil
    ) {
        self.fieldName = fieldName
        self.accessTypes = accessTypes
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
