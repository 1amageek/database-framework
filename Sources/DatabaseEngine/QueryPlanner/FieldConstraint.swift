// FieldConstraint.swift
// QueryPlanner - Field constraint types for query conditions
//
// Design:
// - ScalarConstraintType enum for constraint classification
// - Values stored as [any TupleElement] for FDB compatibility
// - Specialized constraint types for text/spatial/vector

import Core
import FoundationDB

// MARK: - ScalarConstraintType

/// Type of scalar constraint (without values)
public enum ScalarConstraintType: Sendable, Hashable {
    /// Exact equality: field = value
    case equals
    /// Not equal: field != value
    case notEquals
    /// Less than: field < value
    case lessThan
    /// Less than or equal: field <= value
    case lessThanOrEqual
    /// Greater than: field > value
    case greaterThan
    /// Greater than or equal: field >= value
    case greaterThanOrEqual
    /// Between: lower <= field <= upper (inclusivity stored separately)
    case between
    /// Membership: field IN [values]
    case `in`
    /// Not in membership: field NOT IN [values]
    case notIn
    /// Null check: field IS NULL
    case isNull
    /// Not null check: field IS NOT NULL
    case isNotNull

    /// Whether this is an equality constraint
    public var isEquality: Bool {
        self == .equals
    }

    /// Whether this is a range constraint
    public var isRange: Bool {
        switch self {
        case .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual, .between:
            return true
        default:
            return false
        }
    }

    /// Whether this is an IN constraint
    public var isIn: Bool {
        self == .in
    }

    /// Whether this is a NOT IN constraint
    public var isNotIn: Bool {
        self == .notIn
    }

    /// Whether this is a null check
    public var isNullCheck: Bool {
        self == .isNull || self == .isNotNull
    }

    /// Get the negation of this constraint type
    public var negated: ScalarConstraintType {
        switch self {
        case .equals: return .notEquals
        case .notEquals: return .equals
        case .lessThan: return .greaterThanOrEqual
        case .lessThanOrEqual: return .greaterThan
        case .greaterThan: return .lessThanOrEqual
        case .greaterThanOrEqual: return .lessThan
        case .between: return .between  // Negation of between is complex
        case .in: return .notIn
        case .notIn: return .in
        case .isNull: return .isNotNull
        case .isNotNull: return .isNull
        }
    }
}

// MARK: - ScalarConstraintBounds

/// Bounds information for range constraints
public struct ScalarConstraintBounds: Sendable {
    /// Lower bound value (as TupleElement)
    public let lower: (any TupleElement)?
    /// Upper bound value (as TupleElement)
    public let upper: (any TupleElement)?
    /// Whether lower bound is inclusive
    public let lowerInclusive: Bool
    /// Whether upper bound is inclusive
    public let upperInclusive: Bool

    public init(
        lower: (any TupleElement)? = nil,
        upper: (any TupleElement)? = nil,
        lowerInclusive: Bool = true,
        upperInclusive: Bool = true
    ) {
        self.lower = lower
        self.upper = upper
        self.lowerInclusive = lowerInclusive
        self.upperInclusive = upperInclusive
    }
}

// MARK: - Text Search Constraint

/// Constraint for full-text search
public struct TextSearchConstraint: Sendable {
    /// Search terms
    public let terms: [String]

    /// Match mode
    public let matchMode: TextMatchMode

    /// Minimum score threshold (0.0 - 1.0)
    public let minScore: Double?

    public init(terms: [String], matchMode: TextMatchMode = .any, minScore: Double? = nil) {
        self.terms = terms
        self.matchMode = matchMode
        self.minScore = minScore
    }
}

/// How to match multiple terms in full-text search
public enum TextMatchMode: Sendable {
    /// Any term must match (OR)
    case any
    /// All terms must match (AND)
    case all
    /// Exact phrase match
    case phrase
}

// MARK: - Spatial Constraint

/// Constraint for spatial queries
public struct SpatialConstraint: Sendable {
    /// The type of spatial constraint
    public let type: SpatialConstraintType

    public init(type: SpatialConstraintType) {
        self.type = type
    }
}

/// Types of spatial constraints
public enum SpatialConstraintType: Sendable {
    /// Within distance of a point
    case withinDistance(center: (latitude: Double, longitude: Double), radiusMeters: Double)

    /// Within a bounding box
    case withinBounds(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double
    )

    /// Within a polygon
    case withinPolygon(points: [(latitude: Double, longitude: Double)])
}

// MARK: - Vector Constraint

/// Constraint for vector similarity search
public struct VectorConstraint: Sendable {
    /// The query vector
    public let queryVector: [Float]

    /// Number of nearest neighbors to return
    public let k: Int

    /// Distance metric
    public let metric: VectorDistanceMetric

    /// HNSW ef_search parameter (controls recall vs speed)
    public let efSearch: Int?

    public init(
        queryVector: [Float],
        k: Int,
        metric: VectorDistanceMetric = .cosine,
        efSearch: Int? = nil
    ) {
        self.queryVector = queryVector
        self.k = k
        self.metric = metric
        self.efSearch = efSearch
    }
}

/// Distance metrics for vector similarity
public enum VectorDistanceMetric: String, Sendable {
    case cosine
    case euclidean
    case dotProduct
}

// MARK: - String Pattern Constraint

/// Constraint for string pattern matching
public struct StringPatternConstraint: Sendable {
    /// Pattern type
    public let type: StringPatternType

    /// The pattern or substring
    public let pattern: String

    /// Case sensitivity
    public let caseSensitive: Bool

    public init(type: StringPatternType, pattern: String, caseSensitive: Bool = true) {
        self.type = type
        self.pattern = pattern
        self.caseSensitive = caseSensitive
    }
}

/// Types of string pattern matching
public enum StringPatternType: Sendable {
    /// Contains substring
    case contains
    /// Starts with prefix
    case prefix
    /// Ends with suffix
    case suffix
    /// SQL-style LIKE pattern
    case like
    /// Regular expression
    case regex
}
