// FieldConstraint.swift
// QueryPlanner - Field constraint types for query conditions

import Core

/// Types of constraints on a field
public enum FieldConstraint: Sendable {
    /// Exact equality: field = value
    case equals(AnySendable)

    /// Not equal: field != value
    case notEquals(AnySendable)

    /// Range: field > lower AND/OR field < upper
    case range(RangeBound)

    /// Membership: field IN [values]
    case `in`([AnySendable])

    /// Not in membership: field NOT IN [values]
    case notIn([AnySendable])

    /// Null check: field IS NULL / IS NOT NULL
    case isNull(Bool)

    /// Text search: full-text match
    case textSearch(TextSearchConstraint)

    /// Spatial: within distance/bounds
    case spatial(SpatialConstraint)

    /// Vector similarity: nearest neighbors
    case vectorSimilarity(VectorConstraint)

    /// String pattern: LIKE, PREFIX, SUFFIX, CONTAINS
    case stringPattern(StringPatternConstraint)
}

// MARK: - Range Bound

/// Range constraint with bounds
public struct RangeBound: Sendable {
    /// Lower bound (nil means unbounded)
    public let lower: Bound?

    /// Upper bound (nil means unbounded)
    public let upper: Bound?

    public init(lower: Bound? = nil, upper: Bound? = nil) {
        self.lower = lower
        self.upper = upper
    }

    /// A single bound value
    public struct Bound: Sendable {
        public let value: AnySendable
        public let inclusive: Bool

        public init(value: AnySendable, inclusive: Bool) {
            self.value = value
            self.inclusive = inclusive
        }
    }

    /// Create a greater-than range
    public static func greaterThan(_ value: AnySendable) -> RangeBound {
        RangeBound(lower: Bound(value: value, inclusive: false), upper: nil)
    }

    /// Create a greater-than-or-equal range
    public static func greaterThanOrEqual(_ value: AnySendable) -> RangeBound {
        RangeBound(lower: Bound(value: value, inclusive: true), upper: nil)
    }

    /// Create a less-than range
    public static func lessThan(_ value: AnySendable) -> RangeBound {
        RangeBound(lower: nil, upper: Bound(value: value, inclusive: false))
    }

    /// Create a less-than-or-equal range
    public static func lessThanOrEqual(_ value: AnySendable) -> RangeBound {
        RangeBound(lower: nil, upper: Bound(value: value, inclusive: true))
    }

    /// Create a between range
    public static func between(
        _ lowerValue: AnySendable,
        lowerInclusive: Bool,
        _ upperValue: AnySendable,
        upperInclusive: Bool
    ) -> RangeBound {
        RangeBound(
            lower: Bound(value: lowerValue, inclusive: lowerInclusive),
            upper: Bound(value: upperValue, inclusive: upperInclusive)
        )
    }

    /// Merge with another range bound (intersection)
    ///
    /// **⚠️ LIMITATION**: This implementation does NOT perform actual value comparison.
    /// It simply takes the first bound when both are present. For correct behavior,
    /// this would need to compare the actual values using a `Comparable` protocol.
    ///
    /// **Correct behavior** (not implemented):
    /// - For lower bounds: pick the MAX (higher = more restrictive)
    /// - For upper bounds: pick the MIN (lower = more restrictive)
    ///
    /// **Current behavior**:
    /// - Takes `self`'s bound when both present (may be incorrect)
    /// - Takes whichever bound exists when only one is present (correct)
    ///
    /// **TODO**: Implement proper value comparison when bounds use `Comparable` values.
    public func merge(with other: RangeBound) -> RangeBound {
        let newLower: Bound?
        if let l1 = self.lower, let _ = other.lower {
            // FIXME: Should compare l1.value vs l2.value and pick the MAX
            // Currently just uses self's bound (may be incorrect)
            newLower = l1
        } else {
            newLower = self.lower ?? other.lower
        }

        let newUpper: Bound?
        if let u1 = self.upper, let _ = other.upper {
            // FIXME: Should compare u1.value vs u2.value and pick the MIN
            // Currently just uses self's bound (may be incorrect)
            newUpper = u1
        } else {
            newUpper = self.upper ?? other.upper
        }

        return RangeBound(lower: newLower, upper: newUpper)
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

// MARK: - Constraint Value Extraction

extension FieldConstraint {
    /// Get the constraint value as AnySendable (for equals/in constraints)
    public var constraintValue: AnySendable? {
        switch self {
        case .equals(let value):
            return value
        case .notEquals(let value):
            return value
        case .range(let bound):
            return bound.lower?.value ?? bound.upper?.value
        case .in(let values):
            return values.first
        case .notIn(let values):
            return values.first
        case .isNull:
            return nil
        case .textSearch(let constraint):
            return AnySendable(constraint.terms)
        case .spatial:
            return nil
        case .vectorSimilarity(let constraint):
            return AnySendable(constraint.queryVector)
        case .stringPattern(let constraint):
            return AnySendable(constraint.pattern)
        }
    }

    /// Get all values for IN/NOT IN constraints
    public var allValues: [AnySendable]? {
        switch self {
        case .in(let values), .notIn(let values):
            return values
        default:
            return nil
        }
    }

    /// Whether this is an equality constraint
    public var isEquality: Bool {
        if case .equals = self { return true }
        return false
    }

    /// Whether this is a range constraint
    public var isRange: Bool {
        if case .range = self { return true }
        return false
    }

    /// Whether this is an IN constraint
    public var isIn: Bool {
        if case .in = self { return true }
        return false
    }

    /// Whether this is a NOT IN constraint
    public var isNotIn: Bool {
        if case .notIn = self { return true }
        return false
    }
}
