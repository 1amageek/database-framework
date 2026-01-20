// PathLength.swift
// GraphIndex - Variable-length path pattern specification
//
// Provides a flexible way to specify path length constraints for
// graph traversal queries, similar to Cypher's *min..max syntax.

import Foundation

// MARK: - PathLength

/// Pattern for variable-length path matching
///
/// Specifies minimum and maximum hop counts for path queries.
/// Inspired by Cypher's variable-length path syntax: `*min..max`
///
/// **Examples**:
/// ```
/// Cypher Syntax     PathLength Equivalent
/// -----------------------------------------
/// *                 .any
/// *1                .exactly(1)
/// *2..5             .range(2, 5)
/// *..3              .atMost(3)
/// *2..              .atLeast(2)
/// ```
///
/// **Usage**:
/// ```swift
/// // Find all paths between 2 and 5 hops
/// let paths = try await context.graph(Edge.self)
///     .defaultIndex()
///     .from("alice")
///     .via("follows")
///     .length(.range(2, 5))
///     .execute()
///
/// // Find all paths of any length (careful with cycles!)
/// let allPaths = try await context.graph(Edge.self)
///     .defaultIndex()
///     .from("alice")
///     .length(.atMost(10))  // Safer than .any
///     .execute()
/// ```
public struct PathLength: Sendable, Equatable, Hashable {

    // MARK: - Properties

    /// Minimum number of hops (inclusive)
    ///
    /// Must be >= 0.
    public let min: Int

    /// Maximum number of hops (inclusive)
    ///
    /// nil means unbounded (use with caution in cyclic graphs).
    public let max: Int?

    // MARK: - Static Constructors

    /// Single hop (min=1, max=1)
    ///
    /// Equivalent to Cypher: `*1`
    public static let one = PathLength(min: 1, max: 1)

    /// Any length (min=0, max=nil)
    ///
    /// **Warning**: Use with caution in cyclic graphs as this can lead
    /// to infinite loops or extremely long traversals.
    /// Consider using `.atMost(n)` with a reasonable limit instead.
    ///
    /// Equivalent to Cypher: `*`
    public static let any = PathLength(min: 0, max: nil)

    /// Zero or one hop (optional relationship)
    ///
    /// Equivalent to Cypher: `*0..1`
    public static let zeroOrOne = PathLength(min: 0, max: 1)

    /// One or more hops
    ///
    /// Equivalent to Cypher: `*1..`
    public static let oneOrMore = PathLength(min: 1, max: nil)

    // MARK: - Factory Methods

    /// Create a range pattern
    ///
    /// - Parameters:
    ///   - min: Minimum hops (inclusive)
    ///   - max: Maximum hops (inclusive)
    /// - Returns: PathLength with specified range
    ///
    /// **Example**: `.range(2, 5)` matches paths with 2, 3, 4, or 5 hops.
    ///
    /// Equivalent to Cypher: `*2..5`
    public static func range(_ min: Int, _ max: Int) -> PathLength {
        PathLength(min: Swift.max(0, min), max: Swift.max(min, max))
    }

    /// Create an exact length pattern
    ///
    /// - Parameter n: Exact number of hops
    /// - Returns: PathLength matching exactly n hops
    ///
    /// **Example**: `.exactly(3)` matches paths with exactly 3 hops.
    ///
    /// Equivalent to Cypher: `*3`
    public static func exactly(_ n: Int) -> PathLength {
        PathLength(min: Swift.max(0, n), max: Swift.max(0, n))
    }

    /// Create an "at least" pattern
    ///
    /// - Parameter n: Minimum number of hops
    /// - Returns: PathLength with min=n and max=nil
    ///
    /// **Warning**: Unbounded max - use with caution in cyclic graphs.
    ///
    /// **Example**: `.atLeast(3)` matches paths with 3 or more hops.
    ///
    /// Equivalent to Cypher: `*3..`
    public static func atLeast(_ n: Int) -> PathLength {
        PathLength(min: Swift.max(0, n), max: nil)
    }

    /// Create an "at most" pattern
    ///
    /// - Parameter n: Maximum number of hops
    /// - Returns: PathLength with min=0 and max=n
    ///
    /// **Example**: `.atMost(5)` matches paths with 0 to 5 hops.
    ///
    /// Equivalent to Cypher: `*..5`
    public static func atMost(_ n: Int) -> PathLength {
        PathLength(min: 0, max: Swift.max(0, n))
    }

    /// Create a "between" pattern (alias for range)
    ///
    /// - Parameters:
    ///   - min: Minimum hops
    ///   - max: Maximum hops
    /// - Returns: PathLength with specified range
    public static func between(_ min: Int, and max: Int) -> PathLength {
        range(min, max)
    }

    // MARK: - Initialization

    /// Create a path length specification
    ///
    /// - Parameters:
    ///   - min: Minimum number of hops (will be clamped to >= 0)
    ///   - max: Maximum number of hops (nil = unbounded)
    public init(min: Int, max: Int?) {
        self.min = Swift.max(0, min)
        if let maxValue = max {
            self.max = Swift.max(self.min, maxValue)
        } else {
            self.max = nil
        }
    }

    // MARK: - Validation

    /// Check if a path length matches this pattern
    ///
    /// - Parameter length: Path length to check
    /// - Returns: true if the length is within the specified range
    public func matches(_ length: Int) -> Bool {
        guard length >= min else { return false }
        if let max = max {
            return length <= max
        }
        return true
    }

    /// Whether this pattern is bounded (has a maximum)
    ///
    /// Unbounded patterns can be dangerous in cyclic graphs.
    public var isBounded: Bool {
        max != nil
    }

    /// Whether this pattern matches exactly one length
    public var isExact: Bool {
        max == min
    }

    /// The effective maximum for bounded traversal
    ///
    /// Returns max if bounded, otherwise returns a default limit.
    ///
    /// - Parameter defaultLimit: Default limit for unbounded patterns
    /// - Returns: Effective maximum hop count
    public func effectiveMax(defaultLimit: Int = 100) -> Int {
        max ?? defaultLimit
    }
}

// MARK: - CustomStringConvertible

extension PathLength: CustomStringConvertible {
    public var description: String {
        switch (min, max) {
        case (0, nil):
            return "*"
        case (1, nil):
            return "*1.."
        case (0, let max?):
            return "*..\(max)"
        case (let min, nil):
            return "*\(min).."
        case (let min, let max?) where min == max:
            return "*\(min)"
        case (let min, let max?):
            return "*\(min)..\(max)"
        }
    }
}

// MARK: - ExpressibleByIntegerLiteral

extension PathLength: ExpressibleByIntegerLiteral {
    /// Create an exact path length from an integer literal
    ///
    /// **Example**:
    /// ```swift
    /// let length: PathLength = 3  // Same as .exactly(3)
    /// ```
    public init(integerLiteral value: Int) {
        self = .exactly(value)
    }
}

// MARK: - Range Operators

/// Create a PathLength from a closed range
///
/// **Example**:
/// ```swift
/// let length = PathLength(2...5)  // Same as .range(2, 5)
/// ```
extension PathLength {
    public init(_ range: ClosedRange<Int>) {
        self = .range(range.lowerBound, range.upperBound)
    }

    public init(_ range: PartialRangeFrom<Int>) {
        self = .atLeast(range.lowerBound)
    }

    public init(_ range: PartialRangeThrough<Int>) {
        self = .atMost(range.upperBound)
    }

    public init(_ range: PartialRangeUpTo<Int>) {
        self = .atMost(range.upperBound - 1)
    }
}

// MARK: - Codable

extension PathLength: Codable {
    enum CodingKeys: String, CodingKey {
        case min
        case max
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let min = try container.decode(Int.self, forKey: .min)
        let max = try container.decodeIfPresent(Int.self, forKey: .max)
        self.init(min: min, max: max)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(min, forKey: .min)
        try container.encodeIfPresent(max, forKey: .max)
    }
}
