// Expression.swift
// Cascades Optimizer - Logical and Physical Expression representations
//
// Reference: Graefe, G. "The Cascades Framework for Query Optimization", 1995
// https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/graefe-ieee1995.pdf

import Foundation
import Core

// MARK: - Expression Types

/// Unique identifier for a group in the Memo
public struct GroupID: Hashable, Sendable, CustomStringConvertible {
    public let id: Int

    public init(_ id: Int) {
        self.id = id
    }

    public var description: String { "G\(id)" }
}

/// Unique identifier for an expression
public struct ExpressionID: Hashable, Sendable, CustomStringConvertible {
    public let groupID: GroupID
    public let index: Int

    public init(groupID: GroupID, index: Int) {
        self.groupID = groupID
        self.index = index
    }

    public var description: String { "\(groupID).E\(index)" }
}

// MARK: - Logical Operators

/// Logical operators represent relational algebra operations
///
/// These are implementation-independent representations of query operations.
/// The optimizer transforms these into physical operators with specific
/// algorithms and access paths.
public enum LogicalOperator: Sendable, Equatable {
    /// Table scan - read all records of a type
    case scan(typeName: String)

    /// Filter - apply predicate to input
    case filter(input: GroupID, predicate: PredicateExpr)

    /// Project - select specific fields
    case project(input: GroupID, fields: [String])

    /// Inner join
    case join(left: GroupID, right: GroupID, condition: PredicateExpr, type: JoinType)

    /// Union of two inputs
    case union(inputs: [GroupID], deduplicate: Bool)

    /// Intersection of two inputs
    case intersection(inputs: [GroupID])

    /// Sort by keys
    case sort(input: GroupID, keys: [SortKeyExpr])

    /// Limit with optional offset
    case limit(input: GroupID, count: Int, offset: Int?)

    /// Aggregation with grouping
    case aggregate(input: GroupID, groupBy: [String], aggregates: [AggregateExpr])

    /// Index scan hint (used during transformation)
    case indexScan(typeName: String, indexName: String, bounds: IndexBoundsExpr?)
}

/// Join types
public enum JoinType: Sendable, Equatable, Hashable {
    case inner
    case leftOuter
    case rightOuter
    case fullOuter
    case semi
    case anti
}

/// Sort key expression
public struct SortKeyExpr: Sendable, Equatable, Hashable {
    public let field: String
    public let ascending: Bool
    public let nullsFirst: Bool

    public init(field: String, ascending: Bool = true, nullsFirst: Bool = false) {
        self.field = field
        self.ascending = ascending
        self.nullsFirst = nullsFirst
    }
}

/// Aggregate expression
public struct AggregateExpr: Sendable, Equatable, Hashable {
    public let function: AggregateFunction
    public let field: String?
    public let alias: String

    public init(function: AggregateFunction, field: String? = nil, alias: String) {
        self.function = function
        self.field = field
        self.alias = alias
    }
}

/// Aggregate functions
public enum AggregateFunction: Sendable, Equatable, Hashable {
    case count
    case sum
    case avg
    case min
    case max
    case countDistinct
}

/// Predicate expression (simplified)
public indirect enum PredicateExpr: Sendable, Equatable, Hashable {
    case comparison(field: String, op: ComparisonOp, value: FieldValue)
    case and([PredicateExpr])
    case or([PredicateExpr])
    case not(PredicateExpr)
    case isNull(field: String)
    case isNotNull(field: String)
    case `true`
    case `false`
}

/// Comparison operators
public enum ComparisonOp: Sendable, Equatable, Hashable {
    case eq, ne, lt, le, gt, ge
    case like, ilike
    case `in`
}

/// Index bounds expression
public struct IndexBoundsExpr: Sendable, Equatable, Hashable {
    public let lowerBound: [FieldValue]?
    public let lowerInclusive: Bool
    public let upperBound: [FieldValue]?
    public let upperInclusive: Bool

    public init(
        lowerBound: [FieldValue]? = nil,
        lowerInclusive: Bool = true,
        upperBound: [FieldValue]? = nil,
        upperInclusive: Bool = true
    ) {
        self.lowerBound = lowerBound
        self.lowerInclusive = lowerInclusive
        self.upperBound = upperBound
        self.upperInclusive = upperInclusive
    }
}

// MARK: - Physical Operators

/// Physical operators represent specific algorithms for query execution
///
/// Each physical operator corresponds to a concrete implementation
/// strategy with specific performance characteristics.
public enum PhysicalOperator: Sendable, Equatable {
    /// Sequential table scan
    case seqScan(typeName: String, filter: PredicateExpr?)

    /// Index scan using a specific index
    case indexScan(typeName: String, indexName: String, bounds: IndexBoundsExpr?, filter: PredicateExpr?)

    /// Index-only scan (no table fetch needed)
    case indexOnlyScan(typeName: String, indexName: String, bounds: IndexBoundsExpr?, fields: [String])

    /// Bitmap index scan
    case bitmapScan(typeName: String, indexName: String, values: [FieldValue])

    /// Nested loop join
    case nestedLoopJoin(outer: GroupID, inner: GroupID, condition: PredicateExpr?, type: JoinType)

    /// Hash join
    case hashJoin(build: GroupID, probe: GroupID, buildKeys: [String], probeKeys: [String], type: JoinType)

    /// Merge join (requires sorted inputs)
    case mergeJoin(left: GroupID, right: GroupID, leftKeys: [String], rightKeys: [String], type: JoinType)

    /// Sort using in-memory or external sort
    case sort(input: GroupID, keys: [SortKeyExpr], limit: Int?)

    /// Top-K using heap
    case topK(input: GroupID, keys: [SortKeyExpr], k: Int)

    /// Hash aggregate
    case hashAggregate(input: GroupID, groupBy: [String], aggregates: [AggregateExpr])

    /// Sort aggregate (requires sorted input)
    case sortAggregate(input: GroupID, groupBy: [String], aggregates: [AggregateExpr])

    /// Union all (simple concatenation)
    case unionAll(inputs: [GroupID])

    /// Hash union (with deduplication)
    case hashUnion(inputs: [GroupID])

    /// Hash intersection
    case hashIntersection(inputs: [GroupID])

    /// Bitmap intersection (using bitmap indexes)
    case bitmapIntersection(inputs: [GroupID])

    /// Filter operator
    case filter(input: GroupID, predicate: PredicateExpr)

    /// Project operator
    case project(input: GroupID, fields: [String])

    /// Limit operator
    case limit(input: GroupID, count: Int, offset: Int?)

    /// Fetch records by IDs (after index scan)
    case fetch(input: GroupID, typeName: String)
}

// MARK: - Memo Expression

/// An expression in the Memo structure
///
/// Expressions can be either logical (implementation-independent) or
/// physical (specific algorithm). Each expression has children that
/// are references to groups, enabling sharing of subexpressions.
public struct MemoExpression: Sendable, Equatable {
    /// Expression identifier
    public let id: ExpressionID

    /// The operator (logical or physical)
    public let op: MemoOperator

    /// Whether this expression has been explored
    public var explored: Bool

    /// Estimated cost (for physical expressions)
    public var cost: Double?

    /// Statistics (cardinality estimate)
    public var cardinality: Double?

    public init(id: ExpressionID, op: MemoOperator) {
        self.id = id
        self.op = op
        self.explored = false
        self.cost = nil
        self.cardinality = nil
    }
}

/// Unified operator type for Memo
public enum MemoOperator: Sendable, Equatable {
    case logical(LogicalOperator)
    case physical(PhysicalOperator)

    /// Get child group IDs
    public var childGroups: [GroupID] {
        switch self {
        case .logical(let op):
            return op.childGroups
        case .physical(let op):
            return op.childGroups
        }
    }
}

// MARK: - Child Group Extraction

extension LogicalOperator {
    /// Get child group IDs
    var childGroups: [GroupID] {
        switch self {
        case .scan, .indexScan:
            return []
        case .filter(let input, _):
            return [input]
        case .project(let input, _):
            return [input]
        case .join(let left, let right, _, _):
            return [left, right]
        case .union(let inputs, _):
            return inputs
        case .intersection(let inputs):
            return inputs
        case .sort(let input, _):
            return [input]
        case .limit(let input, _, _):
            return [input]
        case .aggregate(let input, _, _):
            return [input]
        }
    }
}

extension PhysicalOperator {
    /// Get child group IDs
    var childGroups: [GroupID] {
        switch self {
        case .seqScan, .indexScan, .indexOnlyScan, .bitmapScan:
            return []
        case .nestedLoopJoin(let outer, let inner, _, _):
            return [outer, inner]
        case .hashJoin(let build, let probe, _, _, _):
            return [build, probe]
        case .mergeJoin(let left, let right, _, _, _):
            return [left, right]
        case .sort(let input, _, _):
            return [input]
        case .topK(let input, _, _):
            return [input]
        case .hashAggregate(let input, _, _):
            return [input]
        case .sortAggregate(let input, _, _):
            return [input]
        case .unionAll(let inputs):
            return inputs
        case .hashUnion(let inputs):
            return inputs
        case .hashIntersection(let inputs):
            return inputs
        case .bitmapIntersection(let inputs):
            return inputs
        case .filter(let input, _):
            return [input]
        case .project(let input, _):
            return [input]
        case .limit(let input, _, _):
            return [input]
        case .fetch(let input, _):
            return [input]
        }
    }
}
