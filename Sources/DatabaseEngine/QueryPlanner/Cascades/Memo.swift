// Memo.swift
// Cascades Optimizer - Memoization structure for query optimization
//
// The Memo is a hash-consed DAG (Directed Acyclic Graph) that stores
// equivalence classes of logical expressions and their physical implementations.
//
// Reference: Graefe, G. "The Cascades Framework for Query Optimization", 1995

import Foundation
import Synchronization

// MARK: - Group

/// A group represents an equivalence class of expressions
///
/// All expressions in a group produce the same result (same rows, same columns).
/// The optimizer explores different physical implementations within each group
/// to find the lowest-cost plan.
public struct Group: Sendable {
    /// Group identifier
    public let id: GroupID

    /// Logical expressions in this group
    public var logicalExpressions: [MemoExpression]

    /// Physical expressions in this group
    public var physicalExpressions: [MemoExpression]

    /// Best physical expression for each required property set
    public var bestExpressions: [PropertySet: ExpressionID]

    /// Lower bound on cost (for pruning)
    public var lowerBoundCost: Double?

    /// Estimated cardinality
    public var cardinality: Double?

    /// Logical properties (columns, unique keys, etc.)
    public var logicalProperties: LogicalProperties?

    /// Whether this group has been fully explored
    public var explored: Bool

    public init(id: GroupID) {
        self.id = id
        self.logicalExpressions = []
        self.physicalExpressions = []
        self.bestExpressions = [:]
        self.lowerBoundCost = nil
        self.cardinality = nil
        self.logicalProperties = nil
        self.explored = false
    }
}

// MARK: - Properties

/// Physical properties required by a parent operator
///
/// Properties represent requirements that a child must satisfy,
/// such as sort order or distribution (in distributed systems).
public struct PropertySet: Hashable, Sendable {
    /// Required sort order
    public let sortOrder: [SortKeyExpr]?

    /// Required distribution (for distributed queries)
    public let distribution: Distribution?

    public init(sortOrder: [SortKeyExpr]? = nil, distribution: Distribution? = nil) {
        self.sortOrder = sortOrder
        self.distribution = distribution
    }

    public static let none = PropertySet()
}

/// Distribution requirements (for distributed queries)
public enum Distribution: Hashable, Sendable {
    case any
    case singleton
    case hashed(keys: [String])
    case broadcast
}

/// Functional dependency representation
public struct FunctionalDependency: Sendable, Equatable, Hashable {
    /// Determinant columns
    public let determinant: [String]

    /// Dependent columns
    public let dependent: [String]

    public init(determinant: [String], dependent: [String]) {
        self.determinant = determinant
        self.dependent = dependent
    }
}

/// Logical properties derived from expressions
public struct LogicalProperties: Sendable, Equatable {
    /// Output columns
    public let columns: [String]

    /// Unique keys (sets of columns that uniquely identify rows)
    public let uniqueKeys: [[String]]

    /// Not-null columns
    public let notNullColumns: [String]

    /// Functional dependencies
    public let functionalDependencies: [FunctionalDependency]

    public init(
        columns: [String] = [],
        uniqueKeys: [[String]] = [],
        notNullColumns: [String] = [],
        functionalDependencies: [FunctionalDependency] = []
    ) {
        self.columns = columns
        self.uniqueKeys = uniqueKeys
        self.notNullColumns = notNullColumns
        self.functionalDependencies = functionalDependencies
    }
}

// MARK: - Memo

/// The Memo structure for query optimization
///
/// The Memo is a compact representation of the search space, storing:
/// - Groups: equivalence classes of expressions
/// - Expressions: logical and physical operators with children
/// - Winners: best physical expressions for each group/property combination
///
/// **Key Features**:
/// - Hash-consing: duplicate expressions are detected and shared
/// - Memoization: optimization results are cached
/// - Top-down search: enables branch-and-bound pruning
///
/// **Thread Safety**:
/// Uses Mutex for thread-safe access in concurrent scenarios.
public final class Memo: @unchecked Sendable {
    /// All groups in the memo
    private var groups: [GroupID: Group]

    /// Expression hash index for duplicate detection
    private var expressionIndex: [Int: GroupID]

    /// Next group ID
    private var nextGroupId: Int

    /// Lock for thread safety
    private let lock: Mutex<Void>

    /// Root group (the final result)
    public private(set) var rootGroupId: GroupID?

    // MARK: - Initialization

    public init() {
        self.groups = [:]
        self.expressionIndex = [:]
        self.nextGroupId = 0
        self.lock = Mutex(())
        self.rootGroupId = nil
    }

    // MARK: - Group Management

    /// Create a new group
    public func createGroup() -> GroupID {
        lock.withLock { _ in
            let id = GroupID(nextGroupId)
            nextGroupId += 1
            groups[id] = Group(id: id)
            return id
        }
    }

    /// Get a group by ID
    public func getGroup(_ id: GroupID) -> Group? {
        lock.withLock { _ in
            groups[id]
        }
    }

    /// Update a group
    public func updateGroup(_ id: GroupID, _ update: (inout Group) -> Void) {
        lock.withLock { _ in
            if var group = groups[id] {
                update(&group)
                groups[id] = group
            }
        }
    }

    /// Set the root group
    public func setRootGroup(_ id: GroupID) {
        lock.withLock { _ in
            rootGroupId = id
        }
    }

    // MARK: - Expression Management

    /// Add a logical expression to the memo
    ///
    /// Returns the group ID where the expression was added.
    /// If an equivalent expression already exists, returns its group.
    ///
    /// - Parameter op: The logical operator
    /// - Returns: The group ID containing the expression
    public func addLogicalExpression(_ op: LogicalOperator) -> GroupID {
        lock.withLock { _ in
            // Check for duplicate
            let hash = hashOperator(.logical(op))
            if let existingGroup = expressionIndex[hash] {
                return existingGroup
            }

            // Create new group
            let groupId = GroupID(nextGroupId)
            nextGroupId += 1

            var group = Group(id: groupId)
            let exprId = ExpressionID(groupID: groupId, index: 0)
            let expr = MemoExpression(id: exprId, op: .logical(op))
            group.logicalExpressions.append(expr)

            groups[groupId] = group
            expressionIndex[hash] = groupId

            return groupId
        }
    }

    /// Add a logical expression to an existing group
    ///
    /// Used when a transformation rule derives a new equivalent expression.
    ///
    /// - Parameters:
    ///   - op: The logical operator
    ///   - groupId: The target group
    /// - Returns: The expression ID
    @discardableResult
    public func addLogicalExpressionToGroup(_ op: LogicalOperator, groupId: GroupID) -> ExpressionID? {
        lock.withLock { _ in
            guard var group = groups[groupId] else { return nil }

            // Check if expression already exists in group
            let newOp = MemoOperator.logical(op)
            for expr in group.logicalExpressions {
                if expr.op == newOp {
                    return expr.id
                }
            }

            let exprId = ExpressionID(groupID: groupId, index: group.logicalExpressions.count)
            let expr = MemoExpression(id: exprId, op: newOp)
            group.logicalExpressions.append(expr)
            groups[groupId] = group

            // Update hash index
            let hash = hashOperator(newOp)
            expressionIndex[hash] = groupId

            return exprId
        }
    }

    /// Add a physical expression to a group
    ///
    /// - Parameters:
    ///   - op: The physical operator
    ///   - groupId: The target group
    ///   - cost: Estimated cost
    /// - Returns: The expression ID
    @discardableResult
    public func addPhysicalExpression(
        _ op: PhysicalOperator,
        groupId: GroupID,
        cost: Double
    ) -> ExpressionID? {
        lock.withLock { _ in
            guard var group = groups[groupId] else { return nil }

            let exprId = ExpressionID(groupID: groupId, index: group.physicalExpressions.count)
            var expr = MemoExpression(id: exprId, op: .physical(op))
            expr.cost = cost

            group.physicalExpressions.append(expr)
            groups[groupId] = group

            return exprId
        }
    }

    /// Record the best expression for a group/property combination
    public func recordWinner(groupId: GroupID, properties: PropertySet, expressionId: ExpressionID) {
        lock.withLock { _ in
            guard var group = groups[groupId] else { return }
            group.bestExpressions[properties] = expressionId
            groups[groupId] = group
        }
    }

    /// Get the best expression for a group/property combination
    public func getWinner(groupId: GroupID, properties: PropertySet) -> ExpressionID? {
        lock.withLock { _ in
            groups[groupId]?.bestExpressions[properties]
        }
    }

    /// Get all logical expressions in a group
    public func getLogicalExpressions(_ groupId: GroupID) -> [MemoExpression] {
        lock.withLock { _ in
            groups[groupId]?.logicalExpressions ?? []
        }
    }

    /// Get all physical expressions in a group
    public func getPhysicalExpressions(_ groupId: GroupID) -> [MemoExpression] {
        lock.withLock { _ in
            groups[groupId]?.physicalExpressions ?? []
        }
    }

    /// Mark a group as explored
    public func markExplored(_ groupId: GroupID) {
        lock.withLock { _ in
            if var group = groups[groupId] {
                group.explored = true
                groups[groupId] = group
            }
        }
    }

    /// Check if a group has been explored
    public func isExplored(_ groupId: GroupID) -> Bool {
        lock.withLock { _ in
            groups[groupId]?.explored ?? false
        }
    }

    // MARK: - Statistics

    /// Get total number of groups
    public var groupCount: Int {
        lock.withLock { _ in
            groups.count
        }
    }

    /// Get total number of expressions
    public var expressionCount: Int {
        lock.withLock { _ in
            groups.values.reduce(0) { sum, group in
                sum + group.logicalExpressions.count + group.physicalExpressions.count
            }
        }
    }

    // MARK: - Private Helpers

    /// Hash an operator for duplicate detection
    private func hashOperator(_ op: MemoOperator) -> Int {
        var hasher = Hasher()

        switch op {
        case .logical(let logicalOp):
            hasher.combine("logical")
            hasher.combine(String(describing: logicalOp))
        case .physical(let physicalOp):
            hasher.combine("physical")
            hasher.combine(String(describing: physicalOp))
        }

        return hasher.finalize()
    }
}

// MARK: - Debug Support

extension Memo: CustomStringConvertible {
    public var description: String {
        var result = "Memo (\(groupCount) groups, \(expressionCount) expressions)\n"

        for groupId in groups.keys.sorted(by: { $0.id < $1.id }) {
            guard let group = groups[groupId] else { continue }
            result += "  \(groupId):\n"
            for expr in group.logicalExpressions {
                result += "    L: \(expr.op)\n"
            }
            for expr in group.physicalExpressions {
                result += "    P: \(expr.op) [cost=\(expr.cost ?? -1)]\n"
            }
        }

        return result
    }
}
