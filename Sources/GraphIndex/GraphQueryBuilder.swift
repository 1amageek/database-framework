// GraphQueryBuilder.swift
// GraphIndex - SPARQL-like query builder for graph indexes
//
// Provides pattern-based graph queries with automatic index selection.

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

/// Graph query builder with SPARQL-like pattern matching
///
/// Supports triple pattern queries using the graph index with automatic
/// selection of the optimal index ordering based on the query pattern.
///
/// **Usage**:
/// ```swift
/// let builder = GraphQueryBuilder<Statement>(
///     database: database,
///     subspace: indexSubspace,
///     strategy: .tripleStore
/// )
///
/// // Find all edges from "Alice"
/// let edges = try await builder.from("Alice").execute()
///
/// // Find "knows" relationships to "Bob"
/// let whoKnowsBob = try await builder.edge("knows").to("Bob").execute()
/// ```
///
/// **Query Pattern Optimization**:
/// The builder automatically selects the optimal index based on bound variables:
/// - `(from, edge, to)` → any index (point lookup)
/// - `(from, edge, ?)` → SPO index
/// - `(from, ?, to)` → SOP index (hexastore) or OSP (tripleStore)
/// - `(?, edge, to)` → POS index
/// - `(from, ?, ?)` → SPO index
/// - `(?, edge, ?)` → POS/PSO index
/// - `(?, ?, to)` → OSP index
public struct GraphQueryBuilder<Item: Persistable> {
    // MARK: - Types

    /// Query pattern for a single element
    public enum Pattern {
        /// Match any value (wildcard)
        case any
        /// Match exact value
        case exact(any TupleElement)
    }

    /// Query result containing matched edge components
    public struct GraphEdge: Sendable {
        public let from: String
        public let edge: String
        public let to: String

        public init(from: String, edge: String, to: String) {
            self.from = from
            self.edge = edge
            self.to = to
        }
    }

    // MARK: - Properties

    /// Database connection (internally thread-safe)
    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let subspace: Subspace
    private let strategy: GraphIndexStrategy

    private var fromPattern: Pattern = .any
    private var edgePattern: Pattern = .any
    private var toPattern: Pattern = .any
    private var limitCount: Int?

    // MARK: - Initialization

    /// Initialize query builder
    ///
    /// - Parameters:
    ///   - database: FDB database connection
    ///   - subspace: Index subspace
    ///   - strategy: Index strategy for optimal index selection
    public init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        strategy: GraphIndexStrategy
    ) {
        self.database = database
        self.subspace = subspace
        self.strategy = strategy
    }

    // MARK: - Pattern Setters

    /// Set from/subject pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New builder with pattern set
    public func from(_ value: any TupleElement) -> Self {
        var copy = self
        copy.fromPattern = .exact(value)
        return copy
    }

    /// Set edge/predicate pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New builder with pattern set
    public func edge(_ value: any TupleElement) -> Self {
        var copy = self
        copy.edgePattern = .exact(value)
        return copy
    }

    /// Set to/object pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New builder with pattern set
    public func to(_ value: any TupleElement) -> Self {
        var copy = self
        copy.toPattern = .exact(value)
        return copy
    }

    /// Set result limit
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: New builder with limit set
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    // MARK: - Execution

    /// Execute query and return matching edges
    ///
    /// Automatically selects the optimal index based on the query pattern
    /// and performs a range scan.
    ///
    /// - Returns: Array of matching graph edges
    public func execute() async throws -> [GraphEdge] {
        let ordering = selectOptimalOrdering()
        return try await scanIndex(ordering: ordering)
    }

    // MARK: - Index Selection

    /// Select optimal index ordering based on query pattern
    ///
    /// Uses the pattern of bound variables to select the index that requires
    /// the fewest range scans.
    private func selectOptimalOrdering() -> GraphIndexOrdering {
        let fromBound = isBound(fromPattern)
        let edgeBound = isBound(edgePattern)
        let toBound = isBound(toPattern)

        switch (fromBound, edgeBound, toBound) {
        // All bound - any index works, use SPO
        case (true, true, true):
            return strategy == .adjacency ? .out : .spo

        // Two bound
        case (true, true, false):
            return strategy == .adjacency ? .out : .spo  // SPO: [from][edge]/*
        case (true, false, true):
            // Need SOP for hexastore, OSP for tripleStore
            return strategy == .hexastore ? .sop : .osp
        case (false, true, true):
            return strategy == .adjacency ? .in : .pos   // POS: [edge][to]/*

        // One bound
        case (true, false, false):
            return strategy == .adjacency ? .out : .spo  // SPO: [from]/*/*
        case (false, true, false):
            // PSO for hexastore, POS for tripleStore
            return strategy == .hexastore ? .pso : .pos
        case (false, false, true):
            return strategy == .adjacency ? .in : .osp   // OSP: [to]/*/*

        // None bound - full scan
        case (false, false, false):
            return strategy == .adjacency ? .out : .spo
        }
    }

    /// Check if a pattern is bound (not wildcard)
    private func isBound(_ pattern: Pattern) -> Bool {
        switch pattern {
        case .any:
            return false
        case .exact:
            return true
        }
    }

    // MARK: - Scan Implementation

    /// Scan index and collect results
    private func scanIndex(ordering: GraphIndexOrdering) async throws -> [GraphEdge] {
        var results: [GraphEdge] = []
        let indexSubspace = Self.subspaceForOrdering(ordering, base: subspace)
        let (beginKey, endKey) = try Self.buildScanRange(
            ordering: ordering,
            subspace: indexSubspace,
            fromPattern: fromPattern,
            edgePattern: edgePattern,
            toPattern: toPattern
        )

        try await database.withTransaction { transaction in
            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let edge = try Self.parseKey(key, ordering: ordering, subspace: indexSubspace) {
                    if Self.matchesPatterns(edge, fromPattern: self.fromPattern, edgePattern: self.edgePattern, toPattern: self.toPattern) {
                        results.append(edge)
                        if let limit = self.limitCount, results.count >= limit {
                            break
                        }
                    }
                }
            }
            return ()
        }

        return results
    }

    /// Get subspace for index ordering
    private static func subspaceForOrdering(_ ordering: GraphIndexOrdering, base: Subspace) -> Subspace {
        let key: Int64
        switch ordering {
        case .out: key = 0
        case .in: key = 1
        case .spo: key = 2
        case .pos: key = 3
        case .osp: key = 4
        case .sop: key = 5
        case .pso: key = 6
        case .ops: key = 7
        }
        return base.subspace(key)
    }

    /// Build scan range based on pattern and ordering
    private static func buildScanRange(
        ordering: GraphIndexOrdering,
        subspace: Subspace,
        fromPattern: Pattern,
        edgePattern: Pattern,
        toPattern: Pattern
    ) throws -> (begin: FDB.Bytes, end: FDB.Bytes) {
        var prefixElements: [any TupleElement] = []

        // Build prefix based on ordering and bound patterns
        let elementOrder = ordering.elementOrder
        let patterns = [fromPattern, edgePattern, toPattern]

        for idx in elementOrder {
            let pattern = patterns[idx]
            switch pattern {
            case .exact(let value):
                prefixElements.append(value)
            case .any:
                // Stop building prefix at first unbound element
                break
            }
            // Check if we should continue (only if previous was exact)
            if case .exact = pattern {
                continue
            } else {
                break
            }
        }

        if prefixElements.isEmpty {
            // Full scan
            return subspace.range()
        } else {
            // Prefix scan
            let prefix = Subspace(prefix: subspace.prefix + Tuple(prefixElements).pack())
            return prefix.range()
        }
    }

    /// Parse key into GraphEdge based on ordering
    private static func parseKey(_ key: FDB.Bytes, ordering: GraphIndexOrdering, subspace: Subspace) throws -> GraphEdge? {
        let tuple = try subspace.unpack(key)

        guard tuple.count >= 3 else {
            return nil
        }

        // Extract elements based on ordering
        let elementOrder = ordering.elementOrder

        // Find indices in the tuple for each component
        var fromValue: String?
        var edgeValue: String?
        var toValue: String?

        for (tupleIdx, componentIdx) in elementOrder.enumerated() {
            guard tupleIdx < tuple.count, let element = tuple[tupleIdx] else {
                continue
            }

            let stringValue: String
            if let str = element as? String {
                stringValue = str
            } else {
                stringValue = String(describing: element)
            }

            switch componentIdx {
            case 0: fromValue = stringValue
            case 1: edgeValue = stringValue
            case 2: toValue = stringValue
            default: break
            }
        }

        guard let from = fromValue, let edge = edgeValue, let to = toValue else {
            return nil
        }

        return GraphEdge(from: from, edge: edge, to: to)
    }

    /// Check if edge matches all patterns (for post-filtering)
    private static func matchesPatterns(
        _ edge: GraphEdge,
        fromPattern: Pattern,
        edgePattern: Pattern,
        toPattern: Pattern
    ) -> Bool {
        return matchesPattern(edge.from, pattern: fromPattern) &&
               matchesPattern(edge.edge, pattern: edgePattern) &&
               matchesPattern(edge.to, pattern: toPattern)
    }

    /// Check if value matches pattern
    private static func matchesPattern(_ value: String, pattern: Pattern) -> Bool {
        switch pattern {
        case .any:
            return true
        case .exact(let expected):
            if let str = expected as? String {
                return value == str
            }
            return value == String(describing: expected)
        }
    }
}
