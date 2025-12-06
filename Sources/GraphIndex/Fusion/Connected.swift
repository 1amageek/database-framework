// Connected.swift
// GraphIndex - Graph connectivity query for Fusion
//
// This file is part of GraphIndex module, not DatabaseEngine.
// Provides graph-based filtering and scoring for Fusion queries.

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

/// Graph connectivity query for Fusion
///
/// Filters and scores items based on graph relationships.
/// Items connected to the specified source get higher scores based on path length.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Person.self) {
///     // Find people connected to "Alice" via "knows"
///     Connected(\.name)
///         .from("Alice")
///         .via("knows")
///         .maxHops(2)
///
///     // Combined with text search
///     Search(\.bio).terms(["engineer"])
/// }
/// .algorithm(.rrf())
/// .execute()
/// ```
///
/// **Scoring**:
/// - Direct connection (1 hop): score = 1.0
/// - 2 hops: score = 0.5
/// - 3 hops: score = 0.33
/// - General: score = 1.0 / hops
public struct Connected<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var sourceValue: String?
    private var edgeType: String?
    private var targetValue: String?
    private var maxHopCount: Int = 1
    private var direction: Direction = .outgoing

    /// Direction of graph traversal
    public enum Direction: Sendable {
        /// Follow outgoing edges (from → to)
        case outgoing
        /// Follow incoming edges (to ← from)
        case incoming
        /// Follow edges in both directions
        case both
    }

    // MARK: - Initialization (FusionContext)

    /// Create a Connected query for a field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the field used as graph node identifier
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Person.self) {
    ///     Connected(\.userId).from("user123").via("follows")
    /// }
    /// ```
    public init(_ keyPath: KeyPath<T, String>) {
        guard let context = FusionContext.current else {
            fatalError("Connected must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Connected query for an optional field
    public init(_ keyPath: KeyPath<T, String?>) {
        guard let context = FusionContext.current else {
            fatalError("Connected must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Connected query with explicit context
    public init(_ keyPath: KeyPath<T, String>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Connected query for an optional field with explicit context
    public init(_ keyPath: KeyPath<T, String?>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Set the source node to start traversal from
    ///
    /// - Parameter value: Source node identifier
    /// - Returns: Updated query
    public func from(_ value: String) -> Self {
        var copy = self
        copy.sourceValue = value
        copy.direction = .outgoing
        return copy
    }

    /// Set the target node to find paths to
    ///
    /// - Parameter value: Target node identifier
    /// - Returns: Updated query
    public func to(_ value: String) -> Self {
        var copy = self
        copy.targetValue = value
        copy.direction = .incoming
        return copy
    }

    /// Set the edge type to traverse
    ///
    /// - Parameter edgeType: Edge type (e.g., "follows", "knows", "likes")
    /// - Returns: Updated query
    public func via(_ edgeType: String) -> Self {
        var copy = self
        copy.edgeType = edgeType
        return copy
    }

    /// Set maximum number of hops for traversal
    ///
    /// - Parameter hops: Maximum hops (default: 1)
    /// - Returns: Updated query
    public func maxHops(_ hops: Int) -> Self {
        var copy = self
        copy.maxHopCount = max(1, hops)
        return copy
    }

    /// Set traversal direction
    ///
    /// - Parameter direction: Direction to traverse (.outgoing, .incoming, .both)
    /// - Returns: Updated query
    public func direction(_ direction: Direction) -> Self {
        var copy = self
        copy.direction = direction
        return copy
    }

    // MARK: - Index Discovery

    /// Find the graph index descriptor
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == GraphIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? GraphIndexKind<T> else {
                return false
            }
            // Match by source field or any graph index
            return kind.fieldNames.contains(fieldName) || true
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard sourceValue != nil || targetValue != nil else {
            throw FusionQueryError.invalidConfiguration("Must specify from() or to() for Connected query")
        }

        // Find connected nodes via graph traversal
        let connectedNodes = try await findConnectedNodes()

        // If no candidates, fetch items by their graph node values
        let items: [T]
        if let candidateIds = candidates {
            items = try await queryContext.fetchItemsByStringIds(type: T.self, ids: Array(candidateIds))
        } else {
            // Fetch items that match connected node values
            items = try await fetchItemsByNodeValues(connectedNodes.map(\.node))
        }

        // Score items based on graph connectivity
        var results: [ScoredResult<T>] = []
        for item in items {
            guard let nodeValue = item[dynamicMember: fieldName] as? String else {
                continue
            }

            // Find if this item's node value is in connected nodes
            if let connection = connectedNodes.first(where: { $0.node == nodeValue }) {
                // Score based on hop distance (closer = higher score)
                let score = 1.0 / Double(connection.hops)
                results.append(ScoredResult(item: item, score: score))
            }
        }

        // Sort by score descending
        return results.sorted { $0.score > $1.score }
    }

    // MARK: - Graph Traversal

    private struct ConnectedNode: Sendable {
        let node: String
        let hops: Int
    }

    /// Find nodes connected within maxHops
    private func findConnectedNodes() async throws -> [ConnectedNode] {
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "graph"
            )
        }

        // Use GraphTraverser or direct index scan
        // For now, implement simple BFS-style traversal
        var visited: Set<String> = []
        var results: [ConnectedNode] = []
        var frontier: [(node: String, hops: Int)] = []

        // Initialize frontier
        if let source = sourceValue {
            frontier.append((node: source, hops: 0))
            visited.insert(source)
        }
        if let target = targetValue {
            frontier.append((node: target, hops: 0))
            visited.insert(target)
        }

        // BFS traversal
        while !frontier.isEmpty {
            let (currentNode, currentHops) = frontier.removeFirst()

            if currentHops > 0 {
                results.append(ConnectedNode(node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHopCount {
                continue
            }

            // Find neighbors
            let neighbors = try await findNeighbors(
                node: currentNode,
                indexName: descriptor.name,
                direction: direction
            )

            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        return results
    }

    /// Find neighbors of a node via graph index
    private func findNeighbors(
        node: String,
        indexName: String,
        direction: Direction
    ) async throws -> [String] {
        try await queryContext.executeGraphNeighbors(
            type: T.self,
            indexName: indexName,
            node: node,
            edgeType: edgeType,
            direction: direction == .outgoing ? .outgoing :
                       direction == .incoming ? .incoming : .both
        )
    }

    /// Fetch items by their node field values
    private func fetchItemsByNodeValues(_ nodeValues: [String]) async throws -> [T] {
        // This would ideally use a scalar index on the node field
        // For now, fetch all and filter
        let allItems = try await queryContext.fetchAllItems(type: T.self)
        return allItems.filter { item in
            guard let value = item[dynamicMember: fieldName] as? String else {
                return false
            }
            return nodeValues.contains(value)
        }
    }
}
