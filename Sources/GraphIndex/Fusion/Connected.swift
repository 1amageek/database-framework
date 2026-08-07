// Connected.swift
// GraphIndex - Graph connectivity query for Fusion
//
// This file is part of GraphIndex module, not DatabaseEngine.
// Provides graph-based filtering and scoring for Fusion queries.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import ScalarIndex
import StorageKit

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
///         .hops(2)
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
    private let field: FieldIdentity
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
    public init(_ field: Field<T, String>) {
        guard let context = FusionContext.current else {
            fatalError("Connected must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Connected query for an optional field
    public init(_ field: Field<T, String?>) {
        guard let context = FusionContext.current else {
            fatalError("Connected must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Connected query with explicit context
    public init(
        _ field: Field<T, String>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Connected query for an optional field with explicit context
    public init(
        _ field: Field<T, String?>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
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
    /// - Parameter count: Maximum hops (default: 1)
    /// - Returns: Updated query
    public func hops(_ count: Int) -> Self {
        var copy = self
        copy.maxHopCount = max(1, count)
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
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        queryContext.indexDescriptors(for: T.self).first { descriptor in
            guard descriptor.kindIdentifier == "graph" else {
                return false
            }
            // Match by source field
            return descriptor.fieldNames.contains(field.name)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard sourceValue != nil || targetValue != nil else {
            throw FusionQueryError.invalidConfiguration("Must specify from() or to() for Connected query")
        }

        // Find connected nodes via graph traversal
        let connectedNodes = try await findConnectedNodes()

        // If no candidates, fetch items by their graph node values
        let items: [T]
        if let candidateIDs = candidates {
            items = try await queryContext.fetchItems(
                identifiers: Array(candidateIDs),
                type: T.self
            )
        } else {
            // Fetch items that match connected node values
            items = try await fetchItemsByNodeValues(connectedNodes.map { $0.node })
        }

        // Score items based on graph connectivity
        var results: [ScoredResult<T>] = []
        for item in items {
            guard let nodeValue = try nodeValue(in: item) else {
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
        guard let descriptor = try findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: field.name,
                kind: "graph"
            )
        }

        let strategy = try PropertyGraphIndexMetadata(
            canonical: descriptor.kind
        ).strategy

        // BFS traversal
        var visited: Set<String> = []
        var results: [ConnectedNode] = []
        var frontier: [(node: String, hops: Int)] = []
        var frontierIndex = 0

        // Initialize frontier
        if let source = sourceValue {
            frontier.append((node: source, hops: 0))
            visited.insert(source)
        }
        if let target = targetValue {
            frontier.append((node: target, hops: 0))
            visited.insert(target)
        }

        // BFS traversal using index-based iteration to avoid O(n) removeFirst()
        while frontierIndex < frontier.count {
            let (currentNode, currentHops) = frontier[frontierIndex]
            frontierIndex += 1

            if currentHops > 0 {
                results.append(ConnectedNode(node: currentNode, hops: currentHops))
            }

            if currentHops >= maxHopCount {
                continue
            }

            // Find neighbors within transaction
            let neighbors = try await queryContext.withReadableIndex(
                named: descriptor.name,
                kindIdentifier: descriptor.kindIdentifier,
                for: T.self
            ) { readableIndex, transaction -> [String] in
                guard let readableIndex else {
                    return []
                }
                return try await self.findNeighbors(
                    node: currentNode,
                    indexSubspace: readableIndex.subspace,
                    strategy: strategy,
                    transaction: transaction
                )
            }

            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        return results
    }

    // MARK: - Graph Index Reading

    /// Find neighbors of a node via graph index
    private func findNeighbors(
        node: String,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionAccess
    ) async throws -> [String] {
        guard strategy != .quadStore else {
            throw GraphIndexError.unsupportedQueryPattern(
                pattern: "String-based connectivity",
                strategy: strategy
            )
        }

        let scanner = GraphEdgeScanner(
            indexSubspace: indexSubspace,
            strategy: strategy,
            graphTarget: .all
        )
        let nodeIdentity = GraphIdentity.identifier(node)
        let edgeIdentity = edgeType.map(GraphIdentity.identifier)
        var results: Set<String> = []

        switch direction {
        case .outgoing:
            let edgeSequence = scanner.scanOutgoing(
                from: nodeIdentity,
                edgeLabel: edgeIdentity,
                transaction: transaction
            )
            var edgeCursor = edgeSequence.makeCursor()
            while let edge = try await edgeCursor.next() {
                results.insert(
                    try edge.target.requirePropertyGraphIdentifier()
                )
            }

        case .incoming:
            let edgeSequence = scanner.scanIncoming(
                to: nodeIdentity,
                edgeLabel: edgeIdentity,
                transaction: transaction
            )
            var edgeCursor = edgeSequence.makeCursor()
            while let edge = try await edgeCursor.next() {
                results.insert(
                    try edge.source.requirePropertyGraphIdentifier()
                )
            }

        case .both:
            let outgoingSequence = scanner.scanOutgoing(
                from: nodeIdentity,
                edgeLabel: edgeIdentity,
                transaction: transaction
            )
            var outgoingCursor = outgoingSequence.makeCursor()
            while let edge = try await outgoingCursor.next() {
                results.insert(
                    try edge.target.requirePropertyGraphIdentifier()
                )
            }
            let incomingSequence = scanner.scanIncoming(
                to: nodeIdentity,
                edgeLabel: edgeIdentity,
                transaction: transaction
            )
            var incomingCursor = incomingSequence.makeCursor()
            while let edge = try await incomingCursor.next() {
                results.insert(
                    try edge.source.requirePropertyGraphIdentifier()
                )
            }
        }

        return Array(results)
    }

    /// Fetch items by their node field values
    ///
    /// Uses optimized lookup strategy:
    /// 1. If field is "id", use direct ID lookup (O(k) where k = nodeValues.count)
    /// 2. If ScalarIndex exists on field, use index query (O(k) lookups)
    /// 3. Otherwise, fallback to full scan (O(n) - expensive, logs warning)
    private func fetchItemsByNodeValues(_ nodeValues: [String]) async throws -> [T] {
        guard !nodeValues.isEmpty else { return [] }

        // Strategy 1: If the field is the ID field, use direct ID lookup
        if field.name == "id" {
            guard T.persistableIdentifierType == .string else {
                throw FusionQueryError.invalidConfiguration(
                    "Graph node identifiers require a String entity identifier when the graph field is 'id'"
                )
            }
            let identifierTuples = try nodeValues.map { nodeValue in
                try PersistableIdentifierKeyCodec.tuple(
                    for: .string(nodeValue),
                    expectedType: T.persistableIdentifierType
                )
            }
            return try await queryContext.fetchItems(
                ids: identifierTuples,
                type: T.self
            )
        }

        // Strategy 2: If there's a ScalarIndex on this field, use it
        if let indexDescriptor = try findScalarIndexForField() {
            return try await fetchUsingScalarIndex(
                nodeValues: nodeValues,
                indexDescriptor: indexDescriptor
            )
        }

        // Strategy 3: Fallback to full scan (expensive)
        // Log warning for visibility
        #if DEBUG
        print("[Connected] Warning: No index found for field '\(field.name)' on type '\(T.persistableType)'. Performing full table scan.")
        #endif

        let allItems = try await queryContext.fetchAllItems(type: T.self)
        let nodeValueSet = Set(nodeValues)
        return try allItems.filter { item in
            guard let value = try nodeValue(in: item) else {
                return false
            }
            return nodeValueSet.contains(value)
        }
    }

    /// Find a ScalarIndex that covers the field
    private func findScalarIndexForField() throws -> IndexDescriptor? {
        queryContext.indexDescriptors(for: T.self).first { descriptor in
            // Check if it's a ScalarIndex
            guard descriptor.kindIdentifier == "scalar" else { return false }

            // The first indexed field defines the scalar lookup prefix. Read the
            // canonical descriptor metadata instead of parsing its display name.
            return descriptor.fieldNames.first == field.name
        }
    }

    private func nodeValue(in item: T) throws -> String? {
        guard let value = try item.persistedFieldValue(for: field) else {
            throw DataAccessError.fieldNotFound(
                itemType: T.persistableType,
                keyPath: field.name
            )
        }
        switch value {
        case .null:
            return nil
        case .string(let string):
            return string
        default:
            throw GraphIndexError.invalidFieldType(
                fieldName: field.name,
                expectedType: "String",
                actualType: GraphValueSemanticName.field(value)
            )
        }
    }

    /// Fetch items using ScalarIndex lookup
    private func fetchUsingScalarIndex(
        nodeValues: [String],
        indexDescriptor: IndexDescriptor
    ) async throws -> [T] {
        let searcher = ScalarIndexSearcher(keyFieldCount: 1)
        let allIds: [Tuple] = try await queryContext.withReadableIndex(
            named: indexDescriptor.name,
            kindIdentifier: indexDescriptor.kindIdentifier,
            for: T.self
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            let reader = queryContext.storageReader(
                transaction: transaction
            )
            var identifiers: [Tuple] = []
            for nodeValue in nodeValues {
                let query = ScalarIndexQuery.equals([nodeValue])
                let entries = try await searcher.search(
                    query: query,
                    in: readableIndex.subspace,
                    using: reader
                )
                for entry in entries {
                    identifiers.append(entry.itemID)
                }
            }
            return identifiers
        }

        // Deduplicate IDs by packed representation
        var seenPacked: Set<ByteString> = []
        var uniqueIds: [Tuple] = []
        for id in allIds {
            let packed = id.pack()
            if !seenPacked.contains(packed) {
                seenPacked.insert(packed)
                uniqueIds.append(id)
            }
        }

        // Fetch items by IDs
        return try await queryContext.fetchItems(ids: uniqueIds, type: T.self)
    }
}
