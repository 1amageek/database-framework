// Connected.swift
// GraphIndex - Graph connectivity query for Fusion
//
// This file is part of GraphIndex module, not DatabaseEngine.
// Provides graph-based filtering and scoring for Fusion queries.

@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
/// .strategy(.reciprocalRank())
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

    private let queryContext: IndexQueryContext!
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
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Connected query for an optional field
    public init(_ field: Field<T, String?>) {
        let context = FusionContext.current
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
            guard descriptor.type == .graph(.property) else {
                return false
            }
            // Match by source field
            return descriptor.fieldNames.contains(field.name)
        }
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Connected requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["depth"]
            ),
            indexDescriptor: {
                guard let descriptor = try self.findIndexDescriptor() else {
                    throw FusionQueryError.indexNotFound(
                        entity: T.persistableType,
                        field: self.field.name,
                        indexType: .graph(.property)
                    )
                }
                return descriptor
            },
            operation: { [self] candidates, execution in
                try await executeBound(
                    candidates: candidates,
                    execution: execution
                )
            }
        )
    }

    private func executeBound(
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        guard sourceValue != nil || targetValue != nil else {
            throw FusionQueryError.invalidConfiguration("Must specify from() or to() for Connected query")
        }

        guard let descriptor = try findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: field.name,
                indexType: .graph(.property)
            )
        }
        guard let configuration = PropertyGraphIndexConfiguration(
            descriptor: descriptor
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Index '\(descriptor.name)' is not a property-graph index"
            )
        }
        guard let entity = queryContext.schema.entity(
            named: T.persistableType
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Entity '\(T.persistableType)' is not present in the active schema"
            )
        }

        return try await queryContext.withReadableIndex(
            named: descriptor.name,
            indexType: descriptor.type,
            for: T.self,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["depth"]
            )
        ) { readableIndex, transaction -> FusionQueryResult<T> in
            guard let readableIndex else {
                return try FusionQueryResultBuilder<T>(
                    execution: execution
                ).finish()
            }
            let connectedNodes = try await self.findConnectedNodes(
                indexSubspace: readableIndex.subspace,
                strategy: configuration.strategy,
                transaction: transaction,
                workMeter: execution.workMeter
            )
            let models: DatabaseSharedRetainedArray<PersistedModel?>
            if let candidateIDs = candidates {
                models = try await self.fetchCandidateModels(
                    candidateIDs,
                    entity: entity,
                    sourceIndexName: descriptor.name,
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
            } else {
                models = try await self.fetchItemsByNodeValues(
                    connectedNodes,
                    entity: entity,
                    sourceIndexName: descriptor.name,
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
            }

            let hopReservation = try execution.workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<[String: Int]>.stride),
                at: .indexScan
            )
            defer { hopReservation.release() }
            var hopsByNode: [String: Int] = [:]
            hopsByNode.reserveCapacity(connectedNodes.count)
            for connection in connectedNodes {
                try hopReservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(connection.node.utf8.count) + 64,
                    at: .indexScan
                )
                hopsByNode[connection.node] = connection.hops
            }

            var output = try FusionQueryResultBuilder<T>(
                execution: execution,
                expectedCount: models.count
            )
            for model in models {
                guard let model else {
                    throw GraphIndexError.invalidScanState
                }
                try output.appendDecodedModel(model) { item in
                    guard let node = try self.nodeValue(in: item),
                          let hops = hopsByNode[node] else {
                        return nil
                    }
                    return 1.0 / Double(hops)
                }
            }
            return try output.finish()
        }
    }

    // MARK: - Graph Traversal

    private struct ConnectedNode: Sendable {
        let node: String
        let hops: Int
    }

    /// Find nodes connected within maxHops
    private func findConnectedNodes(
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<ConnectedNode> {
        // BFS traversal
        let traversalReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<Set<String>>.stride)
                + UInt64(MemoryLayout<[(node: String, hops: Int)]>.stride),
            at: .indexScan
        )
        defer { traversalReservation.release() }
        var visited: Set<String> = []
        var results = try DatabaseRetainedArrayBuilder<ConnectedNode>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: ConnectedNode.self)
        )
        var frontier: [(node: String, hops: Int)] = []
        var frontierIndex = 0

        // Initialize frontier
        if let source = sourceValue {
            try traversalReservation.reserveAdditional(
                rows: 1,
                bytes: UInt64(source.utf8.count) + 128,
                at: .indexScan
            )
            frontier.append((node: source, hops: 0))
            visited.insert(source)
        }
        if let target = targetValue {
            try traversalReservation.reserveAdditional(
                rows: 1,
                bytes: UInt64(target.utf8.count) + 128,
                at: .indexScan
            )
            frontier.append((node: target, hops: 0))
            visited.insert(target)
        }

        // BFS traversal using index-based iteration to avoid O(n) removeFirst()
        while frontierIndex < frontier.count {
            let (currentNode, currentHops) = frontier[frontierIndex]
            frontierIndex += 1

            if currentHops > 0 {
                try results.append(
                    footprint: DatabaseIntermediateFootprint(
                        rows: 1,
                        bytes: UInt64(currentNode.utf8.count) + 64
                    ),
                    at: .indexScan
                ) {
                    ConnectedNode(node: currentNode, hops: currentHops)
                }
            }

            if currentHops >= maxHopCount {
                continue
            }

            let neighbors = try await findNeighbors(
                node: currentNode,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                workMeter: workMeter
            )

            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    try traversalReservation.reserveAdditional(
                        rows: 1,
                        bytes: UInt64(neighbor.utf8.count) + 128,
                        at: .indexScan
                    )
                    visited.insert(neighbor)
                    frontier.append((node: neighbor, hops: currentHops + 1))
                }
            }
        }

        return try results.finish().moveToSharedOwnership(at: .indexScan)
    }

    // MARK: - Graph Index Reading

    /// Find neighbors of a node via graph index
    private func findNeighbors(
        node: String,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<String> {
        guard strategy != .quadStore else {
            throw GraphIndexError.unsupportedQueryPattern(
                pattern: "String-based connectivity",
                strategy: strategy
            )
        }

        let scanner = GraphEdgeScanner(
            indexSubspace: indexSubspace,
            strategy: strategy,
            graphTarget: .all,
            workMeter: workMeter
        )
        let nodeIdentity = GraphIdentity.identifier(node)
        let edgeIdentity = edgeType.map(GraphIdentity.identifier)
        let resultReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<Set<String>>.stride),
            at: .indexScan
        )
        var transferredReservation = false
        defer {
            if !transferredReservation { resultReservation.release() }
        }
        var results: Set<String> = []

        func insert(_ value: String) throws {
            guard !results.contains(value) else { return }
            try resultReservation.reserveAdditional(
                rows: 1,
                bytes: UInt64(value.utf8.count) + 64,
                at: .indexScan
            )
            results.insert(value)
        }

        switch direction {
        case .outgoing:
            let edgeSequence = scanner.scanOutgoing(
                from: nodeIdentity,
                edgeLabel: edgeIdentity,
                transaction: transaction
            )
            var edgeCursor = edgeSequence.makeCursor()
            while let edge = try await edgeCursor.next() {
                try insert(
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
                try insert(
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
                try insert(
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
                try insert(
                    try edge.source.requirePropertyGraphIdentifier()
                )
            }
        }

        try resultReservation.reserveAdditional(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: results.count,
                element: String.self
            ).bytes,
            at: .indexScan
        )
        let sortedResults = results.sorted()
        let retained = try DatabaseSharedRetainedArray.adopting(
            sortedResults,
            reservation: resultReservation,
            workMeter: workMeter,
            stage: .indexScan
        )
        transferredReservation = true
        return retained
    }

    /// Fetch items by their node field values
    ///
    /// Uses an index-backed lookup strategy:
    /// 1. If field is "id", use direct ID lookup (O(k) where k = nodeValues.count)
    /// 2. If ScalarIndex exists on field, use index query (O(k) lookups)
    /// A non-identifier graph field without a scalar lookup index is invalid;
    /// connectivity must never silently widen into a table scan.
    private func fetchItemsByNodeValues(
        _ connectedNodes: DatabaseSharedRetainedArray<ConnectedNode>,
        entity: Schema.Entity,
        sourceIndexName: String,
        transaction: any IndexQueryReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?> {
        guard !connectedNodes.isEmpty else {
            return try DatabaseSharedRetainedArray.empty(
                workMeter: workMeter,
                stage: .storageRow
            )
        }

        // Strategy 1: If the field is the ID field, use direct ID lookup
        if field.name == "id" {
            guard T.persistableIdentifierType == .string else {
                throw FusionQueryError.invalidConfiguration(
                    "Graph node identifiers require a String entity identifier when the graph field is 'id'"
                )
            }
            let reservation = try workMeter.reserveIntermediate(
                bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: connectedNodes.count,
                    element: Tuple.self
                ).bytes,
                at: .indexScan
            )
            defer { reservation.release() }
            var identifierTuples: [Tuple] = []
            identifierTuples.reserveCapacity(connectedNodes.count)
            for connectedNode in connectedNodes {
                let primaryKey = try PersistableIdentifierKeyCodec.tuple(
                    for: .string(connectedNode.node),
                    expectedType: T.persistableIdentifierType
                )
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(primaryKey.pack().count) + 32,
                    at: .indexScan
                )
                identifierTuples.append(primaryKey)
            }
            let models = try await transaction.fetchPersistedModelsPreservingOrder(
                    entity: entity,
                    primaryKeys: identifierTuples,
                    partitions: queryContext.partitionValues,
                    workMeter: workMeter
                )
            try validatePersistedModels(
                models,
                primaryKeys: identifierTuples,
                indexName: sourceIndexName
            )
            return models
        }

        // Strategy 2: If there's a ScalarIndex on this field, use it
        if let indexDescriptor = try findScalarIndexForField() {
            return try await fetchUsingScalarIndex(
                connectedNodes: connectedNodes,
                indexDescriptor: indexDescriptor,
                entity: entity,
                transaction: transaction,
                workMeter: workMeter
            )
        }

        throw FusionQueryError.indexNotFound(
            entity: T.persistableType,
            field: field.name,
            indexType: .ordered
        )
    }

    /// Find a ScalarIndex that covers the field
    private func findScalarIndexForField() throws -> IndexDescriptor? {
        queryContext.indexDescriptors(for: T.self).first { descriptor in
            // Check if it's a ScalarIndex
            guard descriptor.type == .ordered else { return false }

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
        connectedNodes: DatabaseSharedRetainedArray<ConnectedNode>,
        indexDescriptor: IndexDescriptor,
        entity: Schema.Entity,
        transaction: any IndexQueryReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?> {
        let searcher = ScalarIndexSearcher(keyFieldCount: 1)
        return try await transaction.withReadableIndex(
            named: indexDescriptor.name,
            indexType: indexDescriptor.type,
            for: T.self,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["depth"]
            )
        ) { readableIndex, scalarTransaction
            -> DatabaseSharedRetainedArray<PersistedModel?> in
            guard let readableIndex else {
                return try DatabaseSharedRetainedArray.empty(
                    workMeter: workMeter,
                    stage: .storageRow
                )
            }
            let reservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<Set<ByteString>>.stride)
                    + UInt64(MemoryLayout<[Tuple]>.stride),
                at: .indexScan
            )
            defer { reservation.release() }
            var seenPacked: Set<ByteString> = []
            var uniqueIds: [Tuple] = []
            let reader = queryContext.storageReader(
                transaction: scalarTransaction
            )
            for connectedNode in connectedNodes {
                try await searcher.forEach(
                    query: ScalarIndexQuery.equals([connectedNode.node]),
                    in: readableIndex.subspace,
                    using: reader
                ) { entry in
                    let packed = entry.itemID.pack()
                    guard !seenPacked.contains(packed) else { return true }
                    try reservation.reserveAdditional(
                        rows: 1,
                        bytes: UInt64(packed.count)
                            + UInt64(MemoryLayout<Tuple>.stride)
                            + 64,
                        at: .indexScan
                    )
                    seenPacked.insert(packed)
                    uniqueIds.append(entry.itemID)
                    return true
                }
            }

            let models = try await scalarTransaction
                .fetchPersistedModelsPreservingOrder(
                    entity: entity,
                    primaryKeys: uniqueIds,
                    partitions: queryContext.partitionValues,
                    workMeter: workMeter
                )
            try validatePersistedModels(
                models,
                primaryKeys: uniqueIds,
                indexName: indexDescriptor.name
            )
            return models
        }
    }

    private func fetchCandidateModels(
        _ identifiers: Set<T.ID>,
        entity: Schema.Entity,
        sourceIndexName: String,
        transaction: any IndexQueryReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?> {
        let reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: identifiers.count,
                element: T.ID.self
            ).adding(
                try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: identifiers.count,
                    element: Tuple.self
                )
            ).bytes,
            at: .indexScan
        )
        defer { reservation.release() }
        var orderedIdentifiers: [T.ID] = []
        orderedIdentifiers.reserveCapacity(identifiers.count)
        orderedIdentifiers.append(contentsOf: identifiers)
        orderedIdentifiers.sort {
            $0.persistableIdentifierValue < $1.persistableIdentifierValue
        }
        var primaryKeys: [Tuple] = []
        primaryKeys.reserveCapacity(identifiers.count)
        for identifier in orderedIdentifiers {
            let primaryKey = try PersistableIdentifierKeyCodec.tuple(
                for: identifier
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: UInt64(primaryKey.pack().count) + 32,
                at: .indexScan
            )
            primaryKeys.append(primaryKey)
        }
        let models = try await transaction.fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: primaryKeys,
                partitions: queryContext.partitionValues,
                workMeter: workMeter
            )
        try validatePersistedModels(
            models,
            primaryKeys: primaryKeys,
            indexName: sourceIndexName
        )
        return models
    }

    private func validatePersistedModels(
        _ models: DatabaseSharedRetainedArray<PersistedModel?>,
        primaryKeys: [Tuple],
        indexName: String
    ) throws {
        precondition(models.count == primaryKeys.count)
        for index in models.indices where models[index] == nil {
            throw GraphIndexError.indexedItemMissing(
                index: indexName,
                primaryKey: primaryKeys[index].pack()
            )
        }
    }
}
