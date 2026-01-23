// GraphEdgeScanner.swift
// GraphIndex - Unified edge scanning utility
//
// Provides correct and consistent edge scanning across all graph algorithms.
// This centralizes the key structure knowledge to prevent bugs from propagating.

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

// MARK: - EdgeInfo

/// Information about a graph edge
public struct EdgeInfo: Sendable, Equatable {
    /// Source node ID
    public let source: String

    /// Target node ID
    public let target: String

    /// Edge label (empty string if no edge field defined in index)
    public let edgeLabel: String

    public init(source: String, target: String, edgeLabel: String) {
        self.source = source
        self.target = target
        self.edgeLabel = edgeLabel
    }
}

// MARK: - GraphEdgeScanner

/// Unified graph edge scanning utility
///
/// Provides correct and consistent edge scanning for all graph algorithms.
/// Centralizes key structure knowledge to prevent bugs from propagating.
///
/// **Key Structures by Strategy**:
///
/// Adjacency (2-index):
/// ```
/// Outgoing: [out]/[edge]/[from]/[to]  (subspace 0)
/// Incoming: [in]/[edge]/[to]/[from]   (subspace 1)
/// ```
///
/// TripleStore/Hexastore (3/6-index):
/// ```
/// SPO: [from]/[edge]/[to]   (subspace 2) - for outgoing edges
/// POS: [edge]/[to]/[from]   (subspace 3) - for incoming with specific label
/// OSP: [to]/[from]/[edge]   (subspace 4) - for incoming with wildcard label
/// ```
///
/// **edgeLabel Parameter Semantics**:
/// ```
/// nil      → Scan ALL labels (wildcard) - full subspace scan + filter
/// ""       → Scan only edges with empty label (unlabeled graphs)
/// "knows"  → Scan only edges with label "knows"
/// ```
///
/// **Performance Note**:
/// - Specific label (including ""): Efficient prefix-based scan O(log n)
/// - Wildcard (nil): Full subspace scan + filter O(n) - use only when necessary
///
/// **Usage**:
/// ```swift
/// // Adjacency strategy (default)
/// let scanner = GraphEdgeScanner(indexSubspace: subspace)
///
/// // TripleStore strategy
/// let scanner = GraphEdgeScanner(indexSubspace: subspace, strategy: .tripleStore)
///
/// // Scan ALL outgoing edges regardless of label
/// for try await edge in scanner.scanOutgoing(from: "A", edgeLabel: nil, transaction: tx) {
///     print("\(edge.source) -> \(edge.target) via \(edge.edgeLabel)")
/// }
/// ```
public struct GraphEdgeScanner: Sendable {

    // MARK: - Properties

    /// Storage strategy
    private let strategy: GraphIndexStrategy

    /// Subspace for outgoing edges
    /// - Adjacency: [out]/[edge]/[from]/[to] (subspace 0)
    /// - TripleStore/Hexastore: [spo]/[from]/[edge]/[to] (subspace 2)
    private let outgoingSubspace: Subspace

    /// Subspace for incoming edges (primary)
    /// - Adjacency: [in]/[edge]/[to]/[from] (subspace 1)
    /// - TripleStore/Hexastore with specific label: [pos]/[edge]/[to]/[from] (subspace 3)
    private let incomingSubspace: Subspace

    /// Subspace for incoming edges with wildcard label (tripleStore/hexastore only)
    /// Uses OSP index: [osp]/[to]/[from]/[edge] (subspace 4)
    private let incomingWildcardSubspace: Subspace?

    // MARK: - Initialization

    /// Initialize with pre-configured subspaces (for adjacency strategy only)
    ///
    /// - Parameters:
    ///   - outgoingSubspace: Subspace for outgoing edges (typically `indexSubspace.subspace(Int64(0))`)
    ///   - incomingSubspace: Subspace for incoming edges (typically `indexSubspace.subspace(Int64(1))`)
    public init(
        outgoingSubspace: Subspace,
        incomingSubspace: Subspace
    ) {
        self.strategy = .adjacency
        self.outgoingSubspace = outgoingSubspace
        self.incomingSubspace = incomingSubspace
        self.incomingWildcardSubspace = nil
    }

    /// Initialize from index subspace with strategy
    ///
    /// Creates appropriate subspaces based on the storage strategy.
    ///
    /// - Parameters:
    ///   - indexSubspace: The base index subspace
    ///   - strategy: Graph index storage strategy (default: .adjacency)
    public init(indexSubspace: Subspace, strategy: GraphIndexStrategy = .adjacency) {
        self.strategy = strategy

        switch strategy {
        case .adjacency:
            // Adjacency: use subspaces 0 (out) and 1 (in)
            self.outgoingSubspace = indexSubspace.subspace(Int64(0))
            self.incomingSubspace = indexSubspace.subspace(Int64(1))
            self.incomingWildcardSubspace = nil

        case .tripleStore, .hexastore:
            // TripleStore/Hexastore:
            // - Outgoing: SPO (subspace 2) for (from, ?, ?) and (from, edge, ?) queries
            // - Incoming with label: POS (subspace 3) for (?, edge, to) queries
            // - Incoming wildcard: OSP (subspace 4) for (?, ?, to) queries
            self.outgoingSubspace = indexSubspace.subspace(Int64(2))  // SPO
            self.incomingSubspace = indexSubspace.subspace(Int64(3))  // POS
            self.incomingWildcardSubspace = indexSubspace.subspace(Int64(4))  // OSP
        }
    }

    /// Initialize from index subspace (backward compatible, assumes adjacency strategy)
    ///
    /// - Parameter indexSubspace: The base index subspace
    public init(indexSubspace: Subspace) {
        self.init(indexSubspace: indexSubspace, strategy: .adjacency)
    }

    // MARK: - Helpers

    /// Build a nested subspace from an array of tuple elements
    ///
    /// Uses the proper Subspace API pattern instead of manual byte concatenation.
    private static func buildPrefixSubspace(
        from base: Subspace,
        elements: [any TupleElement]
    ) -> Subspace {
        var result = base
        for element in elements {
            result = result.subspace(element)
        }
        return result
    }

    // MARK: - Public API

    /// Scan outgoing edges from a node
    ///
    /// Returns all edges where the specified node is the source.
    ///
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - edgeLabel: Edge label filter:
    ///     - `nil`: Scan ALL labels (wildcard) - less efficient, full scan + filter
    ///     - `""`: Scan only edges with empty label (unlabeled graphs)
    ///     - `"X"`: Scan only edges with label "X" (efficient prefix scan)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Sequence of EdgeInfo for outgoing edges
    public func scanOutgoing(
        from nodeID: String,
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) -> AsyncThrowingStream<EdgeInfo, Error> {
        scanEdges(
            nodeID: nodeID,
            edgeLabel: edgeLabel,
            direction: .outgoing,
            transaction: transaction
        )
    }

    /// Scan incoming edges to a node
    ///
    /// Returns all edges where the specified node is the target.
    ///
    /// - Parameters:
    ///   - nodeID: Target node ID
    ///   - edgeLabel: Edge label filter:
    ///     - `nil`: Scan ALL labels (wildcard) - less efficient, full scan + filter
    ///     - `""`: Scan only edges with empty label (unlabeled graphs)
    ///     - `"X"`: Scan only edges with label "X" (efficient prefix scan)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Sequence of EdgeInfo for incoming edges
    public func scanIncoming(
        to nodeID: String,
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) -> AsyncThrowingStream<EdgeInfo, Error> {
        scanEdges(
            nodeID: nodeID,
            edgeLabel: edgeLabel,
            direction: .incoming,
            transaction: transaction
        )
    }

    /// Scan all edges in the graph
    ///
    /// Useful for building the complete graph structure (e.g., for topological sort).
    ///
    /// - Parameters:
    ///   - edgeLabel: Edge label filter:
    ///     - `nil`: Scan ALL edges regardless of label
    ///     - `""`: Scan only edges with empty label
    ///     - `"X"`: Scan only edges with label "X"
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Sequence of all EdgeInfo in the graph
    public func scanAllEdges(
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) -> AsyncThrowingStream<EdgeInfo, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    switch self.strategy {
                    case .adjacency:
                        try await self.scanAllEdgesAdjacency(
                            edgeLabel: edgeLabel,
                            transaction: transaction,
                            continuation: continuation
                        )
                    case .tripleStore, .hexastore:
                        try await self.scanAllEdgesTripleStore(
                            edgeLabel: edgeLabel,
                            transaction: transaction,
                            continuation: continuation
                        )
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Scan all edges using adjacency strategy
    ///
    /// Uses [out] subspace with key structure: [edge]/[from]/[to]
    private func scanAllEdgesAdjacency(
        edgeLabel: String?,
        transaction: any TransactionProtocol,
        continuation: AsyncThrowingStream<EdgeInfo, Error>.Continuation
    ) async throws {
        if let label = edgeLabel {
            // Specific label: Scan [out]/[label]/* prefix
            let prefix = Self.buildPrefixSubspace(from: outgoingSubspace, elements: [label])
            let (beginKey, endKey) = prefix.range()

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let edgeInfo = try self.extractEdgeFromAllEdgesScanAdjacency(
                    key: key,
                    prefix: prefix,
                    edgeLabel: label
                ) {
                    continuation.yield(edgeInfo)
                }
            }
        } else {
            // Wildcard: Scan entire [out]/* subspace
            let (beginKey, endKey) = outgoingSubspace.range()

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let edgeInfo = try self.extractEdgeFromFullScanAdjacency(
                    key: key,
                    subspace: outgoingSubspace
                ) {
                    continuation.yield(edgeInfo)
                }
            }
        }
    }

    /// Scan all edges using tripleStore/hexastore strategy
    ///
    /// **Optimization**:
    /// - Specific label: Uses POS index `[edge]/[to]/[from]` with prefix scan O(E_label)
    /// - Wildcard: Uses SPO index `[from]/[edge]/[to]` with full scan O(E)
    private func scanAllEdgesTripleStore(
        edgeLabel: String?,
        transaction: any TransactionProtocol,
        continuation: AsyncThrowingStream<EdgeInfo, Error>.Continuation
    ) async throws {
        if let label = edgeLabel {
            // Optimized: Use POS index for specific label
            // POS key structure: [edge]/[to]/[from]
            // Prefix scan on [label] is efficient O(E_label)
            let prefix = Self.buildPrefixSubspace(from: incomingSubspace, elements: [label])
            let (beginKey, endKey) = prefix.range()

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let edgeInfo = try self.extractEdgeFromPOSLabelScan(
                    key: key,
                    prefix: prefix,
                    edgeLabel: label
                ) {
                    continuation.yield(edgeInfo)
                }
            }
        } else {
            // Wildcard: Scan entire SPO subspace
            // SPO key structure: [from]/[edge]/[to]
            let (beginKey, endKey) = outgoingSubspace.range()

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let edgeInfo = try self.extractEdgeFromFullScanTripleStore(
                    key: key,
                    subspace: outgoingSubspace,
                    filterEdgeLabel: nil
                ) {
                    continuation.yield(edgeInfo)
                }
            }
        }
    }

    /// Extract edge info from POS key after label prefix
    ///
    /// Key structure after prefix [edge]: [to]/[from]
    private func extractEdgeFromPOSLabelScan(
        key: [UInt8],
        prefix: Subspace,
        edgeLabel: String
    ) throws -> EdgeInfo? {
        let elements = try prefix.unpack(key)

        // Expecting [to, from] after prefix [edge]
        guard elements.count >= 2 else { return nil }
        guard let toElement = elements[0], let fromElement = elements[1] else { return nil }

        let target: String
        if let str = toElement as? String {
            target = str
        } else {
            target = String(describing: toElement)
        }

        let source: String
        if let str = fromElement as? String {
            source = str
        } else {
            source = String(describing: fromElement)
        }

        return EdgeInfo(source: source, target: target, edgeLabel: edgeLabel)
    }

    /// Extract edge info from SPO key with optional label filter
    ///
    /// Key structure: [from]/[edge]/[to]
    private func extractEdgeFromFullScanTripleStore(
        key: [UInt8],
        subspace: Subspace,
        filterEdgeLabel: String?
    ) throws -> EdgeInfo? {
        let elements = try subspace.unpack(key)

        // Expecting [from, edge, to]
        guard elements.count >= 3 else { return nil }

        guard let fromElement = elements[0],
              let edgeElement = elements[1],
              let toElement = elements[2] else { return nil }

        let source: String
        if let str = fromElement as? String {
            source = str
        } else {
            source = String(describing: fromElement)
        }

        let edgeLabel: String
        if let str = edgeElement as? String {
            edgeLabel = str
        } else {
            edgeLabel = String(describing: edgeElement)
        }

        let target: String
        if let str = toElement as? String {
            target = str
        } else {
            target = String(describing: toElement)
        }

        // Apply label filter if specified
        if let filter = filterEdgeLabel, edgeLabel != filter {
            return nil
        }

        return EdgeInfo(source: source, target: target, edgeLabel: edgeLabel)
    }

    /// Batch scan outgoing edges for multiple source nodes
    ///
    /// More efficient than scanning one node at a time when using specific labels.
    ///
    /// - Parameters:
    ///   - nodeIDs: Source node IDs
    ///   - edgeLabel: Edge label filter:
    ///     - `nil`: Scan ALL labels (wildcard) - full scan + filter by nodeIDs
    ///     - `""`: Scan only edges with empty label
    ///     - `"X"`: Scan only edges with label "X"
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Array of EdgeInfo for all outgoing edges
    public func batchScanOutgoing(
        from nodeIDs: [String],
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        try await batchScan(
            nodeIDs: nodeIDs,
            edgeLabel: edgeLabel,
            direction: .outgoing,
            transaction: transaction
        )
    }

    /// Batch scan incoming edges for multiple target nodes
    ///
    /// - Parameters:
    ///   - nodeIDs: Target node IDs
    ///   - edgeLabel: Edge label filter:
    ///     - `nil`: Scan ALL labels (wildcard) - full scan + filter by nodeIDs
    ///     - `""`: Scan only edges with empty label
    ///     - `"X"`: Scan only edges with label "X"
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Array of EdgeInfo for all incoming edges
    public func batchScanIncoming(
        to nodeIDs: [String],
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        try await batchScan(
            nodeIDs: nodeIDs,
            edgeLabel: edgeLabel,
            direction: .incoming,
            transaction: transaction
        )
    }

    // MARK: - Direction Enum

    /// Edge traversal direction
    public enum Direction: Sendable {
        case outgoing
        case incoming
    }

    // MARK: - Private Methods

    /// Core edge scanning implementation
    ///
    /// Handles two modes based on strategy:
    /// - Adjacency: [edge]/[nodeID]/* prefix scan
    /// - TripleStore/Hexastore: [nodeID]/[edge]/* or [nodeID]/* prefix scan
    ///
    /// And two label modes:
    /// - Specific label (including ""): Efficient prefix scan
    /// - Wildcard (nil): Full subspace scan + manual filter
    private func scanEdges(
        nodeID: String,
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol
    ) -> AsyncThrowingStream<EdgeInfo, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    switch self.strategy {
                    case .adjacency:
                        try await self.scanEdgesAdjacency(
                            nodeID: nodeID,
                            edgeLabel: edgeLabel,
                            direction: direction,
                            transaction: transaction,
                            continuation: continuation
                        )
                    case .tripleStore, .hexastore:
                        try await self.scanEdgesTripleStore(
                            nodeID: nodeID,
                            edgeLabel: edgeLabel,
                            direction: direction,
                            transaction: transaction,
                            continuation: continuation
                        )
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Scan edges using adjacency strategy
    ///
    /// Key structure:
    /// - Outgoing: [out]/[edge]/[from]/[to]
    /// - Incoming: [in]/[edge]/[to]/[from]
    private func scanEdgesAdjacency(
        nodeID: String,
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol,
        continuation: AsyncThrowingStream<EdgeInfo, Error>.Continuation
    ) async throws {
        let scanSubspace = direction == .outgoing ? outgoingSubspace : incomingSubspace

        if let label = edgeLabel {
            // Specific label: prefix on [edge]/[nodeID]
            let prefix = Self.buildPrefixSubspace(from: scanSubspace, elements: [label, nodeID])
            let (beginKey, endKey) = prefix.range()

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let otherNodeID = try self.extractNodeID(key: key, prefix: prefix) {
                    let edgeInfo: EdgeInfo
                    if direction == .outgoing {
                        edgeInfo = EdgeInfo(source: nodeID, target: otherNodeID, edgeLabel: label)
                    } else {
                        edgeInfo = EdgeInfo(source: otherNodeID, target: nodeID, edgeLabel: label)
                    }
                    continuation.yield(edgeInfo)
                }
            }
        } else {
            // Wildcard: full scan + filter
            let (beginKey, endKey) = scanSubspace.range()

            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let edgeInfo = try self.extractEdgeFromWildcardScan(
                    key: key,
                    subspace: scanSubspace,
                    direction: direction,
                    filterNodeID: nodeID
                ) {
                    continuation.yield(edgeInfo)
                }
            }
        }
    }

    /// Scan edges using tripleStore/hexastore strategy
    ///
    /// Key structures:
    /// - SPO (outgoing): [from]/[edge]/[to]
    /// - POS (incoming with label): [edge]/[to]/[from]
    /// - OSP (incoming wildcard): [to]/[from]/[edge]
    private func scanEdgesTripleStore(
        nodeID: String,
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol,
        continuation: AsyncThrowingStream<EdgeInfo, Error>.Continuation
    ) async throws {
        if direction == .outgoing {
            // Use SPO index: [from]/[edge]/[to]
            if let label = edgeLabel {
                // Specific label: prefix on [from]/[edge]
                let prefix = Self.buildPrefixSubspace(from: outgoingSubspace, elements: [nodeID, label])
                let (beginKey, endKey) = prefix.range()

                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: true
                )

                for try await (key, _) in stream {
                    // Extract [to] from key
                    if let toNodeID = try self.extractNodeID(key: key, prefix: prefix) {
                        continuation.yield(EdgeInfo(source: nodeID, target: toNodeID, edgeLabel: label))
                    }
                }
            } else {
                // Wildcard: prefix on [from], extract [edge, to]
                let prefix = Self.buildPrefixSubspace(from: outgoingSubspace, elements: [nodeID])
                let (beginKey, endKey) = prefix.range()

                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: true
                )

                for try await (key, _) in stream {
                    // Extract [edge, to] from key
                    if let edgeInfo = try self.extractEdgeToFromSPOWildcard(key: key, prefix: prefix, fromNodeID: nodeID) {
                        continuation.yield(edgeInfo)
                    }
                }
            }
        } else {
            // Incoming edges
            if let label = edgeLabel {
                // Use POS index: [edge]/[to]/[from]
                // Prefix on [edge]/[to]
                let prefix = Self.buildPrefixSubspace(from: incomingSubspace, elements: [label, nodeID])
                let (beginKey, endKey) = prefix.range()

                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: true
                )

                for try await (key, _) in stream {
                    // Extract [from] from key
                    if let fromNodeID = try self.extractNodeID(key: key, prefix: prefix) {
                        continuation.yield(EdgeInfo(source: fromNodeID, target: nodeID, edgeLabel: label))
                    }
                }
            } else {
                // Use OSP index: [to]/[from]/[edge]
                // Prefix on [to]
                guard let ospSubspace = incomingWildcardSubspace else {
                    // Fallback to POS full scan if OSP not available (shouldn't happen)
                    return
                }

                let prefix = Self.buildPrefixSubspace(from: ospSubspace, elements: [nodeID])
                let (beginKey, endKey) = prefix.range()

                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: true
                )

                for try await (key, _) in stream {
                    // Extract [from, edge] from key
                    if let edgeInfo = try self.extractEdgeFromOSPWildcard(key: key, prefix: prefix, toNodeID: nodeID) {
                        continuation.yield(edgeInfo)
                    }
                }
            }
        }
    }

    /// Extract [edge, to] from SPO key after [from] prefix
    private func extractEdgeToFromSPOWildcard(key: [UInt8], prefix: Subspace, fromNodeID: String) throws -> EdgeInfo? {
        let elements = try prefix.unpack(key)

        // Expecting [edge, to] after prefix [from]
        guard elements.count >= 2 else { return nil }
        guard let edgeElement = elements[0], let toElement = elements[1] else { return nil }

        let edgeLabel: String
        if let str = edgeElement as? String {
            edgeLabel = str
        } else {
            edgeLabel = String(describing: edgeElement)
        }

        let toNodeID: String
        if let str = toElement as? String {
            toNodeID = str
        } else {
            toNodeID = String(describing: toElement)
        }

        return EdgeInfo(source: fromNodeID, target: toNodeID, edgeLabel: edgeLabel)
    }

    /// Extract [from, edge] from OSP key after [to] prefix
    private func extractEdgeFromOSPWildcard(key: [UInt8], prefix: Subspace, toNodeID: String) throws -> EdgeInfo? {
        let elements = try prefix.unpack(key)

        // Expecting [from, edge] after prefix [to]
        guard elements.count >= 2 else { return nil }
        guard let fromElement = elements[0], let edgeElement = elements[1] else { return nil }

        let fromNodeID: String
        if let str = fromElement as? String {
            fromNodeID = str
        } else {
            fromNodeID = String(describing: fromElement)
        }

        let edgeLabel: String
        if let str = edgeElement as? String {
            edgeLabel = str
        } else {
            edgeLabel = String(describing: edgeElement)
        }

        return EdgeInfo(source: fromNodeID, target: toNodeID, edgeLabel: edgeLabel)
    }

    /// Batch scan implementation
    ///
    /// Dispatches to strategy-specific implementation.
    private func batchScan(
        nodeIDs: [String],
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        guard !nodeIDs.isEmpty else { return [] }

        switch strategy {
        case .adjacency:
            return try await batchScanAdjacency(
                nodeIDs: nodeIDs,
                edgeLabel: edgeLabel,
                direction: direction,
                transaction: transaction
            )
        case .tripleStore, .hexastore:
            return try await batchScanTripleStore(
                nodeIDs: nodeIDs,
                edgeLabel: edgeLabel,
                direction: direction,
                transaction: transaction
            )
        }
    }

    /// Batch scan using adjacency strategy
    private func batchScanAdjacency(
        nodeIDs: [String],
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        let scanSubspace = direction == .outgoing ? outgoingSubspace : incomingSubspace

        if let label = edgeLabel {
            // Specific label: Efficient per-node prefix scans
            return try await batchScanWithSpecificLabelAdjacency(
                nodeIDs: nodeIDs,
                label: label,
                direction: direction,
                scanSubspace: scanSubspace,
                transaction: transaction
            )
        } else {
            // Wildcard: Full subspace scan + filter by nodeID set
            return try await batchScanWithWildcardAdjacency(
                nodeIDs: Set(nodeIDs),
                direction: direction,
                scanSubspace: scanSubspace,
                transaction: transaction
            )
        }
    }

    /// Batch scan using tripleStore/hexastore strategy
    private func batchScanTripleStore(
        nodeIDs: [String],
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        var results: [EdgeInfo] = []

        if direction == .outgoing {
            // Use SPO index: [from]/[edge]/[to]
            if let label = edgeLabel {
                // Specific label: prefix on [from]/[edge]
                for nodeID in nodeIDs {
                    let prefix = Self.buildPrefixSubspace(from: outgoingSubspace, elements: [nodeID, label])
                    let (beginKey, endKey) = prefix.range()

                    let stream = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(beginKey),
                        endSelector: .firstGreaterOrEqual(endKey),
                        snapshot: true
                    )

                    for try await (key, _) in stream {
                        if let toNodeID = try extractNodeID(key: key, prefix: prefix) {
                            results.append(EdgeInfo(source: nodeID, target: toNodeID, edgeLabel: label))
                        }
                    }
                }
            } else {
                // Wildcard: prefix on [from]
                for nodeID in nodeIDs {
                    let prefix = Self.buildPrefixSubspace(from: outgoingSubspace, elements: [nodeID])
                    let (beginKey, endKey) = prefix.range()

                    let stream = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(beginKey),
                        endSelector: .firstGreaterOrEqual(endKey),
                        snapshot: true
                    )

                    for try await (key, _) in stream {
                        if let edgeInfo = try extractEdgeToFromSPOWildcard(key: key, prefix: prefix, fromNodeID: nodeID) {
                            results.append(edgeInfo)
                        }
                    }
                }
            }
        } else {
            // Incoming edges
            if let label = edgeLabel {
                // Use POS index: [edge]/[to]/[from]
                for nodeID in nodeIDs {
                    let prefix = Self.buildPrefixSubspace(from: incomingSubspace, elements: [label, nodeID])
                    let (beginKey, endKey) = prefix.range()

                    let stream = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(beginKey),
                        endSelector: .firstGreaterOrEqual(endKey),
                        snapshot: true
                    )

                    for try await (key, _) in stream {
                        if let fromNodeID = try extractNodeID(key: key, prefix: prefix) {
                            results.append(EdgeInfo(source: fromNodeID, target: nodeID, edgeLabel: label))
                        }
                    }
                }
            } else {
                // Use OSP index: [to]/[from]/[edge]
                guard let ospSubspace = incomingWildcardSubspace else {
                    return results
                }

                for nodeID in nodeIDs {
                    let prefix = Self.buildPrefixSubspace(from: ospSubspace, elements: [nodeID])
                    let (beginKey, endKey) = prefix.range()

                    let stream = transaction.getRange(
                        beginSelector: .firstGreaterOrEqual(beginKey),
                        endSelector: .firstGreaterOrEqual(endKey),
                        snapshot: true
                    )

                    for try await (key, _) in stream {
                        if let edgeInfo = try extractEdgeFromOSPWildcard(key: key, prefix: prefix, toNodeID: nodeID) {
                            results.append(edgeInfo)
                        }
                    }
                }
            }
        }

        return results
    }

    /// Batch scan with specific edge label (adjacency strategy)
    private func batchScanWithSpecificLabelAdjacency(
        nodeIDs: [String],
        label: String,
        direction: Direction,
        scanSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        // Pre-compute scan parameters
        let scanParams: [(nodeID: String, beginKey: [UInt8], endKey: [UInt8], prefix: Subspace)] =
            nodeIDs.map { nodeID in
                let prefix = Self.buildPrefixSubspace(from: scanSubspace, elements: [label, nodeID])
                let (beginKey, endKey) = prefix.range()
                return (nodeID, beginKey, endKey, prefix)
            }

        var results: [EdgeInfo] = []

        for (nodeID, beginKey, endKey, prefix) in scanParams {
            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: true
            )

            for try await (key, _) in stream {
                if let otherNodeID = try extractNodeID(key: key, prefix: prefix) {
                    let edgeInfo: EdgeInfo
                    if direction == .outgoing {
                        edgeInfo = EdgeInfo(
                            source: nodeID,
                            target: otherNodeID,
                            edgeLabel: label
                        )
                    } else {
                        edgeInfo = EdgeInfo(
                            source: otherNodeID,
                            target: nodeID,
                            edgeLabel: label
                        )
                    }
                    results.append(edgeInfo)
                }
            }
        }

        return results
    }

    /// Batch scan with wildcard (adjacency strategy)
    ///
    /// More efficient than per-node wildcard scans when nodeIDs set is large,
    /// as it only requires a single pass through the subspace.
    private func batchScanWithWildcardAdjacency(
        nodeIDs: Set<String>,
        direction: Direction,
        scanSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        let (beginKey, endKey) = scanSubspace.range()

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        var results: [EdgeInfo] = []

        for try await (key, _) in stream {
            if let edgeInfo = try extractEdgeFromBatchWildcardScan(
                key: key,
                subspace: scanSubspace,
                direction: direction,
                filterNodeIDs: nodeIDs
            ) {
                results.append(edgeInfo)
            }
        }

        return results
    }

    /// Extract edge info from batch wildcard scan
    ///
    /// Key structure: [edge]/[nodeA]/[nodeB]
    /// Filters by nodeIDs set based on direction.
    private func extractEdgeFromBatchWildcardScan(
        key: [UInt8],
        subspace: Subspace,
        direction: Direction,
        filterNodeIDs: Set<String>
    ) throws -> EdgeInfo? {
        let elements = try subspace.unpack(key)

        // Expecting [edge, nodeA, nodeB]
        guard elements.count >= 3 else { return nil }

        guard let edgeElement = elements[0],
              let nodeAElement = elements[1],
              let nodeBElement = elements[2] else { return nil }

        let edgeLabel: String
        if let str = edgeElement as? String {
            edgeLabel = str
        } else {
            edgeLabel = String(describing: edgeElement)
        }

        let nodeA: String
        if let str = nodeAElement as? String {
            nodeA = str
        } else {
            nodeA = String(describing: nodeAElement)
        }

        let nodeB: String
        if let str = nodeBElement as? String {
            nodeB = str
        } else {
            nodeB = String(describing: nodeBElement)
        }

        // Filter based on direction
        if direction == .outgoing {
            // [edge]/[from]/[to] → from must be in filterNodeIDs
            guard filterNodeIDs.contains(nodeA) else { return nil }
            return EdgeInfo(source: nodeA, target: nodeB, edgeLabel: edgeLabel)
        } else {
            // [edge]/[to]/[from] → to must be in filterNodeIDs
            guard filterNodeIDs.contains(nodeA) else { return nil }
            return EdgeInfo(source: nodeB, target: nodeA, edgeLabel: edgeLabel)
        }
    }

    /// Extract node ID from key after prefix
    ///
    /// After unpacking with prefix [edge]/[nodeID], the remaining element is the other node ID.
    private func extractNodeID(key: [UInt8], prefix: Subspace) throws -> String? {
        let elements = try prefix.unpack(key)

        guard !elements.isEmpty else { return nil }

        // The remaining element after prefix is the other node ID
        guard let lastElement = elements[elements.count - 1] else { return nil }

        if let str = lastElement as? String {
            return str
        } else {
            return String(describing: lastElement)
        }
    }

    /// Extract edge info from wildcard scan (edgeLabel=nil)
    ///
    /// Key structure: [edge]/[from]/[to] (outgoing) or [edge]/[to]/[from] (incoming)
    /// Filters by nodeID based on direction.
    private func extractEdgeFromWildcardScan(
        key: [UInt8],
        subspace: Subspace,
        direction: Direction,
        filterNodeID: String
    ) throws -> EdgeInfo? {
        let elements = try subspace.unpack(key)

        // Expecting [edge, nodeA, nodeB] where:
        //   Outgoing: [edge, from, to] → filter by from == filterNodeID
        //   Incoming: [edge, to, from] → filter by to == filterNodeID
        guard elements.count >= 3 else { return nil }

        guard let edgeElement = elements[0],
              let nodeAElement = elements[1],
              let nodeBElement = elements[2] else { return nil }

        let edgeLabel: String
        if let str = edgeElement as? String {
            edgeLabel = str
        } else {
            edgeLabel = String(describing: edgeElement)
        }

        let nodeA: String
        if let str = nodeAElement as? String {
            nodeA = str
        } else {
            nodeA = String(describing: nodeAElement)
        }

        let nodeB: String
        if let str = nodeBElement as? String {
            nodeB = str
        } else {
            nodeB = String(describing: nodeBElement)
        }

        // Apply filter based on direction
        if direction == .outgoing {
            // [edge]/[from]/[to] → from must match filterNodeID
            guard nodeA == filterNodeID else { return nil }
            return EdgeInfo(source: nodeA, target: nodeB, edgeLabel: edgeLabel)
        } else {
            // [edge]/[to]/[from] → to must match filterNodeID
            guard nodeA == filterNodeID else { return nil }
            return EdgeInfo(source: nodeB, target: nodeA, edgeLabel: edgeLabel)
        }
    }

    /// Extract edge info from all-edges scan with specific label (adjacency)
    ///
    /// Key structure after edge label prefix: [from]/[to]
    private func extractEdgeFromAllEdgesScanAdjacency(
        key: [UInt8],
        prefix: Subspace,
        edgeLabel: String
    ) throws -> EdgeInfo? {
        let elements = try prefix.unpack(key)

        // Expecting [from, to] after prefix [edge]
        guard elements.count >= 2 else { return nil }

        guard let fromElement = elements[0],
              let toElement = elements[1] else { return nil }

        let source: String
        if let str = fromElement as? String {
            source = str
        } else {
            source = String(describing: fromElement)
        }

        let target: String
        if let str = toElement as? String {
            target = str
        } else {
            target = String(describing: toElement)
        }

        return EdgeInfo(source: source, target: target, edgeLabel: edgeLabel)
    }

    /// Extract edge info from full subspace scan (edgeLabel=nil, adjacency)
    ///
    /// Key structure: [edge]/[from]/[to]
    private func extractEdgeFromFullScanAdjacency(
        key: [UInt8],
        subspace: Subspace
    ) throws -> EdgeInfo? {
        let elements = try subspace.unpack(key)

        // Expecting [edge, from, to]
        guard elements.count >= 3 else { return nil }

        guard let edgeElement = elements[0],
              let fromElement = elements[1],
              let toElement = elements[2] else { return nil }

        let edgeLabel: String
        if let str = edgeElement as? String {
            edgeLabel = str
        } else {
            edgeLabel = String(describing: edgeElement)
        }

        let source: String
        if let str = fromElement as? String {
            source = str
        } else {
            source = String(describing: fromElement)
        }

        let target: String
        if let str = toElement as? String {
            target = str
        } else {
            target = String(describing: toElement)
        }

        return EdgeInfo(source: source, target: target, edgeLabel: edgeLabel)
    }

    // MARK: - Node Enumeration

    /// Get all unique nodes in the graph
    ///
    /// Scans all edges and extracts unique source and target nodes.
    /// Used for Property Path evaluation when both endpoints are unbound variables.
    ///
    /// **Performance Note**:
    /// This requires a full scan of all edges and is O(E) where E is the number of edges.
    /// Use sparingly and prefer bounded queries when possible.
    ///
    /// - Parameters:
    ///   - edgeLabel: Edge label filter (nil for all edges)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Set of all unique node IDs in the graph
    public func getAllNodes(
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        var nodes = Set<String>()

        for try await edge in scanAllEdges(edgeLabel: edgeLabel, transaction: transaction) {
            nodes.insert(edge.source)
            nodes.insert(edge.target)
        }

        return nodes
    }

    /// Get all unique nodes in the graph with early termination
    ///
    /// Like `getAllNodes` but stops after collecting `maxNodes` unique nodes.
    /// Useful for Property Path evaluation with result limits.
    ///
    /// - Parameters:
    ///   - edgeLabel: Edge label filter (nil for all edges)
    ///   - maxNodes: Maximum number of nodes to collect
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Set of unique node IDs (up to maxNodes)
    public func getAllNodes(
        edgeLabel: String?,
        maxNodes: Int,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        var nodes = Set<String>()

        for try await edge in scanAllEdges(edgeLabel: edgeLabel, transaction: transaction) {
            nodes.insert(edge.source)
            nodes.insert(edge.target)

            if nodes.count >= maxNodes {
                break
            }
        }

        return nodes
    }

    // MARK: - Convenience Methods (Collected Results)

    /// Scan all outgoing edges from a node and collect results
    ///
    /// Convenience method that collects stream results into an array.
    ///
    /// - Parameters:
    ///   - nodeID: Source node ID
    ///   - edgeLabel: Edge label filter (nil for wildcard)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Array of EdgeInfo for outgoing edges
    public func scanAllOutgoing(
        from nodeID: String,
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        var results: [EdgeInfo] = []
        for try await edge in scanOutgoing(from: nodeID, edgeLabel: edgeLabel, transaction: transaction) {
            results.append(edge)
        }
        return results
    }

    /// Scan all incoming edges to a node and collect results
    ///
    /// Convenience method that collects stream results into an array.
    ///
    /// - Parameters:
    ///   - nodeID: Target node ID
    ///   - edgeLabel: Edge label filter (nil for wildcard)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Array of EdgeInfo for incoming edges
    public func scanAllIncoming(
        to nodeID: String,
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        var results: [EdgeInfo] = []
        for try await edge in scanIncoming(to: nodeID, edgeLabel: edgeLabel, transaction: transaction) {
            results.append(edge)
        }
        return results
    }

    /// Scan all edges with a callback for each edge
    ///
    /// Iterates through all edges and calls the callback for each.
    /// The callback can return `false` to stop iteration.
    ///
    /// - Parameters:
    ///   - edgeLabel: Edge label filter (nil for wildcard)
    ///   - transaction: FDB transaction for the scan
    ///   - callback: Callback for each edge. Return `false` to stop.
    public func scanAllEdges(
        edgeLabel: String?,
        transaction: any TransactionProtocol,
        _ callback: (EdgeInfo) throws -> Bool
    ) async throws {
        for try await edge in scanAllEdges(edgeLabel: edgeLabel, transaction: transaction) {
            if try !callback(edge) {
                break
            }
        }
    }

    // MARK: - Grouped Batch Methods

    /// Batch scan outgoing edges for multiple source nodes, grouped by source
    ///
    /// Returns a dictionary mapping each source node to its outgoing edges.
    /// More efficient than calling `scanAllOutgoing` for each node individually.
    ///
    /// **Performance Note**:
    /// When using a specific edge label, this performs one prefix scan per node.
    /// When using wildcard (nil), it performs a single full scan and filters.
    ///
    /// - Parameters:
    ///   - sources: Array of source node IDs
    ///   - edgeLabel: Edge label filter (nil for wildcard)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Dictionary mapping source node ID to array of outgoing EdgeInfo
    public func batchScanAllOutgoing(
        from sources: [String],
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> [String: [EdgeInfo]] {
        let edges = try await batchScanOutgoing(
            from: sources,
            edgeLabel: edgeLabel,
            transaction: transaction
        )

        // Group edges by source node
        var grouped: [String: [EdgeInfo]] = [:]
        for source in sources {
            grouped[source] = []
        }
        for edge in edges {
            grouped[edge.source, default: []].append(edge)
        }
        return grouped
    }

    /// Batch scan incoming edges for multiple target nodes, grouped by target
    ///
    /// Returns a dictionary mapping each target node to its incoming edges.
    /// More efficient than calling `scanAllIncoming` for each node individually.
    ///
    /// **Performance Note**:
    /// When using a specific edge label, this performs one prefix scan per node.
    /// When using wildcard (nil), it performs a single full scan and filters.
    ///
    /// - Parameters:
    ///   - targets: Array of target node IDs
    ///   - edgeLabel: Edge label filter (nil for wildcard)
    ///   - transaction: FDB transaction for the scan
    /// - Returns: Dictionary mapping target node ID to array of incoming EdgeInfo
    public func batchScanAllIncoming(
        to targets: [String],
        edgeLabel: String?,
        transaction: any TransactionProtocol
    ) async throws -> [String: [EdgeInfo]] {
        let edges = try await batchScanIncoming(
            to: targets,
            edgeLabel: edgeLabel,
            transaction: transaction
        )

        // Group edges by target node
        var grouped: [String: [EdgeInfo]] = [:]
        for target in targets {
            grouped[target] = []
        }
        for edge in edges {
            grouped[edge.target, default: []].append(edge)
        }
        return grouped
    }
}
