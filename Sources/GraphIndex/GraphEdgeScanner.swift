// GraphEdgeScanner.swift
// GraphIndex - Unified edge scanning utility
//
// Provides correct and consistent edge scanning across all graph algorithms.
// This centralizes the key structure knowledge to prevent bugs from propagating.

import Foundation
import Core
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
/// **Key Structure** (Adjacency Strategy):
/// ```
/// Outgoing: [out]/[edge]/[from]/[to]
/// Incoming: [in]/[edge]/[to]/[from]
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
/// let scanner = GraphEdgeScanner(indexSubspace: subspace)
///
/// // Scan ALL outgoing edges regardless of label
/// for try await edge in scanner.scanOutgoing(from: "A", edgeLabel: nil, transaction: tx) {
///     print("\(edge.source) -> \(edge.target) via \(edge.edgeLabel)")
/// }
///
/// // Scan only "follows" edges (efficient prefix scan)
/// for try await edge in scanner.scanOutgoing(from: "A", edgeLabel: "follows", transaction: tx) {
///     print("\(edge.source) -> \(edge.target)")
/// }
/// ```
public struct GraphEdgeScanner: Sendable {

    // MARK: - Properties

    /// Subspace for outgoing edges: [out]/[edge]/[from]/[to]
    private let outgoingSubspace: Subspace

    /// Subspace for incoming edges: [in]/[edge]/[to]/[from]
    private let incomingSubspace: Subspace

    // MARK: - Initialization

    /// Initialize with pre-configured subspaces
    ///
    /// - Parameters:
    ///   - outgoingSubspace: Subspace for outgoing edges (typically `indexSubspace.subspace(Int64(0))`)
    ///   - incomingSubspace: Subspace for incoming edges (typically `indexSubspace.subspace(Int64(1))`)
    public init(
        outgoingSubspace: Subspace,
        incomingSubspace: Subspace
    ) {
        self.outgoingSubspace = outgoingSubspace
        self.incomingSubspace = incomingSubspace
    }

    /// Initialize from index subspace
    ///
    /// Automatically creates outgoing (key 0) and incoming (key 1) subspaces.
    ///
    /// - Parameter indexSubspace: The base index subspace
    public init(indexSubspace: Subspace) {
        self.outgoingSubspace = indexSubspace.subspace(Int64(0))
        self.incomingSubspace = indexSubspace.subspace(Int64(1))
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
                    if let label = edgeLabel {
                        // Specific label: Scan [out]/[label]/* prefix
                        let prefix = Subspace(
                            prefix: outgoingSubspace.prefix + Tuple([label]).pack()
                        )
                        let (beginKey, endKey) = prefix.range()

                        let stream = transaction.getRange(
                            beginSelector: .firstGreaterOrEqual(beginKey),
                            endSelector: .firstGreaterOrEqual(endKey),
                            snapshot: true
                        )

                        for try await (key, _) in stream {
                            if let edgeInfo = try self.extractEdgeFromAllEdgesScan(
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
                            if let edgeInfo = try self.extractEdgeFromFullScan(
                                key: key,
                                subspace: outgoingSubspace
                            ) {
                                continuation.yield(edgeInfo)
                            }
                        }
                    }

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
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
    /// Handles two modes:
    /// - Specific label (including ""): Efficient prefix scan on [edge]/[nodeID]/*
    /// - Wildcard (nil): Full subspace scan + manual filter on nodeID
    private func scanEdges(
        nodeID: String,
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol
    ) -> AsyncThrowingStream<EdgeInfo, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    let scanSubspace = direction == .outgoing ? outgoingSubspace : incomingSubspace

                    if let label = edgeLabel {
                        // Specific label: Efficient prefix scan
                        // Key structure:
                        //   Outgoing: [edge]/[from]/[to] → prefix [edge]/[from] → extract [to]
                        //   Incoming: [edge]/[to]/[from] → prefix [edge]/[to] → extract [from]
                        let prefix = Subspace(
                            prefix: scanSubspace.prefix + Tuple([label, nodeID]).pack()
                        )
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
                        // Wildcard: Full subspace scan + filter by nodeID
                        // Key structure: [edge]/[from]/[to] or [edge]/[to]/[from]
                        // We need to scan all edges and filter by nodeID
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

                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Batch scan implementation
    ///
    /// Handles two modes:
    /// - Specific label (including ""): Efficient per-node prefix scans
    /// - Wildcard (nil): Full subspace scan + filter by nodeID set
    private func batchScan(
        nodeIDs: [String],
        edgeLabel: String?,
        direction: Direction,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        guard !nodeIDs.isEmpty else { return [] }

        let scanSubspace = direction == .outgoing ? outgoingSubspace : incomingSubspace

        if let label = edgeLabel {
            // Specific label: Efficient per-node prefix scans
            return try await batchScanWithSpecificLabel(
                nodeIDs: nodeIDs,
                label: label,
                direction: direction,
                scanSubspace: scanSubspace,
                transaction: transaction
            )
        } else {
            // Wildcard: Full subspace scan + filter by nodeID set
            return try await batchScanWithWildcard(
                nodeIDs: Set(nodeIDs),
                direction: direction,
                scanSubspace: scanSubspace,
                transaction: transaction
            )
        }
    }

    /// Batch scan with specific edge label (efficient per-node prefix scans)
    private func batchScanWithSpecificLabel(
        nodeIDs: [String],
        label: String,
        direction: Direction,
        scanSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [EdgeInfo] {
        // Pre-compute scan parameters
        let scanParams: [(nodeID: String, beginKey: [UInt8], endKey: [UInt8], prefix: Subspace)] =
            nodeIDs.map { nodeID in
                let prefix = Subspace(
                    prefix: scanSubspace.prefix + Tuple([label, nodeID]).pack()
                )
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

    /// Batch scan with wildcard (full subspace scan + filter)
    ///
    /// More efficient than per-node wildcard scans when nodeIDs set is large,
    /// as it only requires a single pass through the subspace.
    private func batchScanWithWildcard(
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

    /// Extract edge info from all-edges scan with specific label
    ///
    /// Key structure after edge label prefix: [from]/[to]
    private func extractEdgeFromAllEdgesScan(
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

    /// Extract edge info from full subspace scan (edgeLabel=nil)
    ///
    /// Key structure: [edge]/[from]/[to]
    private func extractEdgeFromFullScan(
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
}
