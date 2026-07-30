import DatabaseEngine
import StorageKit

/// Pull-based neighbor scan for one shortest-path frontier slice.
///
/// The slice shares the frontier's storage. The iterator opens one physical
/// identity range at a time and does not materialize an edge batch.
package struct ShortestPathNeighborScan: Sendable {
    package typealias Element = (
        source: GraphIdentity,
        target: GraphIdentity,
        edge: GraphIdentity
    )

    private let scanner: GraphEdgeScanner
    private let snapshot: GraphReadSnapshot
    private let nodes: ArraySlice<GraphIdentity>
    private let edgeLabel: GraphIdentity?
    private let direction: GraphTraversalDirection

    package init(
        scanner: GraphEdgeScanner,
        snapshot: GraphReadSnapshot,
        nodes: ArraySlice<GraphIdentity>,
        edgeLabel: GraphIdentity?,
        direction: GraphTraversalDirection
    ) {
        self.scanner = scanner
        self.snapshot = snapshot
        self.nodes = nodes
        self.edgeLabel = edgeLabel
        self.direction = direction
    }

    package func makeCursor() -> Cursor {
        Cursor(
            scanner: scanner,
            snapshot: snapshot,
            nodes: nodes,
            edgeLabel: edgeLabel,
            direction: direction
        )
    }

    package struct Cursor {
        private let scanner: GraphEdgeScanner
        private let snapshot: GraphReadSnapshot
        private let nodes: ArraySlice<GraphIdentity>
        private let edgeLabel: GraphIdentity?
        private let direction: GraphTraversalDirection
        private var nodeIndex: ArraySlice<GraphIdentity>.Index
        private var activeEdgeCursor: GraphEdgeScan.Cursor?
        private var isFinished = false
        package private(set) var limitReason: LimitReason?

        fileprivate init(
            scanner: GraphEdgeScanner,
            snapshot: GraphReadSnapshot,
            nodes: ArraySlice<GraphIdentity>,
            edgeLabel: GraphIdentity?,
            direction: GraphTraversalDirection
        ) {
            self.scanner = scanner
            self.snapshot = snapshot
            self.nodes = nodes
            self.edgeLabel = edgeLabel
            self.direction = direction
            self.nodeIndex = nodes.startIndex
        }

        package mutating func next() async throws -> Element? {
            guard !isFinished else { return nil }
            try ensureDatabaseTaskIsActive()

            while nodeIndex < nodes.endIndex {
                if var cursor = activeEdgeCursor {
                    activeEdgeCursor = nil
                    if let edge = try await cursor.next() {
                        activeEdgeCursor = cursor
                        guard try consumeWork() else { return nil }
                        return direction == .outgoing
                            ? (edge.source, edge.target, edge.edgeLabel)
                            : (edge.target, edge.source, edge.edgeLabel)
                    }
                    if let reason = snapshot.workBudget?.limitReason {
                        limitReason = reason
                        isFinished = true
                        return nil
                    }
                    nodeIndex = nodes.index(after: nodeIndex)
                    continue
                }

                guard try consumeWork() else { return nil }
                let node = nodes[nodeIndex]
                let edges = direction == .outgoing
                    ? scanner.scanOutgoing(
                        from: node,
                        edgeLabel: edgeLabel,
                        transaction: snapshot.transaction
                    )
                    : scanner.scanIncoming(
                        to: node,
                        edgeLabel: edgeLabel,
                        transaction: snapshot.transaction
                    )
                activeEdgeCursor = edges.makeCursor()
            }

            isFinished = true
            return nil
        }

        private mutating func consumeWork() throws -> Bool {
            guard let workBudget = snapshot.workBudget else { return true }
            guard try workBudget.consume() else {
                guard let reason = workBudget.limitReason else {
                    throw ShortestPathError.inconsistentWorkBudget
                }
                limitReason = reason
                isFinished = true
                return false
            }
            return true
        }
    }
}
