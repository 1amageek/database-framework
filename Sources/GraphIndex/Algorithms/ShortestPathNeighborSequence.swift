import DatabaseEngine
import StorageKit

/// Pull-based neighbor scan for one shortest-path frontier slice.
///
/// The slice shares the frontier's storage. The iterator opens one physical
/// identity range at a time and does not materialize an edge batch.
package struct ShortestPathNeighborSequence: AsyncSequence, Sendable {
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

    package func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            scanner: scanner,
            snapshot: snapshot,
            nodes: nodes,
            edgeLabel: edgeLabel,
            direction: direction
        )
    }

    package struct AsyncIterator: AsyncIteratorProtocol {
        private let scanner: GraphEdgeScanner
        private let snapshot: GraphReadSnapshot
        private let nodes: ArraySlice<GraphIdentity>
        private let edgeLabel: GraphIdentity?
        private let direction: GraphTraversalDirection
        private var nodeIndex: ArraySlice<GraphIdentity>.Index
        private var activeEdges: GraphEdgeSequence.AsyncIterator?
        private var isFinished = false

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
            try Task.checkCancellation()

            while nodeIndex < nodes.endIndex {
                if var iterator = activeEdges {
                    activeEdges = nil
                    if let edge = try await iterator.next() {
                        activeEdges = iterator
                        try consumeWork()
                        return direction == .outgoing
                            ? (edge.source, edge.target, edge.edgeLabel)
                            : (edge.target, edge.source, edge.edgeLabel)
                    }
                    if let reason = snapshot.workBudget?.limitReason {
                        throw ShortestPathError.incomplete(reason)
                    }
                    nodeIndex = nodes.index(after: nodeIndex)
                    continue
                }

                try consumeWork()
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
                activeEdges = edges.makeAsyncIterator()
            }

            isFinished = true
            return nil
        }

        private func consumeWork() throws {
            guard let workBudget = snapshot.workBudget else { return }
            guard try workBudget.consume() else {
                guard let reason = workBudget.limitReason else {
                    throw ShortestPathError.inconsistentWorkBudget
                }
                throw ShortestPathError.incomplete(reason)
            }
        }
    }
}
