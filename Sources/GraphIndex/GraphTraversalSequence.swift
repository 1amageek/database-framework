/// Pull-based breadth-first traversal over one stable storage snapshot.
///
/// The iterator retains only the BFS frontier and visited identities. Edge
/// payload bytes remain owned by their physical key buffers and are not copied.
package struct GraphTraversalSequence: AsyncSequence, Sendable {
    package typealias Element = GraphTraversalStep

    private let scanner: GraphEdgeScanner
    private let snapshot: GraphReadSnapshot
    private let source: GraphIdentity
    private let edgeLabel: GraphIdentity?
    private let direction: GraphTraversalDirection
    private let configuration: GraphTraverserConfiguration

    package init(
        scanner: GraphEdgeScanner,
        snapshot: GraphReadSnapshot,
        source: GraphIdentity,
        edgeLabel: GraphIdentity?,
        direction: GraphTraversalDirection,
        configuration: GraphTraverserConfiguration
    ) {
        self.scanner = scanner
        self.snapshot = snapshot
        self.source = source
        self.edgeLabel = edgeLabel
        self.direction = direction
        self.configuration = configuration
    }

    package func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            scanner: scanner,
            snapshot: snapshot,
            source: source,
            edgeLabel: edgeLabel,
            direction: direction,
            configuration: configuration
        )
    }

    package struct AsyncIterator: AsyncIteratorProtocol {
        private let scanner: GraphEdgeScanner
        private let snapshot: GraphReadSnapshot
        private let edgeLabel: GraphIdentity?
        private let direction: GraphTraversalDirection
        private let configuration: GraphTraverserConfiguration

        private var visited: Set<GraphIdentity>
        private var currentLevel: [GraphIdentity]
        private var nextLevel: [GraphIdentity] = []
        private var currentDepth = 0
        private var sourceIndex = 0
        private var activeEdges: GraphNeighborSequence.AsyncIterator?
        private var hasEmittedSource = false
        private var isFinished = false

        fileprivate init(
            scanner: GraphEdgeScanner,
            snapshot: GraphReadSnapshot,
            source: GraphIdentity,
            edgeLabel: GraphIdentity?,
            direction: GraphTraversalDirection,
            configuration: GraphTraverserConfiguration
        ) {
            self.scanner = scanner
            self.snapshot = snapshot
            self.edgeLabel = edgeLabel
            self.direction = direction
            self.configuration = configuration
            self.visited = [source]
            self.currentLevel = [source]
        }

        package mutating func next() async throws -> GraphTraversalStep? {
            guard !isFinished else { return nil }
            try Task.checkCancellation()

            if !hasEmittedSource {
                hasEmittedSource = true
                return GraphTraversalStep(depth: 0, node: currentLevel[0])
            }

            while currentDepth < configuration.maximumDepth {
                if var iterator = activeEdges {
                    activeEdges = nil
                    if let edge = try await iterator.next() {
                        activeEdges = iterator
                        let neighbor = direction == .outgoing
                            ? edge.target
                            : edge.source
                        guard !visited.contains(neighbor) else { continue }
                        guard visited.count < configuration.maximumNodes else {
                            throw GraphTraversalError.maximumNodesReached(
                                explored: visited.count,
                                limit: configuration.maximumNodes
                            )
                        }
                        visited.insert(neighbor)
                        nextLevel.append(neighbor)
                        return GraphTraversalStep(
                            depth: currentDepth + 1,
                            node: neighbor
                        )
                    }
                    sourceIndex += 1
                    continue
                }

                if sourceIndex < currentLevel.count {
                    let source = currentLevel[sourceIndex]
                    let edges = direction == .outgoing
                        ? scanner.scanOutgoing(
                            from: source,
                            edgeLabel: edgeLabel,
                            transaction: snapshot.transaction
                        )
                        : scanner.scanIncoming(
                            to: source,
                            edgeLabel: edgeLabel,
                            transaction: snapshot.transaction
                        )
                    activeEdges = GraphNeighborSequence(
                        edges: edges,
                        workBudget: snapshot.workBudget
                    ).makeAsyncIterator()
                    continue
                }

                currentDepth += 1
                guard currentDepth < configuration.maximumDepth,
                      !nextLevel.isEmpty else {
                    isFinished = true
                    return nil
                }
                currentLevel = nextLevel.sorted()
                nextLevel.removeAll(keepingCapacity: true)
                sourceIndex = 0
            }

            isFinished = true
            return nil
        }
    }
}
