/// Pull-based edge sequence bound to one caller-owned graph snapshot.
///
/// The sequence creates no producer task and has no intermediate buffer. If a
/// consumer abandons iteration, no subsequent backend cursor is advanced.
package struct GraphNeighborSequence: AsyncSequence, Sendable {
    package typealias Element = EdgeInfo

    private let edges: GraphEdgeSequence
    private let workBudget: GraphAlgorithmWorkBudget?

    package init(
        edges: GraphEdgeSequence,
        workBudget: GraphAlgorithmWorkBudget?
    ) {
        self.edges = edges
        self.workBudget = workBudget
    }

    package func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            iterator: edges.makeAsyncIterator(),
            workBudget: workBudget
        )
    }

    package struct AsyncIterator: AsyncIteratorProtocol {
        private var iterator: GraphEdgeSequence.AsyncIterator
        private let workBudget: GraphAlgorithmWorkBudget?
        private var isFinished = false

        fileprivate init(
            iterator: GraphEdgeSequence.AsyncIterator,
            workBudget: GraphAlgorithmWorkBudget?
        ) {
            self.iterator = iterator
            self.workBudget = workBudget
        }

        package mutating func next() async throws -> EdgeInfo? {
            guard !isFinished else { return nil }
            try Task.checkCancellation()
            if let edge = try await iterator.next() {
                return edge
            }
            isFinished = true
            if let reason = workBudget?.limitReason {
                throw GraphTraversalError.incomplete(reason)
            }
            return nil
        }
    }
}
