import DatabaseEngine

/// Pull-based edge sequence bound to one caller-owned graph snapshot.
///
/// The sequence creates no producer task and has no intermediate buffer. If a
/// consumer abandons iteration, no subsequent backend cursor is advanced.
package struct GraphNeighborScan: Sendable {
    package typealias Element = EdgeInfo

    private let edges: GraphEdgeScan
    private let workBudget: GraphAlgorithmWorkBudget?

    package init(
        edges: GraphEdgeScan,
        workBudget: GraphAlgorithmWorkBudget?
    ) {
        self.edges = edges
        self.workBudget = workBudget
    }

    package func makeCursor() -> Cursor {
        Cursor(
            edgeCursor: edges.makeCursor(),
            workBudget: workBudget
        )
    }

    package struct Cursor {
        private var edgeCursor: GraphEdgeScan.Cursor
        private let workBudget: GraphAlgorithmWorkBudget?
        private var isFinished = false

        fileprivate init(
            edgeCursor: GraphEdgeScan.Cursor,
            workBudget: GraphAlgorithmWorkBudget?
        ) {
            self.edgeCursor = edgeCursor
            self.workBudget = workBudget
        }

        package mutating func next() async throws -> EdgeInfo? {
            guard !isFinished else { return nil }
            try ensureDatabaseTaskIsActive()
            if let edge = try await edgeCursor.next() {
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
