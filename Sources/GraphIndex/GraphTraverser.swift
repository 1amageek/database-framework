import DatabaseKit
import DatabaseEngine
import StorageKit

/// Creates pull-based graph reads over one caller-owned transaction snapshot.
///
/// A traverser must be created and fully consumed inside the transaction
/// attempt that owns `snapshot`. It never opens nested transactions and never
/// publishes values through a producer task.
package struct GraphTraverser: Sendable {
    private let snapshot: GraphReadSnapshot
    private let scanner: GraphEdgeScanner
    private let configuration: GraphTraverserConfiguration

    package init(
        snapshot: GraphReadSnapshot,
        subspace: Subspace,
        strategy: GraphIndexStrategy = .adjacency,
        scope: GraphScanScope = .all,
        configuration: GraphTraverserConfiguration = .default
    ) throws {
        guard configuration.maximumDepth >= 0 else {
            throw GraphTraversalError.invalidMaximumDepth(
                configuration.maximumDepth
            )
        }
        guard configuration.maximumNodes > 0 else {
            throw GraphTraversalError.invalidMaximumNodes(
                configuration.maximumNodes
            )
        }
        self.snapshot = snapshot
        self.scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: strategy,
            scope: scope,
            snapshot: snapshot
        )
        self.configuration = configuration
    }

    package func neighbors(
        from node: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        direction: GraphTraversalDirection = .outgoing
    ) -> GraphNeighborSequence {
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
        return GraphNeighborSequence(
            edges: edges,
            workBudget: snapshot.workBudget
        )
    }

    package func traverse(
        from node: GraphIdentity,
        edgeLabel: GraphIdentity? = nil,
        direction: GraphTraversalDirection = .outgoing
    ) -> GraphTraversalSequence {
        GraphTraversalSequence(
            scanner: scanner,
            snapshot: snapshot,
            source: node,
            edgeLabel: edgeLabel,
            direction: direction,
            configuration: configuration
        )
    }
}
