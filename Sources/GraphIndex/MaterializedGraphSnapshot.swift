import DatabaseEngine

/// Immutable adjacency representation materialized from one storage snapshot.
///
/// Graph identities retain their canonical backing buffers. Building the maps
/// copies only small identity values and array storage; RDF payload bytes are
/// never re-materialized.
package struct MaterializedGraphSnapshot: Sendable {
    package let nodes: Set<GraphIdentity>
    package let edges: [EdgeInfo]
    package let outgoing: [GraphIdentity: [EdgeInfo]]
    package let incoming: [GraphIdentity: [EdgeInfo]]

    package func outgoingNeighbors(of node: GraphIdentity) -> [EdgeInfo] {
        outgoing[node] ?? []
    }

    package func incomingNeighbors(of node: GraphIdentity) -> [EdgeInfo] {
        incoming[node] ?? []
    }
}

package struct MaterializedGraphLoad: Sendable {
    package let graph: MaterializedGraphSnapshot
    package let limitReason: LimitReason?
}

package enum MaterializedGraphSnapshotBuilder {
    package static func load(
        scanner: GraphEdgeScanner,
        edgeLabel: GraphIdentity?,
        snapshot: GraphReadSnapshot,
        maximumNodes: Int? = nil
    ) async throws -> MaterializedGraphLoad {
        var nodes: Set<GraphIdentity> = []
        var edges: [EdgeInfo] = []
        var outgoing: [GraphIdentity: [EdgeInfo]] = [:]
        var incoming: [GraphIdentity: [EdgeInfo]] = [:]

        let edgeSequence = scanner.scanAllEdges(
            edgeLabel: edgeLabel,
            transaction: snapshot.transaction
        )
        var edgeCursor = edgeSequence.makeCursor()
        while let edge = try await edgeCursor.next() {
            if let workBudget = snapshot.workBudget,
               try !workBudget.consume() {
                return result(
                    nodes: nodes,
                    edges: edges,
                    outgoing: outgoing,
                    incoming: incoming,
                    limitReason: workBudget.limitReason
                )
            }

            if let maximumNodes {
                var missingNodeCount = nodes.contains(edge.source) ? 0 : 1
                if edge.target != edge.source, !nodes.contains(edge.target) {
                    missingNodeCount += 1
                }
                guard nodes.count + missingNodeCount <= maximumNodes else {
                    return result(
                        nodes: nodes,
                        edges: edges,
                        outgoing: outgoing,
                        incoming: incoming,
                        limitReason: .maxNodesReached(
                            explored: nodes.count,
                            limit: maximumNodes
                        )
                    )
                }
            }

            nodes.insert(edge.source)
            nodes.insert(edge.target)
            edges.append(edge)
            outgoing[edge.source, default: []].append(edge)
            incoming[edge.target, default: []].append(edge)
        }

        return result(
            nodes: nodes,
            edges: edges,
            outgoing: outgoing,
            incoming: incoming,
            limitReason: snapshot.workBudget?.limitReason
        )
    }

    private static func result(
        nodes: Set<GraphIdentity>,
        edges: [EdgeInfo],
        outgoing: [GraphIdentity: [EdgeInfo]],
        incoming: [GraphIdentity: [EdgeInfo]],
        limitReason: LimitReason?
    ) -> MaterializedGraphLoad {
        MaterializedGraphLoad(
            graph: MaterializedGraphSnapshot(
                nodes: nodes,
                edges: edges,
                outgoing: outgoing,
                incoming: incoming
            ),
            limitReason: limitReason
        )
    }
}
