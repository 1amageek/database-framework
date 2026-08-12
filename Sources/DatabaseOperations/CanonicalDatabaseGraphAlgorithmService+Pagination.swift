import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

extension CanonicalDatabaseGraphAlgorithmService {
    func page(
        _ response: GraphAlgorithmOperation.Response,
        offset: UInt64,
        limit: UInt32,
        requestFingerprint: ByteString,
        resultFingerprint: ByteString
    ) throws -> GraphAlgorithmOperation.Response {
        guard limit > 0, let offset = Int(exactly: offset) else {
            throw DatabaseGraphAlgorithmError.invalidContinuation
        }
        let limit = Int(limit)

        switch response {
        case .path(let result):
            guard result.found else {
                guard offset == 0 else {
                    throw DatabaseGraphAlgorithmError.invalidContinuation
                }
                return response
            }
            let allNodes = try result.materializedNodes(
                maximumCount: result.nodeCount
            )
            let allEdgeLabels = try result.materializedEdgeLabels(
                maximumCount: result.edgeLabelCount
            )
            let allWeights = try result.materializedWeights(
                maximumCount: result.weightCount
            )
            let edgeCount = max(0, allNodes.count - 1)
            guard edgeCount == 0 ? offset == 0 : offset < edgeCount else {
                throw DatabaseGraphAlgorithmError.invalidContinuation
            }
            let upperEdge = offset + min(edgeCount - offset, limit)
            let nextOffset = upperEdge < edgeCount ? upperEdge : nil
            let nodes = Array(allNodes[offset...upperEdge])
            let edgeLabels = allEdgeLabels.isEmpty
                ? []
                : Array(allEdgeLabels[offset..<upperEdge])
            let weights = allWeights.isEmpty
                ? []
                : Array(allWeights[offset..<upperEdge])
            return .path(
                GraphAlgorithmOperation.PathResult(
                    found: true,
                    nodes: nodes,
                    edgeLabels: edgeLabels,
                    weights: weights,
                    totalWeight: result.totalWeight,
                    nodesExplored: result.nodesExplored,
                    durationNanoseconds: result.durationNanoseconds,
                    progress: try pageProgress(
                        result.progress,
                        nextOffset: nextOffset,
                        kind: .path,
                        requestFingerprint: requestFingerprint,
                        resultFingerprint: resultFingerprint
                    )
                )
            )

        case .ranking(let result):
            let scores = try result.materializedScores(
                maximumCount: result.scoreCount
            )
            let slice = try pageSlice(scores, offset: offset, limit: limit)
            return .ranking(
                GraphAlgorithmOperation.RankingPage(
                    scores: slice.values,
                    iterations: result.iterations,
                    convergenceDelta: result.convergenceDelta,
                    progress: try pageProgress(
                        result.progress,
                        nextOffset: slice.nextOffset,
                        kind: .ranking,
                        requestFingerprint: requestFingerprint,
                        resultFingerprint: resultFingerprint
                    )
                )
            )

        case .communities(let result):
            let assignments = try result.materializedAssignments(
                maximumCount: result.assignmentCount
            )
            let slice = try pageSlice(
                assignments,
                offset: offset,
                limit: limit
            )
            return .communities(
                GraphAlgorithmOperation.CommunityPage(
                    assignments: slice.values,
                    iterations: result.iterations,
                    modularity: result.modularity,
                    progress: try pageProgress(
                        result.progress,
                        nextOffset: slice.nextOffset,
                        kind: .communities,
                        requestFingerprint: requestFingerprint,
                        resultFingerprint: resultFingerprint
                    )
                )
            )

        case .cycles(let result):
            let allCycles = try result.materializedCycles(
                maximumCount: result.cycleCount
            )
            let allBackEdges = try result.materializedBackEdges(
                maximumCount: result.backEdgeCount
            )
            let totalCount = allCycles.count + allBackEdges.count
            try validateOffset(offset, count: totalCount)
            let upper = offset + min(totalCount - offset, limit)
            let cycleUpper = min(allCycles.count, upper)
            let cycles = offset < cycleUpper
                ? Array(allCycles[offset..<cycleUpper])
                : []
            let edgeLower = max(offset, allCycles.count) - allCycles.count
            let edgeUpper = max(upper, allCycles.count) - allCycles.count
            let backEdges = edgeLower < edgeUpper
                ? Array(allBackEdges[edgeLower..<edgeUpper])
                : []
            let nextOffset = upper < totalCount ? upper : nil
            return .cycles(
                GraphAlgorithmOperation.CyclePage(
                    cycles: cycles,
                    backEdges: backEdges,
                    nodesExplored: result.nodesExplored,
                    progress: try pageProgress(
                        result.progress,
                        nextOffset: nextOffset,
                        kind: .cycles,
                        requestFingerprint: requestFingerprint,
                        resultFingerprint: resultFingerprint
                    )
                )
            )

        case .components(let result):
            let components = try result.materializedComponents(
                maximumCount: result.componentCount
            )
            let slice = try pageSlice(
                components,
                offset: offset,
                limit: limit
            )
            return .components(
                GraphAlgorithmOperation.ComponentPage(
                    components: slice.values,
                    nodesExplored: result.nodesExplored,
                    progress: try pageProgress(
                        result.progress,
                        nextOffset: slice.nextOffset,
                        kind: .components,
                        requestFingerprint: requestFingerprint,
                        resultFingerprint: resultFingerprint
                    )
                )
            )

        case .topologicalOrder(let result):
            if let order = try result.materializedOrder(
                maximumCount: result.orderCount ?? 0
            ) {
                let slice = try pageSlice(order, offset: offset, limit: limit)
                return .topologicalOrder(
                    GraphAlgorithmOperation.TopologicalResult(
                        order: slice.values,
                        cyclicNodes: [],
                        totalNodes: result.totalNodes,
                        progress: try pageProgress(
                            result.progress,
                            nextOffset: slice.nextOffset,
                            kind: .topologicalOrder,
                            requestFingerprint: requestFingerprint,
                            resultFingerprint: resultFingerprint
                        )
                    )
                )
            }
            let cyclicNodes = try result.materializedCyclicNodes(
                maximumCount: result.cyclicNodeCount
            )
            let slice = try pageSlice(
                cyclicNodes,
                offset: offset,
                limit: limit
            )
            return .topologicalOrder(
                GraphAlgorithmOperation.TopologicalResult(
                    order: nil,
                    cyclicNodes: slice.values,
                    totalNodes: result.totalNodes,
                    progress: try pageProgress(
                        result.progress,
                        nextOffset: slice.nextOffset,
                        kind: .topologicalOrder,
                        requestFingerprint: requestFingerprint,
                        resultFingerprint: resultFingerprint
                    )
                )
            )
        }
    }

    private func pageProgress(
        _ progress: GraphAlgorithmOperation.Progress,
        nextOffset: Int?,
        kind: DatabaseGraphAlgorithmPageCursor.Kind,
        requestFingerprint: ByteString,
        resultFingerprint: ByteString
    ) throws -> GraphAlgorithmOperation.Progress {
        let continuation = try nextOffset.map {
            try DatabaseGraphAlgorithmPageCursor(
                kind: kind,
                requestFingerprint: requestFingerprint,
                resultFingerprint: resultFingerprint,
                offset: UInt64($0)
            ).encode(limits: wireLimits)
        }
        return GraphAlgorithmOperation.Progress(
            algorithmComplete: progress.algorithmComplete,
            resultPageComplete: continuation == nil,
            limitReason: progress.limitReason,
            continuation: continuation
        )
    }

    private func pageSlice<Element>(
        _ values: [Element],
        offset: Int,
        limit: Int
    ) throws -> (values: [Element], nextOffset: Int?) {
        try validateOffset(offset, count: values.count)
        let upper = offset + min(values.count - offset, limit)
        return (
            Array(values[offset..<upper]),
            upper < values.count ? upper : nil
        )
    }

    private func validateOffset(_ offset: Int, count: Int) throws {
        guard count == 0 ? offset == 0 : offset < count else {
            throw DatabaseGraphAlgorithmError.invalidContinuation
        }
    }
}
#endif
