import DatabaseValue
import DatabaseWire

extension CanonicalDatabaseGraphAlgorithmService {
    func page(
        _ response: GraphAlgorithmOperation.Response,
        offset: UInt64,
        limit: UInt32,
        requestFingerprint: DatabaseBytes,
        resultFingerprint: DatabaseBytes
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
            let edgeCount = max(0, result.nodes.count - 1)
            guard edgeCount == 0 ? offset == 0 : offset < edgeCount else {
                throw DatabaseGraphAlgorithmError.invalidContinuation
            }
            let upperEdge = offset + min(edgeCount - offset, limit)
            let nextOffset = upperEdge < edgeCount ? upperEdge : nil
            let nodes = Array(result.nodes[offset...upperEdge])
            let edgeLabels = result.edgeLabels.isEmpty
                ? []
                : Array(result.edgeLabels[offset..<upperEdge])
            let weights = result.weights.isEmpty
                ? []
                : Array(result.weights[offset..<upperEdge])
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
            let slice = try pageSlice(result.scores, offset: offset, limit: limit)
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
            let slice = try pageSlice(result.assignments, offset: offset, limit: limit)
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
            let totalCount = result.cycles.count + result.backEdges.count
            try validateOffset(offset, count: totalCount)
            let upper = offset + min(totalCount - offset, limit)
            let cycleUpper = min(result.cycles.count, upper)
            let cycles = offset < cycleUpper
                ? Array(result.cycles[offset..<cycleUpper])
                : []
            let edgeLower = max(offset, result.cycles.count) - result.cycles.count
            let edgeUpper = max(upper, result.cycles.count) - result.cycles.count
            let backEdges = edgeLower < edgeUpper
                ? Array(result.backEdges[edgeLower..<edgeUpper])
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
            let slice = try pageSlice(result.components, offset: offset, limit: limit)
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
            if let order = result.order {
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
            let slice = try pageSlice(result.cyclicNodes, offset: offset, limit: limit)
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
        requestFingerprint: DatabaseBytes,
        resultFingerprint: DatabaseBytes
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
