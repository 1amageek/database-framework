import DatabaseEngine
import DatabaseValue
import DatabaseWire
import GraphIndex

extension CanonicalDatabaseGraphAlgorithmService {
    func executeFull(
        _ invocation: GraphAlgorithmOperation.Invocation,
        source: ResolvedDatabaseGraphSource,
        snapshot: GraphReadSnapshot,
        workBudget: GraphAlgorithmWorkBudget,
        requestFingerprint: DatabaseBytes
    ) async throws -> GraphAlgorithmOperation.Response {
        switch invocation {
        case .shortestPath(
            let sourceTerm,
            let targetTerm,
            let maximumDepth,
            let bidirectional,
            let maximumNodes
        ):
            let maximumDepth = try positiveInt(maximumDepth, field: "maximumDepth")
            let maximumNodes = try positiveInt(maximumNodes, field: "maximumNodes")
            let finder = ShortestPathFinder(
                snapshot: snapshot,
                subspace: source.indexSubspace,
                strategy: source.strategy,
                scope: source.scope,
                configuration: ShortestPathConfiguration(
                    maxDepth: maximumDepth,
                    bidirectional: bidirectional,
                    maxNodesExplored: maximumNodes
                )
            )
            let result = try await finder.findShortestPath(
                from: source.encodeVertex(sourceTerm),
                to: source.encodeVertex(targetTerm),
                edgeLabel: source.edgeLabel
            )
            if result.path != nil, !result.isComplete {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "a found shortest path must be definitive"
                )
            }
            return .path(
                try pathResult(
                    path: result.path,
                    totalWeight: result.distance,
                    nodesExplored: result.nodesExplored,
                    durationNanoseconds: result.durationNs,
                    isComplete: result.isComplete,
                    limitReason: result.limitReason,
                    source: source
                )
            )

        case .weightedShortestPath(
            let sourceTerm,
            let targetTerm,
            let weightProperty,
            let maximumWeight,
            let maximumNodes
        ):
            guard !maximumWeight.isNaN, maximumWeight >= 0 else {
                throw DatabaseGraphAlgorithmError.invalidInvocation(
                    "maximumWeight must be non-negative and not NaN"
                )
            }
            guard source.storedFieldNames.contains(weightProperty) else {
                throw DatabaseGraphAlgorithmError.weightPropertyNotStored(
                    index: source.indexName,
                    property: weightProperty
                )
            }
            let maximumNodes = try positiveInt(maximumNodes, field: "maximumNodes")
            let finder = WeightedShortestPathFinder(
                neighborSource: DatabaseIndexedWeightedGraphNeighborSource(
                    property: weightProperty,
                    source: source,
                    snapshot: snapshot,
                    workBudget: workBudget
                ),
                configuration: WeightedShortestPathConfiguration(
                    maxWeight: maximumWeight,
                    maxNodes: maximumNodes
                ),
                workBudget: workBudget
            )
            let result = try await finder.findShortestPath(
                from: source.encodeVertex(sourceTerm),
                to: source.encodeVertex(targetTerm),
                edgeLabel: source.edgeLabel
            )
            if result.path != nil, !result.isComplete {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "a found weighted path must be definitive"
                )
            }
            return .path(
                try pathResult(
                    path: result.path,
                    totalWeight: result.path == nil ? nil : result.totalWeight,
                    nodesExplored: result.nodesExplored,
                    durationNanoseconds: result.durationNs,
                    isComplete: result.isComplete,
                    limitReason: result.limitReason,
                    source: source
                )
            )

        case .pageRank(
            let dampingFactor,
            let maximumIterations,
            let convergenceThreshold,
            let personalizedSource
        ):
            guard dampingFactor.isFinite,
                  (0...1).contains(dampingFactor) else {
                throw DatabaseGraphAlgorithmError.invalidInvocation(
                    "dampingFactor must be finite and inside 0...1"
                )
            }
            guard convergenceThreshold.isFinite,
                  convergenceThreshold >= 0 else {
                throw DatabaseGraphAlgorithmError.invalidInvocation(
                    "convergenceThreshold must be finite and non-negative"
                )
            }
            let maximumIterations = try positiveInt(
                maximumIterations,
                field: "maximumIterations"
            )
            let computer = PageRankComputer(
                snapshot: snapshot,
                subspace: source.indexSubspace,
                strategy: source.strategy,
                scope: source.scope,
                configuration: PageRankConfiguration(
                    dampingFactor: dampingFactor,
                    maxIterations: maximumIterations,
                    convergenceThreshold: convergenceThreshold
                )
            )
            let result: PageRankResult
            if let personalizedSource {
                result = try await computer.computePersonalized(
                    from: source.encodeVertex(personalizedSource),
                    edgeLabel: source.edgeLabel
                )
            } else {
                result = try await computer.compute(edgeLabel: source.edgeLabel)
            }
            let scores = try result.scores.map { vertex, score in
                guard score.isFinite else {
                    throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                        "PageRank produced a non-finite score"
                    )
                }
                return (encodedVertex: vertex, score: score)
            }
            .sorted { lhs, rhs in
                if lhs.score != rhs.score { return lhs.score > rhs.score }
                return lhs.encodedVertex < rhs.encodedVertex
            }
            .map {
                GraphAlgorithmOperation.Score(
                    vertex: try source.decodeVertex($0.encodedVertex),
                    score: $0.score
                )
            }
            return .ranking(
                GraphAlgorithmOperation.RankingPage(
                    scores: scores,
                    iterations: try unsigned32(result.iterations, field: "iterations"),
                    convergenceDelta: result.convergenceDelta,
                    progress: try progress(
                        isComplete: result.isComplete,
                        limitReason: result.limitReason
                    )
                )
            )

        case .community(
            let maximumIterations,
            let computeModularity,
            let minimumCommunitySize,
            let seed
        ):
            let maximumIterations = try positiveInt(
                maximumIterations,
                field: "maximumIterations"
            )
            let minimumCommunitySize = try positiveInt(
                minimumCommunitySize,
                field: "minimumCommunitySize"
            )
            let detector = CommunityDetector(
                snapshot: snapshot,
                subspace: source.indexSubspace,
                strategy: source.strategy,
                scope: source.scope,
                configuration: CommunityDetectionConfiguration(
                    maxIterations: maximumIterations,
                    computeModularity: computeModularity,
                    minCommunitySize: minimumCommunitySize,
                    seed: seed ?? deterministicSeed(from: requestFingerprint)
                )
            )
            let result = try await detector.detect(edgeLabel: source.edgeLabel)
            if let modularity = result.modularity, !modularity.isFinite {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "community detection produced non-finite modularity"
                )
            }
            let assignments = try result.assignments
                .sorted { $0.key < $1.key }
                .map { vertex, community in
                    GraphAlgorithmOperation.CommunityAssignment(
                        vertex: try source.decodeVertex(vertex),
                        community: try source.decodeVertex(community)
                    )
                }
            return .communities(
                GraphAlgorithmOperation.CommunityPage(
                    assignments: assignments,
                    iterations: try unsigned32(result.iterations, field: "iterations"),
                    modularity: result.modularity,
                    progress: try progress(
                        isComplete: result.isComplete,
                        limitReason: result.limitReason
                    )
                )
            )

        case .cycleDetection(let maximumCycles, let maximumNodes):
            let maximumCycles = try positiveInt(maximumCycles, field: "maximumCycles")
            let maximumNodes = try positiveInt(maximumNodes, field: "maximumNodes")
            let detector = CycleDetector(
                snapshot: snapshot,
                subspace: source.indexSubspace,
                strategy: source.strategy,
                scope: source.scope,
                configuration: CycleDetectorConfiguration(
                    maxCycles: maximumCycles,
                    maxNodes: maximumNodes
                )
            )
            let result = try await detector.findCycles(edgeLabel: source.edgeLabel)
            let cycles = try canonicalCycles(result.cycles).map { cycle in
                try cycle.map(source.decodeVertex)
            }
            let backEdges = try result.backEdges
                .sorted {
                    if $0.from != $1.from { return $0.from < $1.from }
                    return $0.to < $1.to
                }
                .map {
                    GraphAlgorithmOperation.DirectedEdge(
                        source: try source.decodeVertex($0.from),
                        target: try source.decodeVertex($0.to)
                    )
                }
            return .cycles(
                GraphAlgorithmOperation.CyclePage(
                    cycles: cycles,
                    backEdges: backEdges,
                    nodesExplored: try unsigned(result.nodesExplored, field: "nodesExplored"),
                    progress: try progress(
                        isComplete: result.isComplete,
                        limitReason: result.limitReason
                    )
                )
            )

        case .stronglyConnectedComponents(
            let maximumComponents,
            let maximumNodes
        ):
            let maximumComponents = try positiveInt(
                maximumComponents,
                field: "maximumComponents"
            )
            let maximumNodes = try positiveInt(maximumNodes, field: "maximumNodes")
            let scanner = GraphEdgeScanner(
                indexSubspace: source.indexSubspace,
                strategy: source.strategy,
                scope: source.scope,
                snapshot: snapshot
            )
            let finder = SCCFinder(
                snapshot: snapshot,
                scanner: scanner,
                configuration: SCCConfiguration(
                    maxComponents: maximumComponents,
                    maxNodes: maximumNodes
                )
            )
            let result = try await finder.findSCCs(edgeLabel: source.edgeLabel)
            let components = try canonicalComponents(result.components).map { component in
                try component.map(source.decodeVertex)
            }
            return .components(
                GraphAlgorithmOperation.ComponentPage(
                    components: components,
                    nodesExplored: try unsigned(result.nodesExplored, field: "nodesExplored"),
                    progress: try progress(
                        isComplete: result.isComplete,
                        limitReason: result.limitReason
                    )
                )
            )

        case .topologicalSort(let maximumNodes):
            let maximumNodes = try positiveInt(maximumNodes, field: "maximumNodes")
            let sorter = TopologicalSorter(
                snapshot: snapshot,
                subspace: source.indexSubspace,
                strategy: source.strategy,
                scope: source.scope,
                configuration: TopologicalSortConfiguration(maxNodes: maximumNodes)
            )
            let result = try await sorter.sort(edgeLabel: source.edgeLabel)
            let order = try result.isComplete
                ? result.order?.map(source.decodeVertex)
                : nil
            let cyclicNodes = try result.cyclicNodes
                .sorted()
                .map(source.decodeVertex)
            return .topologicalOrder(
                GraphAlgorithmOperation.TopologicalResult(
                    order: order,
                    cyclicNodes: cyclicNodes,
                    totalNodes: try unsigned(result.totalNodes, field: "totalNodes"),
                    progress: try progress(
                        isComplete: result.isComplete,
                        limitReason: result.limitReason
                    )
                )
            )
        }
    }

    private func pathResult(
        path: GraphPath?,
        totalWeight: Double?,
        nodesExplored: Int,
        durationNanoseconds: UInt64,
        isComplete: Bool,
        limitReason: DatabaseEngine.LimitReason?,
        source: ResolvedDatabaseGraphSource
    ) throws -> GraphAlgorithmOperation.PathResult {
        if let totalWeight, !totalWeight.isFinite {
            throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                "a found path has non-finite total weight"
            )
        }
        return GraphAlgorithmOperation.PathResult(
            found: path != nil,
            nodes: try path?.nodeIDs.map(source.decodeVertex) ?? [],
            edgeLabels: try path?.edgeLabels.map(source.decodeEdgeLabel) ?? [],
            weights: path?.weights ?? [],
            totalWeight: totalWeight,
            nodesExplored: try unsigned(nodesExplored, field: "nodesExplored"),
            durationNanoseconds: durationNanoseconds,
            progress: try progress(
                isComplete: isComplete,
                limitReason: limitReason
            )
        )
    }
}
