import DatabaseValue
import GraphIndex

actor DatabaseIndexedWeightedGraphNeighborSource: WeightedGraphNeighborSource {
    private struct Cache: Sendable {
        let edgeLabel: GraphIdentity?
        let neighbors: [GraphIdentity: [WeightedGraphNeighbor]]
    }

    let scanner: GraphPropertyScanner
    let scope: GraphScanScope
    let snapshot: GraphReadSnapshot
    let property: String
    let workBudget: GraphAlgorithmWorkBudget
    private var cache: Cache?

    init(
        property: String,
        source: ResolvedDatabaseGraphSource,
        snapshot: GraphReadSnapshot,
        workBudget: GraphAlgorithmWorkBudget
    ) {
        self.scanner = GraphPropertyScanner(
            indexSubspace: source.indexSubspace,
            strategy: source.strategy,
            storedFieldNames: [property],
            snapshot: snapshot
        )
        self.scope = source.scope
        self.snapshot = snapshot
        self.property = property
        self.workBudget = workBudget
    }

    func neighbors(
        from source: GraphIdentity,
        edgeLabel: GraphIdentity?
    ) async throws -> [WeightedGraphNeighbor] {
        if let cache, cache.edgeLabel == edgeLabel {
            return cache.neighbors[source] ?? []
        }

        var allNeighbors: [GraphIdentity: [WeightedGraphNeighbor]] = [:]
        for try await candidate in scanner.scanEdges(
            from: nil,
            edge: edgeLabel,
            to: nil,
            scope: scope,
            propertyFilters: nil,
            transaction: snapshot.transaction
        ) {
            guard try workBudget.consume() else { return [] }
            guard let value = candidate.properties[property] else {
                throw DatabaseGraphAlgorithmError.edgeWeightMissing(
                    property: property
                )
            }
            guard let weight = numericWeight(value), weight.isFinite else {
                throw DatabaseGraphAlgorithmError.invalidEdgeWeight(
                    property: property,
                    value: value
                )
            }
            allNeighbors[candidate.source, default: []].append(
                WeightedGraphNeighbor(
                    edge: EdgeInfo(
                        source: candidate.source,
                        target: candidate.target,
                        edgeLabel: candidate.edgeLabel,
                        graph: candidate.graph
                    ),
                    weight: weight
                )
            )
        }
        if workBudget.limitReason != nil {
            return []
        }
        cache = Cache(edgeLabel: edgeLabel, neighbors: allNeighbors)
        return allNeighbors[source] ?? []
    }

    private func numericWeight(_ value: DatabaseValue) -> Double? {
        value.numericDoubleValue
    }
}
