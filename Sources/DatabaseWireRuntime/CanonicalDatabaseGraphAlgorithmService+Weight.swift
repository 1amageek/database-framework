#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseTypes
import DatabaseMath
import GraphIndex

actor DatabaseIndexedWeightedGraphNeighborSource: WeightedGraphNeighborSource {
    private struct Cache: Sendable {
        let edgeLabel: GraphIdentity?
        let neighbors: [GraphIdentity: [WeightedGraphNeighbor]]
    }

    let scanner: GraphPropertyScanner
    let graphTarget: GraphScanTarget
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
        self.graphTarget = source.graphTarget
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
        var cursor = scanner.scanEdges(
            from: nil,
            edge: edgeLabel,
            to: nil,
            graphTarget: graphTarget,
            propertyFilters: nil,
            transaction: snapshot.transaction
        ).makeCursor()
        while let candidate = try await cursor.next() {
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

    private func numericWeight(_ value: FieldValue) -> Double? {
        switch value {
        case .int8(let value):
            return Double(value)
        case .int16(let value):
            return Double(value)
        case .int32(let value):
            return Double(value)
        case .int64(let value):
            return Double(value)
        case .uint8(let value):
            return Double(value)
        case .uint16(let value):
            return Double(value)
        case .uint32(let value):
            return Double(value)
        case .uint64(let value):
            return Double(value)
        case .float32(let value):
            return Double(value)
        case .float64(let value):
            return value
        case .decimal(let value):
            return Double(value.coefficient)
                * DatabaseMath.power(10, -Double(value.scale))
        case .null, .bool, .string, .bytes, .date, .time, .dateTime,
             .timestamp, .timeSpan, .calendarPeriod, .geographicPoint,
             .geographicPosition, .vector, .uuid, .array, .object,
             .reference, .rdfTerm:
            return nil
        }
    }
}
#endif
