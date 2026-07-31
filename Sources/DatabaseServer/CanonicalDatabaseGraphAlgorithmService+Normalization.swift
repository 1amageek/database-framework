#if DATABASE_SERVER_GRAPH_INDEXES
import GraphIndex

extension CanonicalDatabaseGraphAlgorithmService {
    func canonicalCycles(
        _ values: [[GraphIdentity]]
    ) -> [[GraphIdentity]] {
        let normalized = values.compactMap { cycle -> [GraphIdentity]? in
            var vertices = cycle
            if vertices.count > 1, vertices.first == vertices.last {
                vertices.removeLast()
            }
            guard !vertices.isEmpty else { return nil }

            var best = vertices
            for offset in 1..<vertices.count {
                let rotated = Array(vertices[offset...]) + Array(vertices[..<offset])
                if rotated.lexicographicallyPrecedes(best) {
                    best = rotated
                }
            }
            return best + [best[0]]
        }
        return Array(Set(normalized)).sorted {
            $0.lexicographicallyPrecedes($1)
        }
    }

    func canonicalComponents(
        _ values: [[GraphIdentity]]
    ) -> [[GraphIdentity]] {
        values
            .map { $0.sorted() }
            .sorted { $0.lexicographicallyPrecedes($1) }
    }
}
#endif
