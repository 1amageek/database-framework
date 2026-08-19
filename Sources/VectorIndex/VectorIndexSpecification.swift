import DatabaseEngine
import DatabaseKit

struct VectorIndexSpecification: Sendable {
    let dimensions: Int
    let metric: VectorMetric

    init(
        _ definition: IndexDefinition<FieldIdentity>
    ) throws(IndexMaintainerProviderError) {
        guard case .vector(_, let dimensions, let metric) = definition else {
            throw .typeMismatch(
                registered: .vector,
                actual: definition.type
            )
        }
        self.dimensions = dimensions
        self.metric = metric
    }
}
