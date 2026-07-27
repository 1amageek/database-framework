import DatabaseKit

struct VectorIndexSpecification: Sendable {
    static let identifier = "vector"

    let metadata: IndexKindMetadata
    let dimensions: Int
    let metric: VectorMetric

    init(
        _ metadata: IndexKindMetadata
    ) throws(IndexKindMetadataError) {
        try metadata.validateIdentity(
            identifier: Self.identifier,
            subspaceStructure: .hierarchical
        )
        try metadata.validateMetadataKeys(
            required: ["dimensions", "metric"]
        )
        try metadata.validateFieldCount(1)

        let dimensions = try metadata.requireInt("dimensions")
        guard dimensions > 0 else {
            throw .invalidMetadata(
                identifier: Self.identifier,
                key: "dimensions"
            )
        }
        let metricValue = try metadata.requireString("metric")
        guard let metric = VectorMetric(rawValue: metricValue) else {
            throw .invalidMetadata(
                identifier: Self.identifier,
                key: "metric"
            )
        }

        self.metadata = metadata
        self.dimensions = dimensions
        self.metric = metric
    }
}
