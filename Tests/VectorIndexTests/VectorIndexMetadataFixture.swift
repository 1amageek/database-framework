import DatabaseKit
import DatabaseTypes

func vectorIndexDefinition(
    fieldName: String = "embedding",
    fieldNumber: Int = 3,
    dimensions: Int,
    metric: VectorMetric
) -> IndexDefinition<FieldIdentity> {
    .vector(
        embedding: FieldIdentity(name: fieldName, number: fieldNumber),
        dimensions: dimensions,
        metric: metric
    )
}
