import DatabaseKit
import DatabaseTypes

func vectorIndexMetadata(
    fieldName: String = "embedding",
    fieldNumber: Int = 3,
    dimensions: Int,
    metric: VectorMetric
) -> IndexKindMetadata {
    let definition = IndexDefinition.vector(
        dimensions: dimensions,
        metric: metric
    )
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(name: fieldName, number: fieldNumber)
            )
        ],
        metadata: [
            "dimensions": .int64(Int64(dimensions)),
            "metric": .string(metric.rawValue),
        ]
    )
}
