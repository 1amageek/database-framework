import DatabaseKit
import DatabaseTypes

func countIndexMetadata(
    groupingFields: [FieldIdentity]
) -> IndexKindMetadata {
    let definition = IndexDefinition.count
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: groupingFields.map { IndexFieldMetadata(identity: $0) },
        metadata: [:]
    )
}

func numericAggregationIndexMetadata(
    _ definition: IndexDefinition,
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity,
    valueType: IndexScalarType
) -> IndexKindMetadata {
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: (groupingFields + [valueField]).map {
            IndexFieldMetadata(identity: $0)
        },
        metadata: [
            "valueType": .string(valueType.rawValue)
        ]
    )
}

func countNotNullIndexMetadata(
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity
) -> IndexKindMetadata {
    let definition = IndexDefinition.countNotNull
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: (groupingFields + [valueField]).map {
            IndexFieldMetadata(identity: $0)
        },
        metadata: [:]
    )
}

func distinctIndexMetadata(
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity,
    precision: Int
) -> IndexKindMetadata {
    let definition = IndexDefinition.distinct(precision: precision)
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: (groupingFields + [valueField]).map {
            IndexFieldMetadata(identity: $0)
        },
        metadata: [
            "precision": .int64(Int64(precision))
        ]
    )
}

func percentileIndexMetadata(
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity,
    compression: Double
) -> IndexKindMetadata {
    let definition = IndexDefinition.percentile(compression: compression)
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: (groupingFields + [valueField]).map {
            IndexFieldMetadata(identity: $0)
        },
        metadata: [
            "compression": .float64(compression)
        ]
    )
}
