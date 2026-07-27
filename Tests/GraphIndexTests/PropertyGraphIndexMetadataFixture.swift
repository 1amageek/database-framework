import DatabaseKit
import DatabaseTypes

func propertyGraphIndexMetadata(
    sourceFieldName: String,
    labelFieldName: String,
    targetFieldName: String,
    namespaceFieldName: String? = nil,
    strategy: PropertyGraphIndexStrategy
) -> IndexKindMetadata {
    var fieldNames = [
        sourceFieldName,
        labelFieldName,
        targetFieldName,
    ]
    if let namespaceFieldName {
        fieldNames.append(namespaceFieldName)
    }
    return IndexKindMetadata(
        identifier: IndexDefinition.propertyGraph().identifier,
        subspaceStructure: .hierarchical,
        fields: fieldNames.enumerated().map { number, name in
            IndexFieldMetadata(
                identity: FieldIdentity(
                    name: name,
                    number: number
                )
            )
        },
        metadata: [
            "strategy": .string(strategy.rawValue),
            "hasEdgeField": .bool(true),
            "hasGraphField": .bool(namespaceFieldName != nil),
        ]
    )
}
