import DatabaseKit

func bitmapIndexMetadata(
    fieldName: String,
    fieldNumber: Int
) -> IndexKindMetadata {
    let definition = IndexDefinition.bitmap
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(
                    name: fieldName,
                    number: fieldNumber
                )
            )
        ],
        metadata: [:]
    )
}
