import DatabaseKit

func rankIndexMetadata(scoreType: IndexScalarType) -> IndexKindMetadata {
    IndexKindMetadata(
        identifier: "rank",
        subspaceStructure: .hierarchical,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(name: "score", number: 3)
            )
        ],
        metadata: [
            "scoreType": .string(scoreType.rawValue),
        ]
    )
}
