import DatabaseKit

func rankIndexMetadata(
    scoreType: IndexScalarType,
    bucketSize: Int = 100
) -> IndexKindMetadata {
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
            "bucketSize": .int64(Int64(bucketSize)),
        ]
    )
}
