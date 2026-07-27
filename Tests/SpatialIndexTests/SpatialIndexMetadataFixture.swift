import DatabaseKit
@testable import SpatialIndex

func spatialIndexMetadata(
    fieldName: String,
    fieldNumber: Int,
    encoding: SpatialEncoding,
    level: Int
) -> IndexKindMetadata {
    IndexKindMetadata(
        identifier: "spatial",
        subspaceStructure: .flat,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(name: fieldName, number: fieldNumber)
            )
        ],
        metadata: [
            "encoding": .string(encoding.rawValue),
            "level": .int64(Int64(level)),
        ]
    )
}
