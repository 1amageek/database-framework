import DatabaseKit
@testable import SpatialIndex

func spatialIndexDefinition(
    fieldName: String,
    fieldNumber: Int,
    encoding: SpatialEncoding,
    level: Int
) -> IndexDefinition<FieldIdentity> {
    .spatial(
        location: FieldIdentity(name: fieldName, number: fieldNumber),
        encoding: encoding,
        level: level
    )
}
