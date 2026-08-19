import DatabaseKit

func bitmapIndexDefinition(
    fieldName: String,
    fieldNumber: Int
) -> IndexDefinition<FieldIdentity> {
    .bitmap(
        field: FieldIdentity(
                    name: fieldName,
                    number: fieldNumber)
    )
}
