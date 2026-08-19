import DatabaseKit

func scalarIndexDefinition(
    fields: [FieldIdentity],
    unique: Bool = false
) -> IndexDefinition<FieldIdentity> {
    .ordered(
        keys: fields.map(IndexKey.ascending),
        includedFields: [],
        unique: unique
    )
}
