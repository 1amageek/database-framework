import DatabaseKit

func rankIndexDefinition(
    fieldNumber: Int
) -> IndexDefinition<FieldIdentity> {
    .rank(
        score: FieldIdentity(name: "score", number: fieldNumber)
    )
}
