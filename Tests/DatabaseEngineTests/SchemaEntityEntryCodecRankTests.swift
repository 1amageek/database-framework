import DatabaseKit
import Testing
@testable import DatabaseEngine

@Suite("Schema entity canonical rank declaration")
struct SchemaEntityEntryCodecRankTests {
    @Test("Payload-free rank definitions round-trip through the schema catalog")
    func rankDefinitionRoundTrips() throws {
        let entity = try Schema.Entity(
            name: "RankedPlayer",
            identifierType: .string,
            fields: [
                FieldSchema(
                    name: "id",
                    fieldNumber: 1,
                    type: .string
                ),
                FieldSchema(
                    name: "score",
                    fieldNumber: 2,
                    type: .int64,
                    defaultValue: .int64(0)
                ),
            ],
            polymorphicMembership: PolymorphicMembership(
                identifier: "Rankable",
                directoryComponents: [.staticPath("rankable")],
                directoryLayer: .default,
                indexes: [
                    .rank(
                        name: "rankable_score",
                        score: "score"
                    )
                ]
            )
        )

        let encoded = try SchemaEntityEntryCodec.encode(entity)
        let decoded = try SchemaEntityEntryCodec.decode(encoded)

        #expect(decoded == entity)
        #expect(decoded.fields[0].defaultValue == nil)
        #expect(decoded.fields[1].defaultValue == .int64(0))
        #expect(
            decoded.polymorphicMembership?.indexes.first?.definition == .rank(score: "score")
        )
    }
}
