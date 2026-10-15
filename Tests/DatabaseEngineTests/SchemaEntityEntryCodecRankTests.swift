import DatabaseKit
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Schema entity canonical rank declaration")
struct SchemaEntityEntryCodecRankTests {
    @Test("Unresolved custom field types fail schema and persisted catalog admission")
    func unresolvedCustomQueryTypesAreRejected() throws {
        for schema: String? in [nil, "private"] {
            #expect(throws: SchemaEntityError.invalidQueryField(
                fieldName: "id", cause: .invalidPersistenceDescriptor
            )) {
                try Schema.Entity(
                    name: "CustomField",
                    identifierType: .string,
                    fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string,
                        queryType: .sql(.custom("text", schema: schema)))],
                    directoryComponents: [.staticPath("custom")]
                )
            }
        }
        let encoded = try StorageFrameEncoder.encode { writer throws(StorageFrameError) in
            writer.writeUInt32(0x4353_4244)
            writer.writeUInt16(5)
            try writer.writeString("CustomField")
            writer.writeUInt8(9)
            try writer.writeCount(1)
            try writer.writeString("id")
            writer.writeInt64(1)
            try writer.writeString(FieldSchemaType.string.rawValue)
            writer.writeBool(false)
            writer.writeBool(false)
            try writer.writeOptionalString(nil)
            writer.writeBool(true)
            writer.writeUInt8(1)
            writer.writeUInt8(21)
            try writer.writeString("text")
        }
        do {
            _ = try SchemaEntityEntryCodec.decode(encoded)
            Issue.record("Expected the unresolved persisted query type to fail")
        } catch let error as SchemaEntityEntryCodecError {
            guard case .invalidDefinition(let reason) = error else { throw error }
            #expect(reason == "Invalid query field type")
        }
    }

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
