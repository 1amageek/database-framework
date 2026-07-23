import StorageKit
import Testing
@testable import ScalarIndex

@Suite("Scalar filter index entry decoding")
struct ScalarFilterIndexEntryDecoderTests {
    @Test("Composite index fields are excluded from the primary key")
    func decodesCompositeIndexPrimaryKey() throws {
        let tuple = Tuple("category", Int64(42), "entity-id")

        let primaryKey = try ScalarFilterIndexEntryDecoder.primaryKey(
            from: tuple,
            remainingIndexedFieldCount: 2,
            fieldName: "category"
        )

        #expect(primaryKey == Tuple("entity-id"))
    }

    @Test("Composite primary keys retain every component")
    func preservesCompositePrimaryKey() throws {
        let tuple = Tuple(Int64(42), "tenant", "entity-id")

        let primaryKey = try ScalarFilterIndexEntryDecoder.primaryKey(
            from: tuple,
            remainingIndexedFieldCount: 1,
            fieldName: "sequence"
        )

        #expect(primaryKey == Tuple("tenant", "entity-id"))
    }

    @Test("Missing primary key is a typed failure")
    func rejectsMissingPrimaryKey() {
        #expect(
            throws: FilterError.malformedIndexEntry(
                fieldName: "sequence",
                indexedFieldCount: 1,
                elementCount: 1
            )
        ) {
            try ScalarFilterIndexEntryDecoder.primaryKey(
                from: Tuple(Int64(42)),
                remainingIndexedFieldCount: 1,
                fieldName: "sequence"
            )
        }
    }
}
