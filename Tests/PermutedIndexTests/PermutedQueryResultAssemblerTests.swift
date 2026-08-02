import StorageKit
import Testing
@testable import PermutedIndex

@Suite("Permuted query result assembly")
struct PermutedQueryResultAssemblerTests {
    @Test("Fetched items preserve index order")
    func preservesIndexOrder() throws {
        let identifiers = [Tuple("first"), Tuple("second")]

        let items = try PermutedQueryResultAssembler.requireItems(
            identifiers: identifiers,
            fetchedItems: ["first", "second"],
            indexName: "items_by_value"
        )

        #expect(items == ["first", "second"])
    }

    @Test("Missing indexed item fails explicitly")
    func rejectsMissingItem() {
        let missingIdentifier = Tuple("missing")

        #expect(
            throws: PermutedQueryError.indexedItemMissing(
                index: "items_by_value",
                primaryKey: missingIdentifier.pack()
            )
        ) {
            try PermutedQueryResultAssembler.requireItems(
                identifiers: [missingIdentifier],
                fetchedItems: [Optional<String>.none],
                indexName: "items_by_value"
            )
        }
    }

    @Test("Mismatched fetch result count fails explicitly")
    func rejectsMismatchedCount() {
        #expect(
            throws: PermutedQueryError.fetchedItemCountMismatch(
                index: "items_by_value",
                expected: 1,
                actual: 0
            )
        ) {
            try PermutedQueryResultAssembler.requireItems(
                identifiers: [Tuple("first")],
                fetchedItems: [String?](),
                indexName: "items_by_value"
            )
        }
    }
}
