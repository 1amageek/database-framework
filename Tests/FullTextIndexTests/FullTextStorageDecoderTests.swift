import Testing
import StorageKit
@testable import FullTextIndex

@Suite("Full-text storage decoder")
struct FullTextStorageDecoderTests {
    @Test("posting payload has one canonical frequency-first representation")
    func postingContract() throws {
        let posting = try FullTextStorageDecoder.posting(
            from: Tuple(Int64(2), Int64(0), Int64(5)).pack(),
            positionsStored: true,
            term: "swift"
        )
        #expect(posting.termFrequency == 2)
        #expect(posting.positions == [0, 5])

        let frequencyOnly = try FullTextStorageDecoder.posting(
            from: Tuple(Int64(3)).pack(),
            positionsStored: false,
            term: "swift"
        )
        #expect(frequencyOnly.termFrequency == 3)
        #expect(frequencyOnly.positions.isEmpty)

        #expect(
            try FullTextStorageDecoder.termFrequency(
                from: Tuple(Int64(1), Int64.max).pack(),
                positionsStored: true,
                term: "swift"
            ) == 1
        )

        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.posting(
                from: Tuple(Int64(2), Int64(0)).pack(),
                positionsStored: true,
                term: "swift"
            )
        }
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.posting(
                from: Tuple(Int64(2), Int64(4), Int64(4)).pack(),
                positionsStored: true,
                term: "swift"
            )
        }
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.termFrequency(
                from: Tuple(Int64(3), Int64(1), Int64(3), Int64(2)).pack(),
                positionsStored: true,
                term: "swift"
            )
        }
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.posting(
                from: Tuple(Int64(1), Int64(0)).pack(),
                positionsStored: false,
                term: "swift"
            )
        }
    }

    @Test("document metadata requires exactly two nonnegative integers")
    func documentMetadataContract() throws {
        let metadata = try FullTextStorageDecoder.documentMetadata(
            from: Tuple(Int64(4), Int64(12)).pack()
        )
        #expect(metadata.uniqueTermCount == 4)
        #expect(metadata.docLength == 12)

        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.documentMetadata(
                from: Tuple(Int64(4)).pack()
            )
        }
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.documentMetadata(
                from: Tuple(Int64(4), Int64(-1)).pack()
            )
        }
    }

    @Test("facet keys decode string values and reject corrupt values")
    func facetKeyContract() throws {
        let fieldSubspace = Subspace(prefix: Tuple("fulltext", "facets", "category").pack())
        let validKey = fieldSubspace.pack(Tuple("books"))
        let corruptKey = fieldSubspace.pack(Tuple(42))

        #expect(try FullTextStorageDecoder.facetValue(from: validKey, in: fieldSubspace, field: "category") == "books")
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.facetValue(from: corruptKey, in: fieldSubspace, field: "category")
        }
    }

    @Test("document facet payload rejects non-string values")
    func documentFacetValuesContract() throws {
        let validPayload = Tuple(["ios", "swift"]).pack()
        let corruptPayload = Tuple(["ios" as any TupleElement, 42 as any TupleElement]).pack()

        #expect(try FullTextStorageDecoder.documentFacetValues(from: validPayload, field: "tags") == ["ios", "swift"])
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.documentFacetValues(from: corruptPayload, field: "tags")
        }
    }

    @Test("autocomplete keys decode string terms and reject corrupt values")
    func autocompleteKeyContract() throws {
        let suggestionSubspace = Subspace(prefix: Tuple("fulltext", "suggestions", "title", "swi").pack())
        let termSubspace = Subspace(prefix: Tuple("fulltext", "terms", "title").pack())

        let validSuggestionKey = suggestionSubspace.pack(Tuple("swift"))
        let corruptSuggestionKey = suggestionSubspace.pack(Tuple(42))
        let validTermKey = termSubspace.pack(Tuple("swift"))
        let corruptTermKey = termSubspace.pack(Tuple(42))

        #expect(
            try FullTextStorageDecoder.autocompleteSuggestionTerm(
                from: validSuggestionKey,
                in: suggestionSubspace,
                field: "title",
                prefix: "swi"
            ) == "swift"
        )
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.autocompleteSuggestionTerm(
                from: corruptSuggestionKey,
                in: suggestionSubspace,
                field: "title",
                prefix: "swi"
            )
        }
        #expect(try FullTextStorageDecoder.autocompleteTerm(from: validTermKey, in: termSubspace, field: "title") == "swift")
        #expect(throws: FullTextStorageError.self) {
            _ = try FullTextStorageDecoder.autocompleteTerm(from: corruptTermKey, in: termSubspace, field: "title")
        }
    }
}
