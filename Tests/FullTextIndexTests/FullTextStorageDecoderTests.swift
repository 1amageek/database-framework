import Testing
import StorageKit
@testable import FullTextIndex

@Suite("Full-text storage decoder")
struct FullTextStorageDecoderTests {
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
