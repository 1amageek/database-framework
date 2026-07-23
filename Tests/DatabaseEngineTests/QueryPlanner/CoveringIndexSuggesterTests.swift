#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import Testing

@Suite("Canonical covering index suggestions", .heartbeat)
struct CoveringIndexSuggesterTests {
    @Test("An incomplete usable index receives every missing field")
    func extendsIncompleteIndex() throws {
        let query = Query<IndexProjectionEntity>()
            .where(\IndexProjectionEntity.email == "owner@example.com")
        let analysis = try QueryAnalyzer<IndexProjectionEntity>().analyze(query)
        let suggestion = CoveringIndexSuggester<IndexProjectionEntity>().suggest(
            analysis: analysis,
            existingIndexes: [
                IndexProjectionEntityFactory.descriptor(storedFields: [])
            ]
        )

        #expect(suggestion?.type == .extendExisting)
        #expect(Set(suggestion?.storedFields ?? []) == [
            "name", "age", "nickname", "tags", "target",
        ])
    }

    @Test("An existing complete index needs no suggestion")
    func completeIndexNeedsNoChange() throws {
        let query = Query<IndexProjectionEntity>()
            .where(\IndexProjectionEntity.email == "owner@example.com")
        let analysis = try QueryAnalyzer<IndexProjectionEntity>().analyze(query)

        #expect(
            CoveringIndexSuggester<IndexProjectionEntity>().suggest(
                analysis: analysis,
                existingIndexes: [IndexProjectionEntityFactory.descriptor()]
            ) == nil
        )
    }
}
#endif
#endif
