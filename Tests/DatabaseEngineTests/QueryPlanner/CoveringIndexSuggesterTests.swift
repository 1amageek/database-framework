#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import Testing

@Suite("Canonical covering index suggestions", .heartbeat)
struct CoveringIndexSuggesterTests {
    @Test("An incomplete usable index receives every missing field")
    func extendsIncompleteIndex() throws {
        let query = Query<IndexProjectionRecord>()
            .where(\IndexProjectionRecord.email == "owner@example.com")
        let analysis = try QueryAnalyzer<IndexProjectionRecord>().analyze(query)
        let suggestion = CoveringIndexSuggester<IndexProjectionRecord>().suggest(
            analysis: analysis,
            existingIndexes: [
                IndexProjectionRecordFactory.descriptor(storedFields: [])
            ]
        )

        #expect(suggestion?.type == .extendExisting)
        #expect(Set(suggestion?.storedFields ?? []) == [
            "name", "age", "nickname", "tags", "target",
        ])
    }

    @Test("An existing complete index needs no suggestion")
    func completeIndexNeedsNoChange() throws {
        let query = Query<IndexProjectionRecord>()
            .where(\IndexProjectionRecord.email == "owner@example.com")
        let analysis = try QueryAnalyzer<IndexProjectionRecord>().analyze(query)

        #expect(
            CoveringIndexSuggester<IndexProjectionRecord>().suggest(
                analysis: analysis,
                existingIndexes: [IndexProjectionRecordFactory.descriptor()]
            ) == nil
        )
    }
}
#endif
#endif
