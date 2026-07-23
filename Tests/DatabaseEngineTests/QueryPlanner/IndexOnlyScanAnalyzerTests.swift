#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import Testing

@Suite("Canonical index-only analyzer", .heartbeat)
struct IndexOnlyScanAnalyzerTests {
    @Test("Only complete canonical projections produce index-only plans")
    func coverageDecision() throws {
        let query = Query<IndexProjectionRecord>()
            .where(\IndexProjectionRecord.email == "owner@example.com")
        let analysis = try QueryAnalyzer<IndexProjectionRecord>().analyze(query)
        let analyzer = IndexOnlyScanAnalyzer<IndexProjectionRecord>()

        let full = analyzer.analyze(
            analysis: analysis,
            index: IndexProjectionRecordFactory.descriptor()
        )
        let partial = analyzer.analyze(
            analysis: analysis,
            index: IndexProjectionRecordFactory.descriptor(storedFields: [])
        )

        #expect(full.canUseIndexOnlyScan)
        #expect(full.uncoveredFields.isEmpty)
        #expect(full.estimatedSavings > 0)
        #expect(partial.canUseIndexOnlyScan == false)
        #expect(partial.uncoveredFields.contains("name"))
    }
}
#endif
#endif
