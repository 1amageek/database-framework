#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import Testing

@Suite("Canonical index-only plan enumeration", .heartbeat)
struct PlanEnumeratorIndexOnlyTests {
    @Test("Planner emits index-only only for a complete DBIX projection")
    func planSelection() throws {
        let query = Query<IndexProjectionRecord>()
            .where(\IndexProjectionRecord.email == "owner@example.com")
        let analysis = try QueryAnalyzer<IndexProjectionRecord>().analyze(query)
        let statistics = HeuristicStatisticsProvider(defaultRowCount: 100)
        let fullPlans = PlanEnumerator<IndexProjectionRecord>(
            indexes: [IndexProjectionRecordFactory.descriptor()],
            statistics: statistics
        ).enumerate(analysis: analysis)
        let partialPlans = PlanEnumerator<IndexProjectionRecord>(
            indexes: [IndexProjectionRecordFactory.descriptor(storedFields: [])],
            statistics: statistics
        ).enumerate(analysis: analysis)

        #expect(fullPlans.contains { plan in
            if case .indexOnlyScan = plan { return true }
            return false
        })
        #expect(partialPlans.contains { plan in
            if case .indexOnlyScan = plan { return true }
            return false
        } == false)
    }
}
#endif
#endif
