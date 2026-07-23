import Testing
@testable import DatabaseEngine

@Suite("Query result window")
struct QueryResultWindowTests {
    @Test("Negative limits and offsets are rejected")
    func negativeBoundsAreRejected() {
        #expect(throws: DatabaseQueryError.invalidLimit(-1)) {
            try QueryResultWindow.validate(limit: -1, offset: nil)
        }
        #expect(throws: DatabaseQueryError.invalidOffset(-1)) {
            try QueryResultWindow.validate(limit: nil, offset: -1)
        }
    }

    @Test("Offset and limit shrink the owned result buffer")
    func windowIsAppliedInPlace() {
        var values = [0, 1, 2, 3, 4]
        QueryResultWindow.apply(to: &values, limit: 2, offset: 1)
        #expect(values == [1, 2])

        QueryResultWindow.apply(to: &values, limit: nil, offset: 10)
        #expect(values.isEmpty)
    }

    @Test("Count uses the same window semantics")
    func countUsesWindowSemantics() {
        #expect(
            QueryResultWindow.resultCount(
                totalCount: 10,
                limit: 3,
                offset: 4
            ) == 3
        )
        #expect(
            QueryResultWindow.resultCount(
                totalCount: 2,
                limit: 3,
                offset: 4
            ) == 0
        )
    }

    @Test("Index limit pushdown requires semantic equivalence")
    func indexLimitPushdownRequiresSemanticEquivalence() {
        #expect(
            QueryResultWindow.indexReadLimit(
                requestedLimit: 5,
                offset: nil,
                hasSort: false,
                requiresPostFilter: false,
                hasSecurityFilter: false
            ) == 5
        )
        #expect(
            QueryResultWindow.indexReadLimit(
                requestedLimit: 5,
                offset: 1,
                hasSort: false,
                requiresPostFilter: false,
                hasSecurityFilter: false
            ) == nil
        )
        #expect(
            QueryResultWindow.indexReadLimit(
                requestedLimit: 5,
                offset: nil,
                hasSort: true,
                requiresPostFilter: false,
                hasSecurityFilter: false
            ) == nil
        )
        #expect(
            QueryResultWindow.indexReadLimit(
                requestedLimit: 5,
                offset: nil,
                hasSort: false,
                requiresPostFilter: true,
                hasSecurityFilter: false
            ) == nil
        )
        #expect(
            QueryResultWindow.indexReadLimit(
                requestedLimit: 5,
                offset: nil,
                hasSort: false,
                requiresPostFilter: false,
                hasSecurityFilter: true
            ) == nil
        )
    }
}
