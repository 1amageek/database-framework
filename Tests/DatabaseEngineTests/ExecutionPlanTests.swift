import Testing
import Foundation
import Core
import QueryIR
import TestSupport
@testable import DatabaseEngine

/// Unit tests for the canonical `ExecutionPlan<T>` abstraction.
///
/// These tests exercise the planner-side shape: pushdown derivation,
/// residual propagation, binding extraction, and the explain output.
/// No storage engine is required — the tests run against pure plan values.
@Suite("ExecutionPlan Tests")
struct ExecutionPlanTests {

    // MARK: - Derivation: full scan fallback

    @Test("No forced index produces a full scan plan")
    func deriveFullScanWhenNoForcedIndex() throws {
        var query = Query<Player>()
        query.fetchLimit = 10
        let pushdown = SelectQueryPushdownPlan<Player>(
            typedQuery: query,
            residualFilter: nil,
            residualOrderBy: nil,
            limitPushed: true,
            offsetPushed: false
        )

        let plan = pushdown.executionPlan
        if case .fullScan(let full) = plan {
            #expect(full.limit == 10)
            #expect(full.offset == nil)
            #expect(full.residualFilter == nil)
            #expect(full.residualOrderBy == nil)
        } else {
            Issue.record("Expected fullScan, got \(plan)")
        }
    }

    @Test("Non-pushed limit is not reported on the full-scan plan")
    func nonPushedLimitOmittedFromPlan() throws {
        var query = Query<Player>()
        query.fetchLimit = 10
        let pushdown = SelectQueryPushdownPlan<Player>(
            typedQuery: query,
            residualFilter: nil,
            residualOrderBy: nil,
            limitPushed: false,
            offsetPushed: false
        )

        let plan = pushdown.executionPlan
        #expect(plan.limit == nil)
    }

    // MARK: - Derivation: forced-index access

    @Test("Forced index produces an index-access plan")
    func deriveIndexAccessWhenForced() throws {
        var query = Query<Player>()
        query.forcedIndex = IndexHint(indexName: "idx_score")
        let pushdown = SelectQueryPushdownPlan<Player>(
            typedQuery: query,
            residualFilter: nil,
            residualOrderBy: nil,
            limitPushed: false,
            offsetPushed: false
        )

        let plan = pushdown.executionPlan
        guard case .indexAccess(let access) = plan else {
            Issue.record("Expected indexAccess, got \(plan)")
            return
        }
        #expect(access.indexName == "idx_score")
        #expect(access.direction == .forward)
        #expect(access.range == nil)
        #expect(access.bindings.isEmpty)
    }

    @Test("Equality predicates become bindings on an index-access plan")
    func equalityPredicatesBecomeBindings() throws {
        var query = Query<Player>()
        query.forcedIndex = IndexHint(indexName: "idx_level_score")
        query.predicates = [
            .comparison(FieldComparison<Player>(
                keyPath: \Player.level,
                op: .equal,
                value: .int64(5)
            ))
        ]

        let pushdown = SelectQueryPushdownPlan<Player>(
            typedQuery: query,
            residualFilter: nil,
            residualOrderBy: nil,
            limitPushed: false,
            offsetPushed: false
        )

        guard case .indexAccess(let access) = pushdown.executionPlan else {
            Issue.record("Expected indexAccess")
            return
        }
        #expect(access.bindings.count == 1)
        #expect(access.bindings.first?.fieldName == "level")
        #expect(access.bindings.first?.value == .int64(5))
    }

    @Test("Non-equality predicates are not lifted as bindings")
    func nonEqualityPredicatesAreNotBindings() throws {
        var query = Query<Player>()
        query.forcedIndex = IndexHint(indexName: "idx_score")
        query.predicates = [
            .comparison(FieldComparison<Player>(
                keyPath: \Player.score,
                op: .greaterThan,
                value: .int64(100)
            ))
        ]

        let pushdown = SelectQueryPushdownPlan<Player>(
            typedQuery: query,
            residualFilter: nil,
            residualOrderBy: nil,
            limitPushed: false,
            offsetPushed: false
        )

        guard case .indexAccess(let access) = pushdown.executionPlan else {
            Issue.record("Expected indexAccess")
            return
        }
        #expect(access.bindings.isEmpty)
    }

    // MARK: - Residual propagation

    @Test("Residual filter and orderBy propagate onto the plan")
    func residualsPropagateToPlan() throws {
        let residualFilter = QueryIR.Expression.literal(.bool(true))
        let residualOrder: [SortKey] = [
            SortKey(.column(ColumnRef(column: "name")), direction: .ascending)
        ]

        let pushdown = SelectQueryPushdownPlan<Player>(
            typedQuery: Query<Player>(),
            residualFilter: residualFilter,
            residualOrderBy: residualOrder,
            limitPushed: false,
            offsetPushed: false
        )

        let plan = pushdown.executionPlan
        #expect(plan.residualFilter != nil)
        #expect(plan.residualOrderBy?.count == 1)
    }

    // MARK: - Explain

    @Test("Explain output mentions the index name for index-access plans")
    func explainMentionsIndexName() throws {
        let plan = ExecutionPlan<Player>.indexAccess(
            IndexAccessPlan<Player>(
                indexName: "idx_score",
                bindings: [KeyFieldBinding(fieldName: "level", value: .int64(5))],
                range: nil,
                direction: .forward
            )
        )

        let text = plan.explain()
        #expect(text.contains("idx_score"))
        #expect(text.contains("level=5"))
        #expect(text.contains("direction: asc"))
    }

    @Test("Explain output mentions the type name for full-scan plans")
    func explainMentionsPersistableType() throws {
        let plan = ExecutionPlan<Player>.fullScan(FullScanPlan<Player>(limit: 20))
        let text = plan.explain()
        #expect(text.contains(Player.persistableType))
        #expect(text.contains("limit: 20"))
    }

    // MARK: - RangeBound

    @Test("Range bound records inclusive vs exclusive endpoints")
    func rangeBoundEndpointsSurface() throws {
        let range = KeyRangeBound(
            fieldName: "score",
            lower: .inclusive(.int64(10)),
            upper: .exclusive(.int64(100))
        )
        #expect(range.lower?.isInclusive == true)
        #expect(range.upper?.isInclusive == false)
        #expect(range.lower?.value == .int64(10))
        #expect(range.upper?.value == .int64(100))
    }
}
