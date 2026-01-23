/// QueryPlanTests.swift
/// Comprehensive tests for QueryPlan types

import Testing
@testable import QueryAST

// MARK: - QueryPlan Tests

@Suite("QueryPlan Tests")
struct QueryPlanTests {

    @Test("QueryPlan construction")
    func testConstruction() throws {
        let plan = QueryPlan(
            node: .tableScan(TableScanPlan(schema: "users")),
            cost: QueryCost(startup: 0, total: 100, rows: 1000, width: 50),
            indexes: [],
            statistics: nil
        )

        #expect(plan.cost.total == 100)
        #expect(plan.indexes.isEmpty)
    }

    @Test("QueryPlan with indexes")
    func testWithIndexes() throws {
        let plan = QueryPlan(
            node: .indexScan(IndexScanPlan(
                schema: "users",
                indexName: "idx_name",
                bounds: .exact([.string("Alice")])
            )),
            cost: QueryCost(startup: 0, total: 10, rows: 1, width: 50),
            indexes: [IndexUsage(indexName: "idx_name", kind: .scalar, accessPattern: .exactMatch)],
            statistics: PlanStatistics(planningTimeMs: 1.0, alternativesConsidered: 3)
        )

        #expect(plan.indexes.count == 1)
        #expect(plan.indexes[0].indexName == "idx_name")
    }
}

// MARK: - Scan Plan Tests

@Suite("Scan Plan Tests")
struct ScanPlanTests {

    @Test("TableScanPlan construction")
    func testTableScanPlan() throws {
        let scan = TableScanPlan(schema: "users")
        #expect(scan.schema == "users")
        #expect(scan.filter == nil)

        let filtered = TableScanPlan(
            schema: "users",
            filter: .greaterThan(.column(ColumnRef(column: "age")), .literal(.int(18)))
        )
        #expect(filtered.filter != nil)
    }

    @Test("IndexScanPlan construction")
    func testIndexScanPlan() throws {
        let scan = IndexScanPlan(
            schema: "users",
            indexName: "idx_email",
            bounds: .exact([.string("test@example.com")])
        )

        #expect(scan.schema == "users")
        #expect(scan.indexName == "idx_email")
        #expect(scan.filter == nil)
    }

    @Test("IndexScanPlan with filter")
    func testIndexScanPlanWithFilter() throws {
        let scan = IndexScanPlan(
            schema: "users",
            indexName: "idx_age",
            bounds: .range(from: [.int(18)], to: [.int(65)]),
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        )

        #expect(scan.filter != nil)
    }

    @Test("BitmapScanPlan construction")
    func testBitmapScanPlan() throws {
        let scan1 = IndexScanPlan(schema: "users", indexName: "idx1", bounds: .exact([.int(1)]))
        let scan2 = IndexScanPlan(schema: "users", indexName: "idx2", bounds: .exact([.int(2)]))

        let bitmap = BitmapScanPlan(
            schema: "users",
            scans: [scan1, scan2],
            operation: .and
        )

        #expect(bitmap.scans.count == 2)
        #expect(bitmap.operation == .and)
    }

    @Test("BitmapOperation types")
    func testBitmapOperationTypes() throws {
        let andOp = BitmapOperation.and
        let orOp = BitmapOperation.or

        #expect(andOp != orOp)
        #expect(andOp == .and)
        #expect(orOp == .or)
    }
}

// MARK: - IndexBounds Tests

@Suite("IndexBounds Tests")
struct IndexBoundsTests {

    @Test("IndexBounds exact")
    func testExact() throws {
        let bounds = IndexBounds.exact([.string("Alice"), .int(30)])

        #expect(bounds.lower == [.string("Alice"), .int(30)])
        #expect(bounds.upper == [.string("Alice"), .int(30)])
        #expect(bounds.lowerInclusive == true)
        #expect(bounds.upperInclusive == true)
    }

    @Test("IndexBounds range")
    func testRange() throws {
        let bounds = IndexBounds.range(
            from: [.int(10)],
            to: [.int(100)],
            inclusive: true
        )

        #expect(bounds.lower == [.int(10)])
        #expect(bounds.upper == [.int(100)])
    }

    @Test("IndexBounds prefix")
    func testPrefix() throws {
        let bounds = IndexBounds.prefix([.string("ABC")])

        #expect(bounds.lower == [.string("ABC")])
        #expect(bounds.upper == [.string("ABC")])
    }

    @Test("IndexBounds unbounded")
    func testUnbounded() throws {
        let bounds = IndexBounds(lower: nil, upper: nil)

        #expect(bounds.lower == nil)
        #expect(bounds.upper == nil)
    }

    @Test("IndexBounds half-open ranges")
    func testHalfOpenRanges() throws {
        let lowerOnly = IndexBounds(lower: [.int(10)], upper: nil)
        #expect(lowerOnly.lower != nil)
        #expect(lowerOnly.upper == nil)

        let upperOnly = IndexBounds(lower: nil, upper: [.int(100)])
        #expect(upperOnly.lower == nil)
        #expect(upperOnly.upper != nil)
    }
}

// MARK: - Join Plan Tests

@Suite("Join Plan Tests")
struct JoinPlanTests {

    @Test("JoinPlan construction")
    func testJoinPlan() throws {
        let left = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let right = QueryPlanNode.tableScan(TableScanPlan(schema: "orders"))

        let join = JoinPlan(
            left: left,
            right: right,
            condition: .equal(
                .column(ColumnRef(table: "users", column: "id")),
                .column(ColumnRef(table: "orders", column: "user_id"))
            ),
            joinType: .inner
        )

        #expect(join.joinType == .inner)
        #expect(join.condition != nil)
    }

    @Test("HashJoinPlan construction")
    func testHashJoinPlan() throws {
        let build = QueryPlanNode.tableScan(TableScanPlan(schema: "small_table"))
        let probe = QueryPlanNode.tableScan(TableScanPlan(schema: "large_table"))

        let hashJoin = HashJoinPlan(
            build: build,
            probe: probe,
            buildKeys: [.column(ColumnRef(column: "id"))],
            probeKeys: [.column(ColumnRef(column: "fk_id"))],
            joinType: .inner
        )

        #expect(hashJoin.buildKeys.count == 1)
        #expect(hashJoin.probeKeys.count == 1)
    }

    @Test("MergeJoinPlan construction")
    func testMergeJoinPlan() throws {
        let left = QueryPlanNode.indexScan(IndexScanPlan(schema: "a", indexName: "idx_a", bounds: IndexBounds()))
        let right = QueryPlanNode.indexScan(IndexScanPlan(schema: "b", indexName: "idx_b", bounds: IndexBounds()))

        let mergeJoin = MergeJoinPlan(
            left: left,
            right: right,
            leftKeys: [.column(ColumnRef(column: "key"))],
            rightKeys: [.column(ColumnRef(column: "key"))],
            joinType: .left
        )

        #expect(mergeJoin.joinType == .left)
    }
}

// MARK: - Graph Plan Tests

@Suite("Graph Plan Tests")
struct GraphPlanTests {

    @Test("GraphTraversalPlan construction")
    func testGraphTraversalPlan() throws {
        let start = QueryPlanNode.tableScan(TableScanPlan(schema: "nodes"))
        let pattern = MatchPattern(paths: [
            PathPattern(elements: [
                .node(NodePattern(variable: "a")),
                .edge(EdgePattern(direction: .outgoing)),
                .node(NodePattern(variable: "b"))
            ])
        ])

        let plan = GraphTraversalPlan(
            start: start,
            pattern: pattern,
            strategy: .breadthFirst
        )

        #expect(plan.strategy == .breadthFirst)
    }

    @Test("TraversalStrategy types")
    func testTraversalStrategy() throws {
        #expect(TraversalStrategy.depthFirst != TraversalStrategy.breadthFirst)
        #expect(TraversalStrategy.bidirectional != TraversalStrategy.depthFirst)
    }

    @Test("ShortestPathPlan construction")
    func testShortestPathPlan() throws {
        let start = QueryPlanNode.tableScan(TableScanPlan(schema: "nodes"))
        let end = QueryPlanNode.tableScan(TableScanPlan(schema: "nodes"))
        let pattern = PathPattern(elements: [
            .edge(EdgePattern(direction: .outgoing))
        ])

        let plan = ShortestPathPlan(
            start: start,
            end: end,
            pattern: pattern,
            algorithm: .dijkstra
        )

        #expect(plan.algorithm == .dijkstra)
    }

    @Test("ShortestPathAlgorithm types")
    func testShortestPathAlgorithm() throws {
        let algorithms: [ShortestPathAlgorithm] = [.dijkstra, .bellmanFord, .bfs, .bidirectionalBFS]
        for alg in algorithms {
            #expect(alg == alg)
        }
    }
}

// MARK: - Triple Pattern Plan Tests

@Suite("Triple Pattern Plan Tests")
struct TriplePatternPlanTests {

    @Test("TriplePatternScanPlan construction")
    func testTriplePatternScanPlan() throws {
        let pattern = TriplePattern(
            subject: .variable("s"),
            predicate: .iri("http://example.org/knows"),
            object: .variable("o")
        )

        let plan = TriplePatternScanPlan(
            pattern: pattern,
            index: .spo,
            bindings: ["s": .iri("http://example.org/alice")]
        )

        #expect(plan.index == .spo)
        #expect(plan.bindings.count == 1)
    }

    @Test("TripleIndex types")
    func testTripleIndexTypes() throws {
        let indexes: [TripleIndex] = [.spo, .pos, .osp, .sop, .pso, .ops]
        #expect(indexes.count == 6)
        for index in indexes {
            #expect(index == index)
        }
    }

    @Test("PropertyPathPlan construction")
    func testPropertyPathPlan() throws {
        let path = PropertyPath.oneOrMore(.iri("http://example.org/knows"))

        let plan = PropertyPathPlan(
            subject: nil,
            path: path,
            object: nil,
            algorithm: .iterative
        )

        #expect(plan.algorithm == .iterative)
    }

    @Test("PathEvalAlgorithm types")
    func testPathEvalAlgorithm() throws {
        let algorithms: [PathEvalAlgorithm] = [.iterative, .recursive, .automaton]
        for alg in algorithms {
            #expect(alg == alg)
        }
    }
}

// MARK: - Transformation Plan Tests

@Suite("Transformation Plan Tests")
struct TransformationPlanTests {

    @Test("FilterPlan construction")
    func testFilterPlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = FilterPlan(
            input: input,
            condition: .greaterThan(.column(ColumnRef(column: "age")), .literal(.int(18)))
        )

        if case .greaterThan = plan.condition {
            // OK
        } else {
            Issue.record("Expected greaterThan condition")
        }
    }

    @Test("ProjectPlan construction")
    func testProjectPlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = ProjectPlan(
            input: input,
            columns: [
                ProjectionItem(.column(ColumnRef(column: "name")), alias: "user_name"),
                ProjectionItem(.column(ColumnRef(column: "email")))
            ]
        )

        #expect(plan.columns.count == 2)
    }

    @Test("SortPlan construction")
    func testSortPlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = SortPlan(
            input: input,
            keys: [
                SortKey(.column(ColumnRef(column: "name")), direction: .ascending),
                SortKey(.column(ColumnRef(column: "created_at")), direction: .descending)
            ],
            limit: 100
        )

        #expect(plan.keys.count == 2)
        #expect(plan.limit == 100)
    }

    @Test("LimitPlan construction")
    func testLimitPlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = LimitPlan(input: input, count: 10, offset: 20)

        #expect(plan.count == 10)
        #expect(plan.offset == 20)
    }

    @Test("DistinctPlan construction")
    func testDistinctPlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = DistinctPlan(
            input: input,
            columns: [.column(ColumnRef(column: "category"))]
        )

        #expect(plan.columns?.count == 1)
    }

    @Test("AggregatePlan construction")
    func testAggregatePlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "orders"))
        let plan = AggregatePlan(
            input: input,
            groupBy: [.column(ColumnRef(column: "customer_id"))],
            aggregates: [
                .sum(.column(ColumnRef(column: "amount")), distinct: false),
                .count(nil, distinct: false)
            ]
        )

        #expect(plan.groupBy.count == 1)
        #expect(plan.aggregates.count == 2)
    }
}

// MARK: - Set Operation Plan Tests

@Suite("Set Operation Plan Tests")
struct SetOperationPlanTests {

    @Test("SetOperationPlan construction")
    func testSetOperationPlan() throws {
        let input1 = QueryPlanNode.tableScan(TableScanPlan(schema: "active_users"))
        let input2 = QueryPlanNode.tableScan(TableScanPlan(schema: "premium_users"))

        let plan = SetOperationPlan(inputs: [input1, input2])
        #expect(plan.inputs.count == 2)
    }
}

// MARK: - Special Plan Tests

@Suite("Special Plan Tests")
struct SpecialPlanTests {

    @Test("VectorSearchPlan construction")
    func testVectorSearchPlan() throws {
        let plan = VectorSearchPlan(
            schema: "documents",
            field: "embedding",
            query: [0.1, 0.2, 0.3, 0.4],
            k: 10,
            metric: .cosine,
            filter: .equal(.column(ColumnRef(column: "category")), .literal(.string("tech")))
        )

        #expect(plan.schema == "documents")
        #expect(plan.field == "embedding")
        #expect(plan.k == 10)
        #expect(plan.metric == .cosine)
        #expect(plan.filter != nil)
    }

    @Test("VectorMetric types")
    func testVectorMetric() throws {
        let metrics: [VectorMetric] = [.cosine, .euclidean, .dotProduct, .manhattan]
        for metric in metrics {
            #expect(metric == metric)
        }
    }

    @Test("FullTextSearchPlan construction")
    func testFullTextSearchPlan() throws {
        let plan = FullTextSearchPlan(
            schema: "articles",
            field: "content",
            query: "swift concurrency",
            mode: .match
        )

        #expect(plan.schema == "articles")
        #expect(plan.query == "swift concurrency")
        #expect(plan.mode == .match)
    }

    @Test("FullTextSearchMode types")
    func testFullTextSearchMode() throws {
        let modes: [FullTextSearchMode] = [.match, .phrase, .prefix, .fuzzy, .boolean]
        for mode in modes {
            #expect(mode == mode)
        }
    }

    @Test("SpatialSearchPlan construction")
    func testSpatialSearchPlan() throws {
        let plan = SpatialSearchPlan(
            schema: "locations",
            field: "coordinates",
            query: .withinRadius(lat: 35.6762, lon: 139.6503, radiusMeters: 1000)
        )

        #expect(plan.schema == "locations")
        if case .withinRadius(let lat, _, let radius) = plan.query {
            #expect(abs(lat - 35.6762) < 0.0001)
            #expect(radius == 1000)
        }
    }

    @Test("SpatialQuery types")
    func testSpatialQueryTypes() throws {
        let radius = SpatialQuery.withinRadius(lat: 0, lon: 0, radiusMeters: 100)
        let bounds = SpatialQuery.withinBounds(minLat: 0, minLon: 0, maxLat: 1, maxLon: 1)
        let nearest = SpatialQuery.nearestK(lat: 0, lon: 0, k: 5)
        let intersects = SpatialQuery.intersects(geometry: "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")

        #expect(radius != bounds)
        #expect(nearest != intersects)
    }

    @Test("ValuesPlan construction")
    func testValuesPlan() throws {
        let plan = ValuesPlan(
            columns: ["x", "y", "z"],
            rows: [
                [.int(1), .int(2), .int(3)],
                [.int(4), .int(5), .int(6)]
            ]
        )

        #expect(plan.columns.count == 3)
        #expect(plan.rows.count == 2)
    }

    @Test("SubqueryPlan construction")
    func testSubqueryPlan() throws {
        let inner = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = SubqueryPlan(plan: inner, alias: "subq", lateral: true)

        #expect(plan.alias == "subq")
        #expect(plan.lateral == true)
    }

    @Test("MaterializePlan construction")
    func testMaterializePlan() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let plan = MaterializePlan(input: input, hint: .always)

        #expect(plan.hint == .always)
    }

    @Test("MaterializeHint types")
    func testMaterializeHint() throws {
        let hints: [MaterializeHint] = [.always, .onReuse, .never]
        for hint in hints {
            #expect(hint == hint)
        }
    }
}

// MARK: - QueryPlanNode Equality Tests

@Suite("QueryPlanNode Equality Tests")
struct QueryPlanNodeEqualityTests {

    @Test("tableScan equality")
    func testTableScanEquality() throws {
        let node1 = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let node2 = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let node3 = QueryPlanNode.tableScan(TableScanPlan(schema: "orders"))

        #expect(node1 == node2)
        #expect(node1 != node3)
    }

    @Test("indexScan equality")
    func testIndexScanEquality() throws {
        let node1 = QueryPlanNode.indexScan(IndexScanPlan(schema: "users", indexName: "idx", bounds: IndexBounds()))
        let node2 = QueryPlanNode.indexScan(IndexScanPlan(schema: "users", indexName: "idx", bounds: IndexBounds()))

        #expect(node1 == node2)
    }

    @Test("filter equality")
    func testFilterEquality() throws {
        let input = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let condition = Expression.equal(.column(ColumnRef(column: "id")), .literal(.int(1)))

        let node1 = QueryPlanNode.filter(FilterPlan(input: input, condition: condition))
        let node2 = QueryPlanNode.filter(FilterPlan(input: input, condition: condition))

        #expect(node1 == node2)
    }

    @Test("different node types not equal")
    func testDifferentNodeTypesNotEqual() throws {
        let tableScan = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let indexScan = QueryPlanNode.indexScan(IndexScanPlan(schema: "users", indexName: "idx", bounds: IndexBounds()))

        #expect(tableScan != indexScan)
    }
}
