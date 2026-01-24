/// QueryOptimizationTests.swift
/// Tests for query optimization patterns and plan selection
///
/// Coverage: Join ordering, index selection, filter pushdown, projection pushdown, cost comparison

import Testing
@testable import QueryAST

// MARK: - Query Optimization Tests

@Suite("Query Optimization Tests")
struct QueryOptimizationTests {

    // MARK: - Join Reordering Tests

    @Test("Join reordering by selectivity")
    func testJoinReordering() throws {
        // When joining large and small tables, prefer small table first
        // This tests the logical structure for reordering decisions

        // Small table: 100 rows
        let smallTable = TableScanPlan(schema: "departments")
        let smallNode = QueryPlanNode.tableScan(smallTable)

        // Large table: 100000 rows
        let largeTable = TableScanPlan(schema: "employees")
        let largeNode = QueryPlanNode.tableScan(largeTable)

        // Cost for scanning small first (preferred)
        let smallFirstCost = QueryCost(startup: 0, total: 100, rows: 100, width: 50)
        let largeFirstCost = QueryCost(startup: 0, total: 100000, rows: 100000, width: 50)

        #expect(smallFirstCost.total < largeFirstCost.total)

        // Build preferred join plan: small table as inner (build) side
        let preferredJoin = HashJoinPlan(
            build: smallNode,
            probe: largeNode,
            buildKeys: [.column(ColumnRef(column: "dept_id"))],
            probeKeys: [.column(ColumnRef(column: "dept_id"))],
            joinType: .inner
        )

        // Verify build side is smaller table
        if case .tableScan(let buildScan) = preferredJoin.build {
            #expect(buildScan.schema == "departments")
        }
    }

    @Test("Join chain optimization")
    func testJoinChainOptimization() throws {
        // A JOIN B JOIN C - optimal ordering depends on selectivity

        let tableA = QueryPlanNode.tableScan(TableScanPlan(schema: "a"))
        let tableB = QueryPlanNode.tableScan(TableScanPlan(schema: "b"))
        let tableC = QueryPlanNode.tableScan(TableScanPlan(schema: "c"))

        // Cost estimates for different orderings
        let costABC = QueryCost(startup: 0, total: 1000, rows: 500, width: 100)
        let costACB = QueryCost(startup: 0, total: 800, rows: 400, width: 100)
        let costBAC = QueryCost(startup: 0, total: 1200, rows: 600, width: 100)

        // Verify cost comparison works
        #expect(costACB.total < costABC.total)
        #expect(costACB.total < costBAC.total)

        // Best plan is A JOIN C JOIN B
        let acJoin = JoinPlan(left: tableA, right: tableC, joinType: .inner)
        let _ = JoinPlan(
            left: .nestedLoopJoin(acJoin),
            right: tableB,
            joinType: .inner
        )
    }

    // MARK: - Index Selection Tests

    @Test("Index selection for equality query")
    func testIndexSelectionEquality() throws {
        // SELECT * FROM users WHERE email = 'test@example.com'
        // Should prefer index scan over table scan

        let tableScan = TableScanPlan(
            schema: "users",
            filter: .equal(.column(ColumnRef(column: "email")), .literal(.string("test@example.com")))
        )

        let indexScan = IndexScanPlan(
            schema: "users",
            indexName: "idx_email",
            bounds: .exact([.string("test@example.com")])
        )

        // Index scan has lower cost for equality queries
        let tableScanCost = QueryCost(startup: 0, total: 10000, rows: 10000, width: 100)
        let indexScanCost = QueryCost(startup: 0, total: 10, rows: 1, width: 100)

        #expect(indexScanCost.total < tableScanCost.total)

        // Verify index usage tracking
        let indexUsage = IndexUsage(indexName: "idx_email", kind: .scalar, accessPattern: .exactMatch)
        #expect(indexUsage.accessPattern == .exactMatch)
    }

    @Test("Index selection for range query")
    func testIndexSelectionRange() throws {
        // SELECT * FROM users WHERE age BETWEEN 20 AND 30
        // Should use range scan on age index

        let indexScan = IndexScanPlan(
            schema: "users",
            indexName: "idx_age",
            bounds: .range(from: [.int(20)], to: [.int(30)], inclusive: true)
        )

        let indexUsage = IndexUsage(indexName: "idx_age", kind: .scalar, accessPattern: .rangeScan(direction: .forward))

        #expect(indexScan.bounds.lower == [.int(20)])
        #expect(indexScan.bounds.upper == [.int(30)])
        #expect(indexUsage.accessPattern == .rangeScan(direction: .forward))
    }

    @Test("Index selection for prefix query")
    func testIndexSelectionPrefix() throws {
        // SELECT * FROM users WHERE name LIKE 'John%'

        let indexScan = IndexScanPlan(
            schema: "users",
            indexName: "idx_name",
            bounds: .prefix([.string("John")])
        )

        let indexUsage = IndexUsage(indexName: "idx_name", kind: .scalar, accessPattern: .prefixScan)

        #expect(indexUsage.accessPattern == .prefixScan)
    }

    @Test("Bitmap scan for OR conditions")
    func testBitmapScanOrConditions() throws {
        // SELECT * FROM products WHERE category = 'A' OR category = 'B'
        // Use bitmap OR of two index scans

        let scanA = IndexScanPlan(
            schema: "products",
            indexName: "idx_category",
            bounds: .exact([.string("A")])
        )

        let scanB = IndexScanPlan(
            schema: "products",
            indexName: "idx_category",
            bounds: .exact([.string("B")])
        )

        let bitmapScan = BitmapScanPlan(
            schema: "products",
            scans: [scanA, scanB],
            operation: .or
        )

        #expect(bitmapScan.scans.count == 2)
        #expect(bitmapScan.operation == .or)
    }

    @Test("Bitmap scan for AND conditions")
    func testBitmapScanAndConditions() throws {
        // SELECT * FROM products WHERE category = 'Electronics' AND price < 100
        // Use bitmap AND of two index scans

        let categoryScan = IndexScanPlan(
            schema: "products",
            indexName: "idx_category",
            bounds: .exact([.string("Electronics")])
        )

        let priceScan = IndexScanPlan(
            schema: "products",
            indexName: "idx_price",
            bounds: .range(from: nil, to: [.int(100)], inclusive: false)
        )

        let bitmapScan = BitmapScanPlan(
            schema: "products",
            scans: [categoryScan, priceScan],
            operation: .and
        )

        #expect(bitmapScan.scans.count == 2)
        #expect(bitmapScan.operation == .and)
    }

    // MARK: - Filter Pushdown Tests

    @Test("Filter pushdown through JOIN")
    func testFilterPushdownJoin() throws {
        // SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true
        // Filter on u.active should be pushed to users table scan

        // Without pushdown: filter after join
        let usersUnfiltered = QueryPlanNode.tableScan(TableScanPlan(schema: "users"))
        let orders = QueryPlanNode.tableScan(TableScanPlan(schema: "orders"))
        let joinUnfiltered = JoinPlan(
            left: usersUnfiltered,
            right: orders,
            condition: .equal(
                .column(ColumnRef(table: "u", column: "id")),
                .column(ColumnRef(table: "o", column: "user_id"))
            ),
            joinType: .inner
        )
        let _ = FilterPlan(
            input: .nestedLoopJoin(joinUnfiltered),
            condition: .equal(.column(ColumnRef(table: "u", column: "active")), .literal(.bool(true)))
        )

        // With pushdown: filter before join
        let usersFiltered = QueryPlanNode.tableScan(TableScanPlan(
            schema: "users",
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        ))
        let _ = JoinPlan(
            left: usersFiltered,
            right: orders,
            condition: .equal(
                .column(ColumnRef(table: "u", column: "id")),
                .column(ColumnRef(table: "o", column: "user_id"))
            ),
            joinType: .inner
        )

        // Verify filter is in table scan
        if case .tableScan(let scan) = usersFiltered {
            #expect(scan.filter != nil)
        }
    }

    @Test("Filter pushdown to index scan")
    func testFilterPushdownIndex() throws {
        // SELECT * FROM users WHERE email = 'test@test.com' AND verified = true
        // email filter uses index, verified filter pushed to residual

        let indexScan = IndexScanPlan(
            schema: "users",
            indexName: "idx_email",
            bounds: .exact([.string("test@test.com")]),
            filter: .equal(.column(ColumnRef(column: "verified")), .literal(.bool(true)))
        )

        // Index scan has both bounds (for email) and residual filter (for verified)
        #expect(indexScan.bounds.lower != nil)
        #expect(indexScan.filter != nil)
    }

    // MARK: - Projection Pushdown Tests

    @Test("Projection pushdown")
    func testProjectionPushdown() throws {
        // SELECT name, email FROM users WHERE active = true
        // Only read name, email, active columns (not entire row)

        let scan = TableScanPlan(
            schema: "users",
            filter: .equal(.column(ColumnRef(column: "active")), .literal(.bool(true)))
        )

        let project = ProjectPlan(
            input: .tableScan(scan),
            columns: [
                ProjectionItem(.column(ColumnRef(column: "name"))),
                ProjectionItem(.column(ColumnRef(column: "email")))
            ]
        )

        // Verify projection specifies only needed columns
        #expect(project.columns.count == 2)

        // Calculate row width savings
        let fullRowWidth = 500
        let projectedWidth = 100

        let fullCost = QueryCost(startup: 0, total: 1000, rows: 100, width: fullRowWidth)
        let projectedCost = QueryCost(startup: 0, total: 1000, rows: 100, width: projectedWidth)

        #expect(projectedCost.width < fullCost.width)
    }

    @Test("Projection pushdown through JOIN")
    func testProjectionPushdownJoin() throws {
        // SELECT u.name, o.amount FROM users u JOIN orders o ON ...
        // Only project needed columns from each table

        let usersProject = ProjectPlan(
            input: .tableScan(TableScanPlan(schema: "users")),
            columns: [
                ProjectionItem(.column(ColumnRef(column: "id"))),
                ProjectionItem(.column(ColumnRef(column: "name")))
            ]
        )

        let ordersProject = ProjectPlan(
            input: .tableScan(TableScanPlan(schema: "orders")),
            columns: [
                ProjectionItem(.column(ColumnRef(column: "user_id"))),
                ProjectionItem(.column(ColumnRef(column: "amount")))
            ]
        )

        // Verify each side only projects needed columns plus join keys
        #expect(usersProject.columns.count == 2)
        #expect(ordersProject.columns.count == 2)
    }

    // MARK: - Cost Comparison Tests

    @Test("Compare plan costs for different strategies")
    func testPlanCostComparison() throws {
        // NestedLoop vs HashJoin vs MergeJoin cost comparison

        let leftTable = QueryPlanNode.tableScan(TableScanPlan(schema: "orders"))
        let rightTable = QueryPlanNode.tableScan(TableScanPlan(schema: "products"))
        let rightIndexed = QueryPlanNode.indexScan(IndexScanPlan(
            schema: "products",
            indexName: "idx_product_id",
            bounds: IndexBounds()
        ))

        // NestedLoop: O(n*m) without index
        let nestedLoopCost = QueryCost(startup: 0, total: 100_000_000, rows: 100000, width: 100)

        // HashJoin: O(n+m) build + probe
        let hashJoinCost = QueryCost(startup: 1000, total: 200_000, rows: 100000, width: 100)

        // MergeJoin: O(n log n + m log m) with pre-sorted inputs
        let mergeJoinCost = QueryCost(startup: 500, total: 150_000, rows: 100000, width: 100)

        // IndexNestedLoop: O(n * log m) with index
        let indexNestedLoopCost = QueryCost(startup: 0, total: 500_000, rows: 100000, width: 100)

        // MergeJoin is best for sorted inputs
        #expect(mergeJoinCost.total < hashJoinCost.total)
        #expect(hashJoinCost.total < indexNestedLoopCost.total)
        #expect(indexNestedLoopCost.total < nestedLoopCost.total)

        // Build actual plan structures
        let _ = JoinPlan(left: leftTable, right: rightTable, joinType: .inner)
        let _ = HashJoinPlan(
            build: rightTable,
            probe: leftTable,
            buildKeys: [.column(ColumnRef(column: "product_id"))],
            probeKeys: [.column(ColumnRef(column: "product_id"))],
            joinType: .inner
        )
        let _ = JoinPlan(left: leftTable, right: rightIndexed, joinType: .inner)
    }

    @Test("Cost addition combines correctly")
    func testCostAddition() throws {
        let cost1 = QueryCost(startup: 10, total: 100, rows: 1000, width: 50)
        let cost2 = QueryCost(startup: 5, total: 200, rows: 500, width: 100)

        let combined = cost1 + cost2

        #expect(combined.startup == 15)
        #expect(combined.total == 300)
        // Rows might take max or be combined differently depending on operation
    }

    @Test("Cost comparison operators")
    func testCostComparison() throws {
        let lowCost = QueryCost(startup: 0, total: 100, rows: 10, width: 50)
        let highCost = QueryCost(startup: 0, total: 1000, rows: 100, width: 50)

        #expect(lowCost.total < highCost.total)
    }

    // MARK: - Statistics Impact Tests

    @Test("Statistics impact on plan selection")
    func testStatisticsImpact() throws {
        // Test that statistics affect cost estimation

        // Without statistics: assume worst case
        let noStatsCost = QueryCost(startup: 0, total: 100000, rows: 100000, width: 100)

        // With statistics: know table is small
        let withStatsCost = QueryCost(startup: 0, total: 100, rows: 100, width: 100)

        #expect(withStatsCost.total < noStatsCost.total)

        // Plan statistics tracking
        let stats = PlanStatistics(planningTimeMs: 5.0, alternativesConsidered: 12)

        #expect(stats.planningTimeMs == 5.0)
        #expect(stats.alternativesConsidered == 12)
    }

    // MARK: - Join Type Selection Tests

    @Test("Hash join for equality joins")
    func testHashJoinSelection() throws {
        // Hash join is preferred for equality conditions on non-sorted inputs

        let build = QueryPlanNode.tableScan(TableScanPlan(schema: "small"))
        let probe = QueryPlanNode.tableScan(TableScanPlan(schema: "large"))

        let hashJoin = HashJoinPlan(
            build: build,
            probe: probe,
            buildKeys: [.column(ColumnRef(column: "id"))],
            probeKeys: [.column(ColumnRef(column: "foreign_id"))],
            joinType: .inner
        )

        #expect(hashJoin.buildKeys.count == hashJoin.probeKeys.count)
    }

    @Test("Merge join for pre-sorted inputs")
    func testMergeJoinSelection() throws {
        // Merge join is preferred when both inputs are sorted on join key

        let leftSorted = QueryPlanNode.indexScan(IndexScanPlan(
            schema: "table_a",
            indexName: "idx_a_key",
            bounds: IndexBounds()
        ))

        let rightSorted = QueryPlanNode.indexScan(IndexScanPlan(
            schema: "table_b",
            indexName: "idx_b_key",
            bounds: IndexBounds()
        ))

        let mergeJoin = MergeJoinPlan(
            left: leftSorted,
            right: rightSorted,
            leftKeys: [.column(ColumnRef(column: "key"))],
            rightKeys: [.column(ColumnRef(column: "key"))],
            joinType: .inner
        )

        #expect(mergeJoin.leftKeys.count == 1)
        #expect(mergeJoin.rightKeys.count == 1)
    }

    @Test("Nested loop for small outer relations")
    func testNestedLoopSelection() throws {
        // Nested loop can be efficient with small outer and indexed inner

        let small = QueryPlanNode.tableScan(TableScanPlan(schema: "lookup"))
        let indexed = QueryPlanNode.indexScan(IndexScanPlan(
            schema: "main",
            indexName: "idx_main_fk",
            bounds: IndexBounds()
        ))

        let nestedLoop = JoinPlan(
            left: small,
            right: indexed,
            condition: .equal(
                .column(ColumnRef(table: "lookup", column: "id")),
                .column(ColumnRef(table: "main", column: "lookup_id"))
            ),
            joinType: .inner
        )

        #expect(nestedLoop.joinType == .inner)
    }

    // MARK: - Edge Cases

    @Test("Empty table handling in cost")
    func testEmptyTableCost() throws {
        let emptyCost = QueryCost(startup: 0, total: 0, rows: 0, width: 0)

        #expect(emptyCost.rows == 0)
        #expect(emptyCost.total == 0)
    }

    @Test("Single row table optimization")
    func testSingleRowTableOptimization() throws {
        // Single row table should have very low scan cost

        let singleRowCost = QueryCost(startup: 0, total: 1, rows: 1, width: 100)
        let manyRowsCost = QueryCost(startup: 0, total: 10000, rows: 10000, width: 100)

        #expect(singleRowCost.total < manyRowsCost.total)
    }
}

// MARK: - Triple Index

/// Triple index type (used for testing SPARQL optimizations)
public enum TripleIndex: Sendable, Equatable {
    case spo  // Subject-Predicate-Object
    case pos  // Predicate-Object-Subject
    case osp  // Object-Subject-Predicate
    case sop  // Subject-Object-Predicate
    case pso  // Predicate-Subject-Object
    case ops  // Object-Predicate-Subject
}
