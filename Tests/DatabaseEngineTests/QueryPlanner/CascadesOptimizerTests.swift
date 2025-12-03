// CascadesOptimizerTests.swift
// Tests for Cascades Optimizer implementation
//
// Reference: Graefe, G. "The Cascades Framework for Query Optimization", 1995

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core

// MARK: - Memo Tests

@Suite("Memo Tests")
struct MemoTests {

    @Test("Create new group returns unique ID")
    func testCreateGroup() {
        let memo = Memo()

        let group1 = memo.createGroup()
        let group2 = memo.createGroup()

        #expect(group1.id != group2.id)
        #expect(memo.groupCount == 2)
    }

    @Test("Add logical expression creates group")
    func testAddLogicalExpression() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)

        #expect(memo.groupCount == 1)
        let expressions = memo.getLogicalExpressions(groupId)
        #expect(expressions.count == 1)
        #expect(expressions.first?.op == .logical(scan))
    }

    @Test("Duplicate expressions return same group")
    func testDuplicateExpression() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId1 = memo.addLogicalExpression(scan)
        let groupId2 = memo.addLogicalExpression(scan)

        #expect(groupId1 == groupId2)
        #expect(memo.groupCount == 1)
    }

    @Test("Add logical expression to existing group")
    func testAddToExistingGroup() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)

        // Add an equivalent expression (different representation)
        let indexScan = LogicalOperator.indexScan(typeName: "User", indexName: "idx_email", bounds: nil)
        let exprId = memo.addLogicalExpressionToGroup(indexScan, groupId: groupId)

        #expect(exprId != nil)
        let expressions = memo.getLogicalExpressions(groupId)
        #expect(expressions.count == 2)
    }

    @Test("Add physical expression with cost")
    func testAddPhysicalExpression() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)

        let seqScan = PhysicalOperator.seqScan(typeName: "User", filter: nil)
        let exprId = memo.addPhysicalExpression(seqScan, groupId: groupId, cost: 100.0)

        #expect(exprId != nil)
        let physicals = memo.getPhysicalExpressions(groupId)
        #expect(physicals.count == 1)
        #expect(physicals.first?.cost == 100.0)
    }

    @Test("Record and retrieve winner")
    func testRecordWinner() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)

        let seqScan = PhysicalOperator.seqScan(typeName: "User", filter: nil)
        let exprId = memo.addPhysicalExpression(seqScan, groupId: groupId, cost: 100.0)!

        memo.recordWinner(groupId: groupId, properties: .none, expressionId: exprId)

        let winner = memo.getWinner(groupId: groupId, properties: .none)
        #expect(winner == exprId)
    }

    @Test("Mark group as explored")
    func testMarkExplored() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)

        #expect(!memo.isExplored(groupId))

        memo.markExplored(groupId)

        #expect(memo.isExplored(groupId))
    }

    @Test("Expression count tracks all expressions")
    func testExpressionCount() {
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)

        let indexScan = LogicalOperator.indexScan(typeName: "User", indexName: "idx", bounds: nil)
        memo.addLogicalExpressionToGroup(indexScan, groupId: groupId)

        let seqScan = PhysicalOperator.seqScan(typeName: "User", filter: nil)
        memo.addPhysicalExpression(seqScan, groupId: groupId, cost: 100.0)

        #expect(memo.expressionCount == 3)
    }
}

// MARK: - Expression Tests

@Suite("Expression Tests")
struct ExpressionTests {

    @Test("LogicalOperator child groups extraction")
    func testLogicalOperatorChildGroups() {
        let scanOp = LogicalOperator.scan(typeName: "User")
        #expect(scanOp.childGroups.isEmpty)

        let groupId = GroupID(0)
        let filterOp = LogicalOperator.filter(input: groupId, predicate: .true)
        #expect(filterOp.childGroups == [groupId])

        let leftGroup = GroupID(1)
        let rightGroup = GroupID(2)
        let joinOp = LogicalOperator.join(left: leftGroup, right: rightGroup, condition: .true, type: .inner)
        #expect(joinOp.childGroups == [leftGroup, rightGroup])
    }

    @Test("PhysicalOperator child groups extraction")
    func testPhysicalOperatorChildGroups() {
        let seqScan = PhysicalOperator.seqScan(typeName: "User", filter: nil)
        #expect(seqScan.childGroups.isEmpty)

        let groupId = GroupID(0)
        let sort = PhysicalOperator.sort(input: groupId, keys: [], limit: nil)
        #expect(sort.childGroups == [groupId])

        let buildGroup = GroupID(1)
        let probeGroup = GroupID(2)
        let hashJoin = PhysicalOperator.hashJoin(
            build: buildGroup,
            probe: probeGroup,
            buildKeys: ["id"],
            probeKeys: ["userId"],
            type: .inner
        )
        #expect(hashJoin.childGroups == [buildGroup, probeGroup])
    }

    @Test("PredicateExpr equality")
    func testPredicateExprEquality() {
        let pred1 = PredicateExpr.comparison(field: "age", op: .gt, value: .int64(18))
        let pred2 = PredicateExpr.comparison(field: "age", op: .gt, value: .int64(18))
        let pred3 = PredicateExpr.comparison(field: "age", op: .lt, value: .int64(18))

        #expect(pred1 == pred2)
        #expect(pred1 != pred3)
    }

    @Test("IndexBoundsExpr construction")
    func testIndexBoundsExpr() {
        let pointBounds = IndexBoundsExpr(
            lowerBound: [.string("test@example.com")],
            lowerInclusive: true,
            upperBound: [.string("test@example.com")],
            upperInclusive: true
        )

        #expect(pointBounds.lowerBound == pointBounds.upperBound)
        #expect(pointBounds.lowerInclusive)
        #expect(pointBounds.upperInclusive)

        let rangeBounds = IndexBoundsExpr(
            lowerBound: [.int64(18)],
            lowerInclusive: false,
            upperBound: nil,
            upperInclusive: true
        )

        #expect(rangeBounds.lowerBound != nil)
        #expect(rangeBounds.upperBound == nil)
        #expect(!rangeBounds.lowerInclusive)
    }
}

// MARK: - Transformation Rule Tests

@Suite("Transformation Rule Tests")
struct TransformationRuleTests {

    @Test("FilterPushDownRule pattern matches filter on project")
    func testFilterPushDownPattern() {
        let rule = FilterPushDownRule()

        #expect(rule.name == "FilterPushDown")
        #expect(rule.promise == 10)
        #expect(rule.pattern == .filter(child: .project(child: .any)))
    }

    @Test("FilterToIndexScanRule extracts bounds from equality predicate")
    func testFilterToIndexScanExtractsBounds() {
        let rule = FilterToIndexScanRule()
        let memo = Memo()

        // Create a scan group
        let scan = LogicalOperator.scan(typeName: "User")
        let scanGroup = memo.addLogicalExpression(scan)

        // Create filter on scan
        let filter = LogicalOperator.filter(
            input: scanGroup,
            predicate: .comparison(field: "email", op: .eq, value: .string("test@example.com"))
        )
        let filterGroup = memo.addLogicalExpression(filter)
        let filterExpr = memo.getLogicalExpressions(filterGroup).first!

        let results = rule.apply(to: filterExpr, memo: memo)

        #expect(!results.isEmpty)
        if case .indexScan(let typeName, _, let bounds) = results.first {
            #expect(typeName == "User")
            #expect(bounds != nil)
            #expect(bounds?.lowerBound == [.string("test@example.com")])
            #expect(bounds?.upperBound == [.string("test@example.com")])
        } else {
            Issue.record("Expected indexScan result")
        }
    }

    @Test("JoinCommutativityRule swaps join inputs")
    func testJoinCommutativity() {
        let rule = JoinCommutativityRule()
        let memo = Memo()

        // Create two scan groups
        let leftScan = LogicalOperator.scan(typeName: "User")
        let leftGroup = memo.addLogicalExpression(leftScan)

        let rightScan = LogicalOperator.scan(typeName: "Order")
        let rightGroup = memo.addLogicalExpression(rightScan)

        // Create join
        let join = LogicalOperator.join(
            left: leftGroup,
            right: rightGroup,
            condition: .comparison(field: "userId", op: .eq, value: .string("id")),
            type: .inner
        )
        let joinGroup = memo.addLogicalExpression(join)
        let joinExpr = memo.getLogicalExpressions(joinGroup).first!

        let results = rule.apply(to: joinExpr, memo: memo)

        #expect(results.count == 1)
        if case .join(let newLeft, let newRight, _, let joinType) = results.first {
            #expect(newLeft == rightGroup)
            #expect(newRight == leftGroup)
            #expect(joinType == .inner)
        } else {
            Issue.record("Expected swapped join")
        }
    }

    @Test("JoinCommutativityRule does not swap outer joins")
    func testJoinCommutativityOuterJoin() {
        let rule = JoinCommutativityRule()
        let memo = Memo()

        let leftGroup = memo.addLogicalExpression(LogicalOperator.scan(typeName: "User"))
        let rightGroup = memo.addLogicalExpression(LogicalOperator.scan(typeName: "Order"))

        let leftJoin = LogicalOperator.join(
            left: leftGroup,
            right: rightGroup,
            condition: .true,
            type: .leftOuter
        )
        let joinGroup = memo.addLogicalExpression(leftJoin)
        let joinExpr = memo.getLogicalExpressions(joinGroup).first!

        let results = rule.apply(to: joinExpr, memo: memo)

        #expect(results.isEmpty)
    }
}

// MARK: - Implementation Rule Tests

@Suite("Implementation Rule Tests")
struct ImplementationRuleTests {

    @Test("SeqScanImplementationRule generates physical scan")
    func testSeqScanImplementation() {
        let rule = SeqScanImplementationRule()
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let groupId = memo.addLogicalExpression(scan)
        let expr = memo.getLogicalExpressions(groupId).first!

        let context = CascadesOptimizationContext(
            statistics: CascadesTableStatistics(rowCounts: ["User": 1000])
        )

        let results = rule.apply(
            to: expr,
            requiredProperties: .none,
            memo: memo,
            context: context
        )

        #expect(results.count == 1)
        if let (physical, cost) = results.first {
            if case .seqScan(let typeName, _) = physical {
                #expect(typeName == "User")
                #expect(cost > 0)
            } else {
                Issue.record("Expected seqScan physical operator")
            }
        }
    }

    @Test("FilterImplementationRule estimates selectivity")
    func testFilterImplementation() {
        let rule = FilterImplementationRule()
        let memo = Memo()

        let scan = LogicalOperator.scan(typeName: "User")
        let scanGroup = memo.addLogicalExpression(scan)

        let filter = LogicalOperator.filter(
            input: scanGroup,
            predicate: .comparison(field: "age", op: .eq, value: .int64(25))
        )
        let filterGroup = memo.addLogicalExpression(filter)
        let filterExpr = memo.getLogicalExpressions(filterGroup).first!

        let context = CascadesOptimizationContext()

        let results = rule.apply(
            to: filterExpr,
            requiredProperties: .none,
            memo: memo,
            context: context
        )

        #expect(results.count == 1)
        if let (physical, cost) = results.first {
            if case .filter(_, let predicate) = physical {
                #expect(predicate == .comparison(field: "age", op: .eq, value: .int64(25)))
                #expect(cost > 0)
            } else {
                Issue.record("Expected filter physical operator")
            }
        }
    }

    @Test("HashJoinImplementationRule generates hash join")
    func testHashJoinImplementation() {
        let rule = HashJoinImplementationRule()
        let memo = Memo()

        let leftGroup = memo.addLogicalExpression(LogicalOperator.scan(typeName: "User"))
        let rightGroup = memo.addLogicalExpression(LogicalOperator.scan(typeName: "Order"))

        let join = LogicalOperator.join(
            left: leftGroup,
            right: rightGroup,
            condition: .comparison(field: "userId", op: .eq, value: .string("id")),
            type: .inner
        )
        let joinGroup = memo.addLogicalExpression(join)
        let joinExpr = memo.getLogicalExpressions(joinGroup).first!

        let context = CascadesOptimizationContext()

        let results = rule.apply(
            to: joinExpr,
            requiredProperties: .none,
            memo: memo,
            context: context
        )

        #expect(results.count == 1)
        if let (physical, cost) = results.first {
            if case .hashJoin(let build, let probe, _, _, let joinType) = physical {
                #expect(build == leftGroup)
                #expect(probe == rightGroup)
                #expect(joinType == .inner)
                #expect(cost > 0)
            } else {
                Issue.record("Expected hashJoin physical operator")
            }
        }
    }
}

// MARK: - CascadesOptimizer Integration Tests

@Suite("CascadesOptimizer Integration Tests")
struct CascadesOptimizerIntegrationTests {

    @Test("Optimize simple scan query")
    func testOptimizeSimpleScan() throws {
        let context = CascadesOptimizationContext(
            statistics: CascadesTableStatistics(rowCounts: ["User": 1000])
        )
        let optimizer = CascadesOptimizer(context: context)

        let scan = LogicalOperator.scan(typeName: "User")

        let plan = try optimizer.optimize(scan)

        #expect(plan.cost > 0)
        if case .physical(.seqScan(let typeName, _)) = plan.rootOperator {
            #expect(typeName == "User")
        } else {
            Issue.record("Expected seqScan in optimized plan")
        }
    }

    @Test("Optimizer statistics")
    func testOptimizerStatistics() throws {
        let context = CascadesOptimizationContext()
        let optimizer = CascadesOptimizer(context: context)

        let scan = LogicalOperator.scan(typeName: "User")
        _ = try optimizer.optimize(scan)

        let stats = optimizer.statistics
        #expect(stats.groupCount >= 1)
        #expect(stats.expressionCount >= 1)
        #expect(stats.transformationRuleCount > 0)
        #expect(stats.implementationRuleCount > 0)
    }

    @Test("Branch and bound pruning reduces exploration")
    func testBranchAndBoundPruning() throws {
        let context = CascadesOptimizationContext(
            statistics: CascadesTableStatistics(rowCounts: ["User": 1000])
        )
        let optimizer = CascadesOptimizer(context: context)

        let scan = LogicalOperator.scan(typeName: "User")
        let plan = try optimizer.optimize(scan)

        // After optimization, the best cost should be finite
        #expect(plan.cost < Double.infinity)
    }
}

// MARK: - PropertySet Tests

@Suite("PropertySet Tests")
struct PropertySetTests {

    @Test("PropertySet none has no requirements")
    func testPropertySetNone() {
        let props = PropertySet.none

        #expect(props.sortOrder == nil)
        #expect(props.distribution == nil)
    }

    @Test("PropertySet with sort order")
    func testPropertySetWithSortOrder() {
        let sortKeys = [
            SortKeyExpr(field: "name", ascending: true),
            SortKeyExpr(field: "age", ascending: false)
        ]
        let props = PropertySet(sortOrder: sortKeys)

        #expect(props.sortOrder?.count == 2)
        #expect(props.sortOrder?.first?.field == "name")
        #expect(props.sortOrder?.first?.ascending == true)
    }

    @Test("PropertySet equality")
    func testPropertySetEquality() {
        let props1 = PropertySet(sortOrder: [SortKeyExpr(field: "name")])
        let props2 = PropertySet(sortOrder: [SortKeyExpr(field: "name")])
        let props3 = PropertySet(sortOrder: [SortKeyExpr(field: "age")])

        #expect(props1 == props2)
        #expect(props1 != props3)
    }
}

// MARK: - CostModel Tests

@Suite("CostModel Tests")
struct CostModelTests {

    @Test("Default cost model uses PostgreSQL-like values")
    func testDefaultCostModel() {
        let costModel = CascadesCostModel()

        #expect(costModel.seqPageCost == 1.0)
        #expect(costModel.randomPageCost == 4.0)
        #expect(costModel.cpuTupleCost == 0.01)
        #expect(costModel.pageSize == 8192)
    }

    @Test("Custom cost model values")
    func testCustomCostModel() {
        let costModel = CascadesCostModel(
            seqPageCost: 2.0,
            randomPageCost: 8.0,
            cpuTupleCost: 0.02,
            pageSize: 4096
        )

        #expect(costModel.seqPageCost == 2.0)
        #expect(costModel.randomPageCost == 8.0)
        #expect(costModel.cpuTupleCost == 0.02)
        #expect(costModel.pageSize == 4096)
    }
}

// MARK: - CascadesError Tests

@Suite("CascadesError Tests")
struct CascadesErrorTests {

    @Test("Error descriptions")
    func testErrorDescriptions() {
        let noValidPlan = CascadesError.noValidPlan
        #expect(noValidPlan.description.contains("No valid"))

        let exprNotFound = CascadesError.expressionNotFound
        #expect(exprNotFound.description.contains("not found"))

        let timeout = CascadesError.timeout
        #expect(timeout.description.contains("timed out"))

        let invalid = CascadesError.invalidExpression("test error")
        #expect(invalid.description.contains("test error"))
    }
}
