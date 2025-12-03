// CostEstimatorTests.swift
// Tests for CostEstimator and cost calculations

import Testing
import Foundation
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import Core

// Re-use QPTestUser from QueryPlannerTests.swift

@Suite("CostEstimator Tests")
struct CostEstimatorTests {

    // MARK: - CostModel Tests

    @Test("Default cost model has reasonable values")
    func testDefaultCostModelValues() {
        let model = CostModel.default

        #expect(model.indexReadWeight > 0)
        #expect(model.recordFetchWeight > 0)
        #expect(model.postFilterWeight > 0)
        #expect(model.sortWeight > 0)
        #expect(model.recordFetchWeight > model.indexReadWeight) // Record fetch should cost more
    }

    @Test("Custom cost model can be created")
    func testCustomCostModel() {
        let customModel = CostModel(
            indexReadWeight: 0.5,
            recordFetchWeight: 2.0,
            postFilterWeight: 0.1,
            sortWeight: 0.05,
            rangeInitiationWeight: 10.0
        )

        #expect(customModel.indexReadWeight == 0.5)
        #expect(customModel.recordFetchWeight == 2.0)
        #expect(customModel.rangeInitiationWeight == 10.0)
    }

    @Test("Cost model presets exist")
    func testCostModelPresets() {
        let _default = CostModel.default
        let favorIndexes = CostModel.favorIndexes
        let distributed = CostModel.distributed

        // favorIndexes should penalize record fetches more
        #expect(favorIndexes.recordFetchWeight > _default.recordFetchWeight)

        // distributed should have higher range initiation cost
        #expect(distributed.rangeInitiationWeight > _default.rangeInitiationWeight)
    }

    // MARK: - Cost Calculation Helper Tests

    @Test("Index cost calculation")
    func testIndexCostCalculation() {
        let model = CostModel.default

        let cost = model.indexCost(entries: 100, initiation: true)
        let costNoInit = model.indexCost(entries: 100, initiation: false)

        #expect(cost > costNoInit)
        #expect(cost == 100 * model.indexReadWeight + model.rangeInitiationWeight)
    }

    @Test("Fetch cost calculation")
    func testFetchCostCalculation() {
        let model = CostModel.default

        let cost = model.fetchCost(records: 50)

        #expect(cost == 50 * model.recordFetchWeight)
    }

    @Test("Sort cost calculation")
    func testSortCostCalculation() {
        let model = CostModel.default

        let cost = model.sortCost(records: 100)

        #expect(cost > 0)
        // Sort cost grows with n log n
        let costDouble = model.sortCost(records: 200)
        #expect(costDouble > cost * 2) // More than linear growth
    }

    // MARK: - QueryPlanner Integration Tests

    @Test("Index scan cheaper than table scan for selective query")
    func testIndexScanCheaperThanTableScan() throws {
        let statistics = CollectedStatisticsProvider()
        statistics.updateTableStats(for: QPTestUser.self, rowCount: 100000, sampleSize: 10000)

        // With index
        let plannerWithIndex = QueryPlanner<QPTestUser>(
            indexes: QPTestUser.indexDescriptors,
            statistics: statistics
        )

        // Without index (forces table scan)
        let plannerNoIndex = QueryPlanner<QPTestUser>(
            indexes: [],
            statistics: statistics
        )

        var query = Query<QPTestUser>()
        query = query.where(\QPTestUser.email == "unique@example.com")

        let indexPlan = try plannerWithIndex.plan(query: query)
        let tableScanPlan = try plannerNoIndex.plan(query: query)

        // Index plan should be cheaper for a selective equality query
        #expect(indexPlan.estimatedCost.totalCost < tableScanPlan.estimatedCost.totalCost)
    }

    @Test("Adding sort increases cost")
    func testSortIncreasesCost() throws {
        let statistics = CollectedStatisticsProvider()
        statistics.updateTableStats(for: QPTestUser.self, rowCount: 10000, sampleSize: 1000)

        let planner = QueryPlanner<QPTestUser>(
            indexes: [], // No indexes to force table scan
            statistics: statistics
        )

        var queryNoSort = Query<QPTestUser>()
        queryNoSort = queryNoSort.where(\QPTestUser.age > 18)

        var queryWithSort = Query<QPTestUser>()
        queryWithSort = queryWithSort.where(\QPTestUser.age > 18)
        queryWithSort = queryWithSort.orderBy(\QPTestUser.name)

        let planNoSort = try planner.plan(query: queryNoSort)
        let planWithSort = try planner.plan(query: queryWithSort)

        // Sort should add cost
        #expect(planWithSort.estimatedCost.totalCost >= planNoSort.estimatedCost.totalCost)
    }
}

// MARK: - StatisticsProvider Tests

@Suite("StatisticsProvider Tests")
struct StatisticsProviderTests {

    @Test("DefaultStatisticsProvider returns reasonable defaults")
    func testDefaultStatisticsProvider() {
        let provider = DefaultStatisticsProvider()

        let rowCount = provider.estimatedRowCount(for: QPTestUser.self)
        #expect(rowCount > 0)

        let distinctCount = provider.estimatedDistinctValues(field: "email", type: QPTestUser.self)
        #expect(distinctCount != nil)
        #expect(distinctCount! > 0)
    }

    @Test("CollectedStatisticsProvider stores and retrieves stats")
    func testCollectedStatisticsProvider() {
        let provider = CollectedStatisticsProvider()

        provider.updateTableStats(for: QPTestUser.self, rowCount: 5000, sampleSize: 500)

        let rowCount = provider.estimatedRowCount(for: QPTestUser.self)
        #expect(rowCount == 5000)
    }

    @Test("CollectedStatisticsProvider stores and retrieves index statistics")
    func testCollectedStatisticsIndexStats() {
        let provider = CollectedStatisticsProvider()

        let indexStats = IndexStatistics(
            indexName: "idx_email",
            entryCount: 10000,
            avgEntriesPerKey: 1.0
        )
        provider.updateIndexStats(indexStats)

        // Create an IndexDescriptor to query the stats
        let kind = ScalarIndexKind<QPTestUser>(fields: [\.email])
        let descriptor = IndexDescriptor(
            name: "idx_email",
            keyPaths: [\QPTestUser.email],
            kind: kind
        )

        // Verify the stored stats can be retrieved
        let entryCount = provider.estimatedIndexEntries(index: descriptor)
        #expect(entryCount == 10000)
    }

    @Test("CollectedStatisticsProvider uses fallback for unknown index")
    func testCollectedStatisticsIndexFallback() {
        let provider = CollectedStatisticsProvider(fallbackRowCount: 5000)

        // Query an index that hasn't been registered
        let kind = ScalarIndexKind<QPTestUser>(fields: [\.name])
        let descriptor = IndexDescriptor(
            name: "idx_unknown",
            keyPaths: [\QPTestUser.name],
            kind: kind
        )

        // Should return fallback value
        let entryCount = provider.estimatedIndexEntries(index: descriptor)
        #expect(entryCount == 5000)
    }
}
