// PlanComplexityLimitTests.swift
// Tests for PlanComplexityLimit and QueryPlannerConfiguration

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("PlanComplexityLimit Tests")
struct PlanComplexityLimitTests {

    // MARK: - QueryPlannerConfiguration Tests

    @Test func defaultConfiguration() {
        let config = QueryPlannerConfiguration.default

        #expect(config.complexityThreshold == 1000)
        #expect(config.maxPlanEnumerations == 100)
        #expect(config.maxRuleApplications == 10000)
        #expect(config.timeoutSeconds == 30.0)
        #expect(config.enableCostBasedOptimization)
        #expect(config.enablePlanCaching)
        #expect(config.enableIndexIntersection)
        #expect(config.enableIndexUnion)
        #expect(config.enableInPredicateOptimization)
    }

    @Test func conservativeConfiguration() {
        let config = QueryPlannerConfiguration.conservative

        #expect(config.complexityThreshold == 100)
        #expect(config.maxPlanEnumerations == 20)
        #expect(config.timeoutSeconds == 5.0)
        #expect(!config.enableIndexIntersection)
    }

    @Test func aggressiveConfiguration() {
        let config = QueryPlannerConfiguration.aggressive

        #expect(config.complexityThreshold == 10000)
        #expect(config.maxPlanEnumerations == 1000)
        #expect(config.timeoutSeconds == 120.0)
    }

    @Test func minimalConfiguration() {
        let config = QueryPlannerConfiguration.minimal

        #expect(config.complexityThreshold == 50)
        #expect(config.maxPlanEnumerations == 5)
        #expect(!config.enableCostBasedOptimization)
        #expect(!config.enablePlanCaching)
        #expect(!config.enableInPredicateOptimization)
    }

    @Test func customConfiguration() {
        let config = QueryPlannerConfiguration(
            complexityThreshold: 500,
            maxPlanEnumerations: 50,
            maxRuleApplications: 5000,
            timeoutSeconds: 10.0
        )

        #expect(config.complexityThreshold == 500)
        #expect(config.maxPlanEnumerations == 50)
        #expect(config.maxRuleApplications == 5000)
        #expect(config.timeoutSeconds == 10.0)
    }

    @Test func configurationEquality() {
        let config1 = QueryPlannerConfiguration.default
        let config2 = QueryPlannerConfiguration.default

        #expect(config1 == config2)
    }

    // MARK: - ComplexityBreakdown Tests

    @Test func complexityBreakdownInitialState() {
        let breakdown = ComplexityBreakdown()

        #expect(breakdown.totalComplexity == 0)
        #expect(breakdown.tableScanCount == 0)
        #expect(breakdown.indexScanCount == 0)
        #expect(breakdown.filterCount == 0)
        #expect(breakdown.totalOperators == 0)
    }

    @Test func complexityBreakdownTotalOperators() {
        var breakdown = ComplexityBreakdown()
        breakdown.tableScanCount = 1
        breakdown.indexScanCount = 2
        breakdown.filterCount = 3
        breakdown.sortCount = 1

        #expect(breakdown.totalOperators == 7)
    }

    // MARK: - PlanningStateTracker Tests

    @Test func planningStateTrackerRecordsPlanEnumerations() throws {
        let config = QueryPlannerConfiguration(maxPlanEnumerations: 10)
        let tracker = PlanningStateTracker(configuration: config)

        for _ in 0..<10 {
            try tracker.recordPlanEnumeration()
        }

        #expect(tracker.statistics.planEnumerations == 10)
    }

    @Test func planningStateTrackerThrowsOnExcessivePlanEnumerations() throws {
        let config = QueryPlannerConfiguration(maxPlanEnumerations: 5)
        let tracker = PlanningStateTracker(configuration: config)

        do {
            for _ in 0..<10 {
                try tracker.recordPlanEnumeration()
            }
            Issue.record("Expected error to be thrown")
        } catch let error as PlanningLimitExceededError {
            if case .planEnumerationsExceeded(let count, let limit) = error {
                #expect(count == 6)
                #expect(limit == 5)
            } else {
                Issue.record("Wrong error case")
            }
        }
    }

    @Test func planningStateTrackerRecordsRuleApplications() throws {
        let config = QueryPlannerConfiguration(maxRuleApplications: 100)
        let tracker = PlanningStateTracker(configuration: config)

        for _ in 0..<50 {
            try tracker.recordRuleApplication()
        }

        #expect(tracker.statistics.ruleApplications == 50)
    }

    @Test func planningStateTrackerThrowsOnExcessiveRuleApplications() throws {
        let config = QueryPlannerConfiguration(maxRuleApplications: 5)
        let tracker = PlanningStateTracker(configuration: config)

        do {
            for _ in 0..<10 {
                try tracker.recordRuleApplication()
            }
            Issue.record("Expected error to be thrown")
        } catch let error as PlanningLimitExceededError {
            if case .ruleApplicationsExceeded(let count, let limit) = error {
                #expect(count == 6)
                #expect(limit == 5)
            } else {
                Issue.record("Wrong error case")
            }
        }
    }

    @Test func planningStateTrackerTrackesElapsedTime() {
        let config = QueryPlannerConfiguration.default
        let tracker = PlanningStateTracker(configuration: config)

        // Just verify that elapsed time is tracked
        let stats = tracker.statistics
        #expect(stats.elapsedSeconds >= 0)
    }

    // MARK: - PlanComplexityExceededError Tests

    @Test func planComplexityExceededErrorDescription() {
        let error = PlanComplexityExceededError(
            complexity: 2000,
            threshold: 1000,
            planDescription: "Union(5 children)"
        )

        let description = error.description
        #expect(description.contains("2000"))
        #expect(description.contains("1000"))
        #expect(description.contains("Union"))
    }

    @Test func planComplexityExceededErrorSuggestions() {
        let error = PlanComplexityExceededError(
            complexity: 2000,
            threshold: 1000,
            planDescription: "Union(5 children)"
        )

        let suggestions = error.suggestions
        #expect(suggestions.count >= 2)
        #expect(suggestions.contains { $0.contains("OR conditions") })
    }

    @Test func planComplexityExceededErrorSuggestsBreakingQueryWhenVeryComplex() {
        let error = PlanComplexityExceededError(
            complexity: 20000,
            threshold: 1000,
            planDescription: "Complex plan"
        )

        let suggestions = error.suggestions
        #expect(suggestions.contains { $0.contains("breaking the query") })
    }

    // MARK: - PlanningLimitExceededError Tests

    @Test func planEnumerationsExceededDescription() {
        let error = PlanningLimitExceededError.planEnumerationsExceeded(count: 150, limit: 100)

        let description = error.description
        #expect(description.contains("150"))
        #expect(description.contains("100"))
        #expect(description.contains("Plan enumerations"))
    }

    @Test func ruleApplicationsExceededDescription() {
        let error = PlanningLimitExceededError.ruleApplicationsExceeded(count: 15000, limit: 10000)

        let description = error.description
        #expect(description.contains("15000"))
        #expect(description.contains("10000"))
        #expect(description.contains("Rule applications"))
    }

    @Test func timeoutExceededDescription() {
        let error = PlanningLimitExceededError.timeoutExceeded(elapsed: 35.5, limit: 30.0)

        let description = error.description
        #expect(description.contains("35"))
        #expect(description.contains("30"))
        #expect(description.contains("timeout"))
    }

    // MARK: - PlanningStatistics Tests

    @Test func planningStatisticsCreation() {
        let stats = PlanningStatistics(
            planEnumerations: 50,
            ruleApplications: 500,
            elapsedSeconds: 2.5
        )

        #expect(stats.planEnumerations == 50)
        #expect(stats.ruleApplications == 500)
        #expect(stats.elapsedSeconds == 2.5)
    }

    // MARK: - CascadesError Extension Tests

    @Test func cascadesErrorComplexityExceeded() {
        let error = CascadesError.complexityExceeded(complexity: 2000, threshold: 1000)

        if case .invalidExpression(let message) = error {
            #expect(message.contains("2000"))
            #expect(message.contains("1000"))
        } else {
            Issue.record("Wrong error case")
        }
    }

    @Test func cascadesErrorPlanEnumerationLimit() {
        let error = CascadesError.planEnumerationLimit(count: 150, limit: 100)

        if case .invalidExpression(let message) = error {
            #expect(message.contains("150"))
            #expect(message.contains("100"))
        } else {
            Issue.record("Wrong error case")
        }
    }
}
