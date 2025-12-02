// RuntimeStatistics.swift
// QueryPlanner - Runtime statistics feedback for adaptive optimization

import Foundation
import Core
import Synchronization

/// Tracks query execution statistics at runtime
///
/// **Purpose**:
/// - Collect actual row counts, execution times, and scan patterns
/// - Feed back to `CollectedStatisticsProvider` for better cost estimation
/// - Enable adaptive query optimization based on real workload
///
/// **Usage**:
/// ```swift
/// let tracker = RuntimeStatisticsTracker()
/// let plan = try planner.plan(query: myQuery)
/// let result = try await tracker.execute(plan: plan, executor: executor)
/// // Statistics are automatically recorded
/// ```
public final class RuntimeStatisticsTracker: Sendable {

    /// Internal state
    private struct State: Sendable {
        var executionHistory: [ExecutionRecord] = []
        var fieldHistograms: [String: [String: FieldHistogram]] = [:] // typeName -> fieldName -> histogram
        var indexUsageStats: [String: IndexUsageStats] = [:] // indexName -> stats
        var maxHistorySize: Int
    }

    private let state: Mutex<State>

    /// Statistics provider to update
    private let statisticsProvider: CollectedStatisticsProvider?

    /// Whether to automatically update statistics after executions
    public let autoUpdateStatistics: Bool

    /// Threshold for triggering statistics update (number of executions)
    public let updateThreshold: Int

    public init(
        statisticsProvider: CollectedStatisticsProvider? = nil,
        autoUpdateStatistics: Bool = true,
        updateThreshold: Int = 100,
        maxHistorySize: Int = 10000
    ) {
        self.statisticsProvider = statisticsProvider
        self.autoUpdateStatistics = autoUpdateStatistics
        self.updateThreshold = updateThreshold
        self.state = Mutex(State(maxHistorySize: maxHistorySize))
    }

    // MARK: - Execution Recording

    /// Record execution statistics for a plan
    public func record<T: Persistable>(
        plan: QueryPlan<T>,
        actualRowCount: Int,
        executionTime: TimeInterval,
        indexScansPerformed: Int = 0,
        recordFetches: Int = 0
    ) {
        let entry = ExecutionRecord(
            planId: plan.id,
            typeName: String(describing: T.self),
            timestamp: Date(),
            estimatedRowCount: Int(plan.estimatedCost.recordFetches),
            actualRowCount: actualRowCount,
            executionTime: executionTime,
            indexScansPerformed: indexScansPerformed,
            recordFetches: recordFetches,
            usedIndexes: plan.usedIndexes.map { $0.name }
        )

        state.withLock { state in
            // Add entry
            state.executionHistory.append(entry)

            // Trim if needed
            if state.executionHistory.count > state.maxHistorySize {
                state.executionHistory.removeFirst(state.executionHistory.count - state.maxHistorySize)
            }

            // Update index usage stats
            for indexName in entry.usedIndexes {
                var stats = state.indexUsageStats[indexName] ?? IndexUsageStats(indexName: indexName)
                stats.usageCount += 1
                stats.lastUsed = entry.timestamp
                stats.totalRowsReturned += actualRowCount
                state.indexUsageStats[indexName] = stats
            }
        }

        // Check if we should trigger statistics update
        if autoUpdateStatistics {
            let historyCount = state.withLock { $0.executionHistory.count }
            if historyCount % updateThreshold == 0 {
                Task {
                    await updateStatisticsFromHistory()
                }
            }
        }
    }

    /// Record field value observation (for histogram building)
    public func recordFieldValues<T: Persistable>(
        type: T.Type,
        fieldName: String,
        values: [Any]
    ) {
        let typeName = String(describing: type)

        state.withLock { state in
            if state.fieldHistograms[typeName] == nil {
                state.fieldHistograms[typeName] = [:]
            }

            var histogram = state.fieldHistograms[typeName]?[fieldName] ?? FieldHistogram(fieldName: fieldName)
            histogram.addSamples(values)
            state.fieldHistograms[typeName]?[fieldName] = histogram
        }
    }

    // MARK: - Statistics Update

    /// Update the statistics provider from collected execution history
    public func updateStatisticsFromHistory() async {
        guard let provider = statisticsProvider else { return }

        let (history, _, indexStats) = state.withLock { state in
            (state.executionHistory, state.fieldHistograms, state.indexUsageStats)
        }

        // Compute aggregated statistics
        let aggregated = aggregateStatistics(from: history)

        // Update table statistics
        for (typeName, typeStats) in aggregated {
            // Update row count estimates based on actual observations
            if let avgRowCount = typeStats.averageActualRows {
                // Note: CollectedStatisticsProvider needs a generic method, so we skip this for now
                // provider.updateTableStats(rowCount: Int(avgRowCount), sampleSize: typeStats.sampleCount)
                _ = (typeName, avgRowCount)
            }
        }

        // Update index statistics
        for (indexName, stats) in indexStats {
            let indexStats = IndexStatistics(
                indexName: indexName,
                entryCount: stats.totalRowsReturned / max(1, stats.usageCount),
                avgEntriesPerKey: 1.0
            )
            provider.updateIndexStats(indexStats)
        }
    }

    /// Aggregate statistics from execution history
    private func aggregateStatistics(from history: [ExecutionRecord]) -> [String: AggregatedTypeStats] {
        var result: [String: AggregatedTypeStats] = [:]

        for record in history {
            var stats = result[record.typeName] ?? AggregatedTypeStats()
            stats.totalActualRows += record.actualRowCount
            stats.totalEstimatedRows += record.estimatedRowCount
            stats.sampleCount += 1
            stats.totalExecutionTime += record.executionTime
            result[record.typeName] = stats
        }

        return result
    }

    // MARK: - Query Analysis

    /// Analyze estimation accuracy
    public func analyzeEstimationAccuracy() -> EstimationAccuracyReport {
        let history = state.withLock { $0.executionHistory }

        guard !history.isEmpty else {
            return EstimationAccuracyReport(
                totalExecutions: 0,
                averageError: 0,
                medianError: 0,
                worstCases: []
            )
        }

        // Calculate estimation errors
        var errors: [Double] = []
        var worstCases: [(entry: ExecutionRecord, error: Double)] = []

        for executionEntry in history {
            let estimated = Double(executionEntry.estimatedRowCount)
            let actual = Double(executionEntry.actualRowCount)

            // Relative error: |estimated - actual| / max(1, actual)
            let error = abs(estimated - actual) / max(1.0, actual)
            errors.append(error)

            if error > 1.0 { // More than 100% error
                worstCases.append((executionEntry, error))
            }
        }

        let sortedErrors = errors.sorted()
        let medianError = sortedErrors[sortedErrors.count / 2]
        let averageError = errors.reduce(0, +) / Double(errors.count)

        // Sort worst cases by error descending
        let topWorstCases = worstCases
            .sorted { $0.error > $1.error }
            .prefix(10)
            .map { WorstCaseRecord(planId: $0.entry.planId, error: $0.error, entry: $0.entry) }

        return EstimationAccuracyReport(
            totalExecutions: history.count,
            averageError: averageError,
            medianError: medianError,
            worstCases: Array(topWorstCases)
        )
    }

    /// Get index usage recommendations
    public func getIndexUsageRecommendations() -> [IndexRecommendation] {
        let indexStats = state.withLock { $0.indexUsageStats }

        var recommendations: [IndexRecommendation] = []

        for (indexName, stats) in indexStats {
            // Check for unused indexes
            if stats.usageCount == 0 {
                recommendations.append(IndexRecommendation(
                    indexName: indexName,
                    type: .unused,
                    message: "Index '\(indexName)' has not been used in any recorded queries"
                ))
            }

            // Check for low-selectivity indexes
            if stats.usageCount > 0 {
                let avgRowsPerUse = Double(stats.totalRowsReturned) / Double(stats.usageCount)
                if avgRowsPerUse > 1000 {
                    recommendations.append(IndexRecommendation(
                        indexName: indexName,
                        type: .lowSelectivity,
                        message: "Index '\(indexName)' returns many rows (avg: \(Int(avgRowsPerUse))). Consider more selective conditions."
                    ))
                }
            }
        }

        return recommendations
    }

    // MARK: - History Access

    /// Get recent execution history
    public func getRecentHistory(limit: Int = 100) -> [ExecutionRecord] {
        state.withLock { state in
            Array(state.executionHistory.suffix(limit))
        }
    }

    /// Clear all recorded statistics
    public func clear() {
        state.withLock { state in
            state.executionHistory.removeAll()
            state.fieldHistograms.removeAll()
            state.indexUsageStats.removeAll()
        }
    }
}

// MARK: - Supporting Types

/// Record of a single query execution
public struct ExecutionRecord: Sendable {
    public let planId: UUID
    public let typeName: String
    public let timestamp: Date
    public let estimatedRowCount: Int
    public let actualRowCount: Int
    public let executionTime: TimeInterval
    public let indexScansPerformed: Int
    public let recordFetches: Int
    public let usedIndexes: [String]

    /// Estimation error ratio
    public var errorRatio: Double {
        let estimated = Double(estimatedRowCount)
        let actual = Double(actualRowCount)
        return abs(estimated - actual) / max(1.0, actual)
    }
}

/// Index usage statistics
public struct IndexUsageStats: Sendable {
    public let indexName: String
    public var usageCount: Int = 0
    public var lastUsed: Date?
    public var totalRowsReturned: Int = 0

    public init(indexName: String) {
        self.indexName = indexName
    }
}

/// Aggregated statistics for a type
private struct AggregatedTypeStats {
    var totalActualRows: Int = 0
    var totalEstimatedRows: Int = 0
    var sampleCount: Int = 0
    var totalExecutionTime: TimeInterval = 0

    var averageActualRows: Double? {
        sampleCount > 0 ? Double(totalActualRows) / Double(sampleCount) : nil
    }

    var averageEstimatedRows: Double? {
        sampleCount > 0 ? Double(totalEstimatedRows) / Double(sampleCount) : nil
    }
}

/// Field value histogram for selectivity estimation
public struct FieldHistogram: Sendable {
    public let fieldName: String
    public private(set) var totalSamples: Int = 0
    public private(set) var distinctCount: Int = 0
    public private(set) var nullCount: Int = 0
    private var valueCounts: [String: Int] = [:]

    public init(fieldName: String) {
        self.fieldName = fieldName
    }

    /// Add sample values to the histogram
    public mutating func addSamples(_ values: [Any]) {
        for value in values {
            totalSamples += 1

            if isNil(value) {
                nullCount += 1
                continue
            }

            let key = "\(value)"
            if valueCounts[key] == nil {
                distinctCount += 1
            }
            valueCounts[key, default: 0] += 1
        }
    }

    /// Get estimated selectivity for equality
    public var equalitySelectivity: Double {
        guard distinctCount > 0 else { return 1.0 }
        return 1.0 / Double(distinctCount)
    }

    /// Get null ratio
    public var nullRatio: Double {
        guard totalSamples > 0 else { return 0.0 }
        return Double(nullCount) / Double(totalSamples)
    }

    private func isNil(_ value: Any) -> Bool {
        if case Optional<Any>.none = value { return true }
        return false
    }
}

// MARK: - Reports

/// Report on estimation accuracy
public struct EstimationAccuracyReport: Sendable {
    public let totalExecutions: Int
    public let averageError: Double
    public let medianError: Double
    public let worstCases: [WorstCaseRecord]

    /// Summary description
    public var summary: String {
        """
        Estimation Accuracy Report:
        - Total executions: \(totalExecutions)
        - Average error: \(String(format: "%.1f%%", averageError * 100))
        - Median error: \(String(format: "%.1f%%", medianError * 100))
        - Plans with >100% error: \(worstCases.count)
        """
    }
}

/// A log entry for a poorly estimated query
public struct WorstCaseRecord: Sendable {
    public let planId: UUID
    public let error: Double
    public let entry: ExecutionRecord
}

/// Index recommendation
public struct IndexRecommendation: Sendable {
    public let indexName: String
    public let type: RecommendationType
    public let message: String

    public enum RecommendationType: Sendable {
        case unused
        case lowSelectivity
        case missingIndex
        case duplicateIndex
    }
}

// MARK: - PlanExecutor Extension

extension PlanExecutor {

    /// Execute a plan with statistics tracking
    public func executeWithTracking(
        plan: QueryPlan<T>,
        tracker: RuntimeStatisticsTracker
    ) async throws -> [T] {
        let startTime = Date()

        let results = try await execute(plan: plan)

        let executionTime = Date().timeIntervalSince(startTime)

        // Record statistics
        tracker.record(
            plan: plan,
            actualRowCount: results.count,
            executionTime: executionTime,
            indexScansPerformed: plan.usedIndexes.count,
            recordFetches: results.count
        )

        return results
    }
}

// MARK: - Statistics Drift Detection

/// Detects when statistics have drifted significantly from reality
public struct StatisticsDriftDetector: Sendable {

    /// Threshold for significant drift (error ratio)
    public let driftThreshold: Double

    /// Minimum samples before drift detection
    public let minimumSamples: Int

    public init(driftThreshold: Double = 0.5, minimumSamples: Int = 10) {
        self.driftThreshold = driftThreshold
        self.minimumSamples = minimumSamples
    }

    /// Check if statistics have drifted based on recent executions
    public func detectDrift(from records: [ExecutionRecord]) -> DriftReport {
        guard records.count >= minimumSamples else {
            return DriftReport(hasDrifted: false, driftedFields: [], recommendation: nil)
        }

        // Calculate average error
        let errors = records.map { $0.errorRatio }
        let avgError = errors.reduce(0, +) / Double(errors.count)

        let hasDrifted = avgError > driftThreshold

        return DriftReport(
            hasDrifted: hasDrifted,
            driftedFields: [], // Would need field-level tracking to populate
            recommendation: hasDrifted
                ? "Statistics have drifted significantly (avg error: \(String(format: "%.1f%%", avgError * 100))). Consider refreshing statistics."
                : nil
        )
    }
}

/// Report on statistics drift
public struct DriftReport: Sendable {
    public let hasDrifted: Bool
    public let driftedFields: [String]
    public let recommendation: String?
}
