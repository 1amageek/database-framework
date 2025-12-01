// AdaptiveOptimizer.swift
// QueryPlanner - Adaptive query optimization based on runtime feedback

import Foundation
import Core
import Synchronization

/// Adaptive query optimizer that adjusts plans based on runtime statistics
///
/// **Key Features**:
/// - Monitors plan execution performance
/// - Detects suboptimal plans via estimation drift
/// - Triggers re-planning when statistics diverge
/// - Supports plan replacement mid-execution (future)
///
/// **Architecture**:
/// ```
///     Query Execution
///           │
///           ▼
///   ┌──────────────────┐
///   │ AdaptiveOptimizer │
///   └────────┬─────────┘
///            │
///     ┌──────┼──────┐
///     ▼      ▼      ▼
///  Monitor  Detect  Replan
///  Metrics  Drift   Query
/// ```
public final class AdaptiveOptimizer<T: Persistable>: @unchecked Sendable {

    /// Internal state
    private struct State: Sendable {
        var planPerformance: [UUID: PlanPerformanceMetrics] = [:]
        var replanTriggers: [UUID: ReplanTrigger] = [:]
        var adaptationHistory: [AdaptationEvent] = []
    }

    private let state: Mutex<State>

    /// The underlying query planner
    private let planner: QueryPlanner<T>

    /// Statistics tracker for runtime feedback
    private let statisticsTracker: RuntimeStatisticsTracker

    /// Plan cache
    private let planCache: PlanCache?

    /// Configuration
    public let configuration: AdaptiveConfiguration

    /// Drift detector
    private let driftDetector: StatisticsDriftDetector

    public init(
        planner: QueryPlanner<T>,
        statisticsTracker: RuntimeStatisticsTracker,
        planCache: PlanCache? = nil,
        configuration: AdaptiveConfiguration = .default
    ) {
        self.planner = planner
        self.statisticsTracker = statisticsTracker
        self.planCache = planCache
        self.configuration = configuration
        self.driftDetector = StatisticsDriftDetector(
            driftThreshold: configuration.driftThreshold,
            minimumSamples: configuration.minimumSamplesForAdaptation
        )
        self.state = Mutex(State())
    }

    // MARK: - Adaptive Planning

    /// Plan a query with adaptive optimization support
    ///
    /// Returns an adaptive plan that can be monitored and re-optimized.
    public func plan(query: Query<T>) throws -> AdaptivePlan<T> {
        let basePlan = try planner.plan(query: query)

        let adaptivePlan = AdaptivePlan(
            id: UUID(),
            currentPlan: basePlan,
            query: query,
            version: 1,
            createdAt: Date()
        )

        // Initialize performance tracking
        state.withLock { state in
            state.planPerformance[adaptivePlan.id] = PlanPerformanceMetrics(planId: adaptivePlan.id)
        }

        return adaptivePlan
    }

    /// Record execution result and check for adaptation
    public func recordExecution(
        plan: AdaptivePlan<T>,
        actualRowCount: Int,
        executionTime: TimeInterval
    ) throws -> AdaptationResult<T> {
        // Record metrics
        statisticsTracker.record(
            plan: plan.currentPlan,
            actualRowCount: actualRowCount,
            executionTime: executionTime
        )

        // Update performance metrics
        var needsReplan = false

        state.withLock { state in
            var metrics = state.planPerformance[plan.id] ?? PlanPerformanceMetrics(planId: plan.id)
            metrics.recordExecution(
                estimatedRows: Int(plan.currentPlan.estimatedCost.recordFetches),
                actualRows: actualRowCount,
                executionTime: executionTime
            )
            state.planPerformance[plan.id] = metrics

            // Check if replan is needed
            if metrics.executionCount >= configuration.minimumSamplesForAdaptation {
                if metrics.averageEstimationError > configuration.driftThreshold {
                    needsReplan = true
                    state.replanTriggers[plan.id] = ReplanTrigger(
                        reason: .estimationDrift,
                        detectedAt: Date(),
                        metrics: metrics
                    )
                }
            }
        }

        // Trigger replan if needed
        if needsReplan && configuration.autoReplan {
            return try triggerReplan(for: plan, reason: .estimationDrift)
        }

        return AdaptationResult(action: .noChange, plan: plan)
    }

    /// Force a replan for an adaptive plan
    public func triggerReplan(
        for plan: AdaptivePlan<T>,
        reason: ReplanReason
    ) throws -> AdaptationResult<T> {
        // Re-plan the query
        let newBasePlan = try planner.plan(query: plan.query)

        // Create new adaptive plan
        let newPlan = AdaptivePlan(
            id: plan.id, // Keep same ID for tracking
            currentPlan: newBasePlan,
            query: plan.query,
            version: plan.version + 1,
            createdAt: Date()
        )

        // Record adaptation event
        let event = AdaptationEvent(
            planId: plan.id,
            timestamp: Date(),
            reason: reason,
            previousPlanCost: plan.currentPlan.estimatedCost.totalCost,
            newPlanCost: newBasePlan.estimatedCost.totalCost
        )

        state.withLock { state in
            state.adaptationHistory.append(event)

            // Reset performance metrics for new plan
            state.planPerformance[plan.id] = PlanPerformanceMetrics(planId: plan.id)
            state.replanTriggers.removeValue(forKey: plan.id)
        }

        // Update cache if available
        if let cache = planCache {
            let fingerprintBuilder = QueryFingerprintBuilder<T>()
            let fingerprint = fingerprintBuilder.build(from: plan.query)
            cache.remove(fingerprint: fingerprint)
        }

        return AdaptationResult(action: .replanned(reason: reason), plan: newPlan)
    }

    // MARK: - Monitoring

    /// Get performance metrics for a plan
    public func getMetrics(for planId: UUID) -> PlanPerformanceMetrics? {
        state.withLock { $0.planPerformance[planId] }
    }

    /// Get all pending replan triggers
    public func getPendingReplans() -> [ReplanTrigger] {
        state.withLock { Array($0.replanTriggers.values) }
    }

    /// Get adaptation history
    public func getAdaptationHistory(limit: Int = 100) -> [AdaptationEvent] {
        state.withLock { Array($0.adaptationHistory.suffix(limit)) }
    }

    /// Generate an adaptation report
    public func generateReport() -> AdaptationReport {
        let (performance, history, triggers) = state.withLock { state in
            (state.planPerformance, state.adaptationHistory, state.replanTriggers)
        }

        let totalPlans = performance.count
        let plansNeedingReplan = triggers.count
        let totalAdaptations = history.count

        // Calculate average improvement from adaptations
        var improvements: [Double] = []
        for event in history {
            if event.previousPlanCost > 0 {
                let improvement = (event.previousPlanCost - event.newPlanCost) / event.previousPlanCost
                improvements.append(improvement)
            }
        }
        let avgImprovement = improvements.isEmpty ? 0 : improvements.reduce(0, +) / Double(improvements.count)

        return AdaptationReport(
            totalPlans: totalPlans,
            plansNeedingReplan: plansNeedingReplan,
            totalAdaptations: totalAdaptations,
            averageImprovement: avgImprovement,
            recentEvents: Array(history.suffix(10))
        )
    }

    // MARK: - Manual Control

    /// Check if a specific plan needs replanning
    public func checkPlan(_ plan: AdaptivePlan<T>) -> PlanHealthStatus {
        guard let metrics = state.withLock({ $0.planPerformance[plan.id] }) else {
            return .unknown
        }

        if metrics.executionCount < configuration.minimumSamplesForAdaptation {
            return .insufficientData(samples: metrics.executionCount)
        }

        if metrics.averageEstimationError > configuration.driftThreshold {
            return .degraded(error: metrics.averageEstimationError)
        }

        if metrics.averageEstimationError > configuration.driftThreshold * 0.5 {
            return .warning(error: metrics.averageEstimationError)
        }

        return .healthy
    }

    /// Clear all tracking state
    public func reset() {
        state.withLock { state in
            state.planPerformance.removeAll()
            state.replanTriggers.removeAll()
            state.adaptationHistory.removeAll()
        }
    }
}

// MARK: - Configuration

/// Configuration for adaptive optimization
public struct AdaptiveConfiguration: Sendable {
    /// Threshold for estimation drift to trigger replanning (e.g., 0.5 = 50% error)
    public let driftThreshold: Double

    /// Minimum executions before considering adaptation
    public let minimumSamplesForAdaptation: Int

    /// Whether to automatically replan when drift is detected
    public let autoReplan: Bool

    /// Maximum number of replans per plan
    public let maxReplansPerPlan: Int

    /// Cooldown period between replans (seconds)
    public let replanCooldown: TimeInterval

    public init(
        driftThreshold: Double = 0.5,
        minimumSamplesForAdaptation: Int = 10,
        autoReplan: Bool = true,
        maxReplansPerPlan: Int = 5,
        replanCooldown: TimeInterval = 60
    ) {
        self.driftThreshold = driftThreshold
        self.minimumSamplesForAdaptation = minimumSamplesForAdaptation
        self.autoReplan = autoReplan
        self.maxReplansPerPlan = maxReplansPerPlan
        self.replanCooldown = replanCooldown
    }

    public static let `default` = AdaptiveConfiguration()

    public static let aggressive = AdaptiveConfiguration(
        driftThreshold: 0.3,
        minimumSamplesForAdaptation: 5,
        autoReplan: true
    )

    public static let conservative = AdaptiveConfiguration(
        driftThreshold: 1.0,
        minimumSamplesForAdaptation: 50,
        autoReplan: false
    )
}

// MARK: - Adaptive Plan

/// A query plan with adaptive optimization support
public struct AdaptivePlan<T: Persistable>: @unchecked Sendable {
    /// Unique identifier
    public let id: UUID

    /// Current execution plan
    public let currentPlan: QueryPlan<T>

    /// Original query (for replanning)
    public let query: Query<T>

    /// Plan version (increments on each adaptation)
    public let version: Int

    /// When this plan version was created
    public let createdAt: Date
}

// MARK: - Performance Metrics

/// Performance metrics for a plan
public struct PlanPerformanceMetrics: Sendable {
    public let planId: UUID
    public private(set) var executionCount: Int = 0
    public private(set) var totalEstimatedRows: Int = 0
    public private(set) var totalActualRows: Int = 0
    public private(set) var totalExecutionTime: TimeInterval = 0
    public private(set) var lastExecutionTime: Date?
    public private(set) var errorHistory: [Double] = []

    public init(planId: UUID) {
        self.planId = planId
    }

    /// Record an execution
    public mutating func recordExecution(
        estimatedRows: Int,
        actualRows: Int,
        executionTime: TimeInterval
    ) {
        executionCount += 1
        totalEstimatedRows += estimatedRows
        totalActualRows += actualRows
        totalExecutionTime += executionTime
        lastExecutionTime = Date()

        // Calculate error ratio
        let error = Double(abs(estimatedRows - actualRows)) / max(1.0, Double(actualRows))
        errorHistory.append(error)

        // Keep only last 100 errors
        if errorHistory.count > 100 {
            errorHistory.removeFirst()
        }
    }

    /// Average estimation error (ratio)
    public var averageEstimationError: Double {
        guard !errorHistory.isEmpty else { return 0 }
        return errorHistory.reduce(0, +) / Double(errorHistory.count)
    }

    /// Average execution time
    public var averageExecutionTime: TimeInterval {
        guard executionCount > 0 else { return 0 }
        return totalExecutionTime / Double(executionCount)
    }
}

// MARK: - Replan Types

/// Reason for replanning
public enum ReplanReason: Sendable {
    case estimationDrift
    case indexDropped
    case schemaChanged
    case manualRequest
    case performanceDegraded
}

/// Trigger for replanning
public struct ReplanTrigger: Sendable {
    public let reason: ReplanReason
    public let detectedAt: Date
    public let metrics: PlanPerformanceMetrics
}

/// Result of adaptation check
public struct AdaptationResult<T: Persistable>: @unchecked Sendable {
    public let action: AdaptationAction
    public let plan: AdaptivePlan<T>
}

/// Action taken by adaptive optimizer
public enum AdaptationAction: Sendable {
    case noChange
    case replanned(reason: ReplanReason)
    case deferred(reason: String)
}

/// Health status of a plan
public enum PlanHealthStatus: Sendable {
    case healthy
    case warning(error: Double)
    case degraded(error: Double)
    case insufficientData(samples: Int)
    case unknown
}

// MARK: - Events and Reports

/// Record of an adaptation event
public struct AdaptationEvent: Sendable {
    public let planId: UUID
    public let timestamp: Date
    public let reason: ReplanReason
    public let previousPlanCost: Double
    public let newPlanCost: Double

    public var improvement: Double {
        guard previousPlanCost > 0 else { return 0 }
        return (previousPlanCost - newPlanCost) / previousPlanCost
    }
}

/// Summary report of adaptive optimization
public struct AdaptationReport: Sendable {
    public let totalPlans: Int
    public let plansNeedingReplan: Int
    public let totalAdaptations: Int
    public let averageImprovement: Double
    public let recentEvents: [AdaptationEvent]

    public var summary: String {
        """
        Adaptive Optimization Report:
        - Total plans tracked: \(totalPlans)
        - Plans needing replan: \(plansNeedingReplan)
        - Total adaptations: \(totalAdaptations)
        - Average improvement: \(String(format: "%.1f%%", averageImprovement * 100))
        """
    }
}

// MARK: - Progressive Optimization

/// Progressive optimizer for long-running queries
///
/// Monitors execution progress and can switch plans mid-execution
/// if better options become available.
public final class ProgressiveOptimizer<T: Persistable>: @unchecked Sendable {

    private struct State: Sendable {
        var currentPhase: ExecutionPhase = .initial
        var rowsProcessed: Int = 0
        var estimatedRemaining: Int = 0
    }

    private let state: Mutex<State>
    private let optimizer: AdaptiveOptimizer<T>

    public init(optimizer: AdaptiveOptimizer<T>) {
        self.optimizer = optimizer
        self.state = Mutex(State())
    }

    /// Report progress during execution
    public func reportProgress(rowsProcessed: Int, estimatedTotal: Int) {
        state.withLock { state in
            state.rowsProcessed = rowsProcessed
            state.estimatedRemaining = estimatedTotal - rowsProcessed

            // Update phase based on progress
            let progress = Double(rowsProcessed) / max(1.0, Double(estimatedTotal))
            if progress < 0.1 {
                state.currentPhase = .initial
            } else if progress < 0.5 {
                state.currentPhase = .scanning
            } else {
                state.currentPhase = .finishing
            }
        }
    }

    /// Check if plan switch is recommended
    public func shouldSwitchPlan() -> Bool {
        let (phase, processed, remaining) = state.withLock { state in
            (state.currentPhase, state.rowsProcessed, state.estimatedRemaining)
        }

        // Only consider switching in initial phase
        guard phase == .initial else { return false }

        // If we've processed many more rows than expected, consider switching
        return processed > remaining * 2
    }

    public enum ExecutionPhase: Sendable {
        case initial
        case scanning
        case finishing
    }
}
