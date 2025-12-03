// IndexSkipScan.swift
// QueryPlanner - Index skip scan optimization

import Foundation
import Core
import FoundationDB

/// Index Skip Scan optimization
///
/// **What is Skip Scan?**
/// Skip scan allows using a composite index even when the leading column(s)
/// are not constrained by the query. It works by scanning multiple ranges,
/// one for each distinct value of the leading column(s).
///
/// **Example**:
/// ```
/// Index: (gender, age)
/// Query: WHERE age = 25
///
/// Without skip scan: Full table scan (leading column 'gender' not constrained)
/// With skip scan:
///   1. Scan where gender='M' AND age=25
///   2. Scan where gender='F' AND age=25
///   3. Scan where gender='N' AND age=25
///   → Union of 3 small index scans instead of full table scan
/// ```
///
/// **When is Skip Scan beneficial?**
/// - Leading column has LOW cardinality (few distinct values)
/// - Non-leading column has HIGH selectivity
/// - No single-column index on the queried column
///
/// **Cost Analysis**:
/// Skip scan cost ≈ N × (index seek + range scan)
/// where N = distinct values in leading column(s)
/// Table scan cost = total rows × row access cost
///
/// Skip scan wins when: N × range_cost < total_rows × row_cost
public struct IndexSkipScanAnalyzer<T: Persistable> {

    private let statistics: StatisticsProvider
    private let costModel: CostModel

    public init(statistics: StatisticsProvider, costModel: CostModel = .default) {
        self.statistics = statistics
        self.costModel = costModel
    }

    /// Analyze if skip scan is beneficial for this query and index
    public func analyze(
        index: IndexDescriptor,
        conditions: [any FieldConditionProtocol<T>],
        analysis: QueryAnalysis<T>
    ) -> SkipScanAnalysis {
        // Get index key fields
        let keyFields = index.keyPaths.map { T.fieldName(for: $0) }
        guard keyFields.count >= 2 else {
            return SkipScanAnalysis(
                isApplicable: false,
                reason: "Index must have at least 2 columns for skip scan"
            )
        }

        // Find which key positions have conditions
        let conditionFields = Set(conditions.map { $0.fieldName })
        var constrainedPositions: Set<Int> = []
        var unconstrainedLeadingPositions: [Int] = []

        for (i, field) in keyFields.enumerated() {
            if conditionFields.contains(field) {
                constrainedPositions.insert(i)
            } else if constrainedPositions.isEmpty || i < constrainedPositions.min()! {
                unconstrainedLeadingPositions.append(i)
            }
        }

        // Skip scan only applies when leading column(s) are unconstrained
        guard !unconstrainedLeadingPositions.isEmpty else {
            return SkipScanAnalysis(
                isApplicable: false,
                reason: "Leading columns are constrained, regular index scan is better"
            )
        }

        // Check that at least one non-leading column is constrained
        guard !constrainedPositions.isEmpty else {
            return SkipScanAnalysis(
                isApplicable: false,
                reason: "No conditions on index columns"
            )
        }

        // Estimate distinct values for leading unconstrained columns
        var totalDistinct = 1
        for pos in unconstrainedLeadingPositions {
            let field = keyFields[pos]
            let distinct = statistics.estimatedDistinctValues(field: field, type: T.self) ?? 100
            totalDistinct *= distinct
        }

        // Skip scan is only beneficial if leading column has low cardinality
        let maxDistinctForSkipScan = 50 // Configurable threshold

        if totalDistinct > maxDistinctForSkipScan {
            return SkipScanAnalysis(
                isApplicable: false,
                estimatedDistinctValues: totalDistinct,
                reason: "Too many distinct values in leading column(s): \(totalDistinct)"
            )
        }

        // Estimate costs
        let skipScanCost = estimateSkipScanCost(
            distinctValues: totalDistinct,
            conditions: conditions,
            analysis: analysis
        )

        let tableScanCost = estimateTableScanCost(analysis: analysis)

        let isBeneficial = skipScanCost < tableScanCost

        return SkipScanAnalysis(
            isApplicable: true,
            isBeneficial: isBeneficial,
            index: index,
            skippedFields: unconstrainedLeadingPositions.map { keyFields[$0] },
            constrainedFields: constrainedPositions.map { keyFields[$0] },
            estimatedDistinctValues: totalDistinct,
            estimatedSkipScanCost: skipScanCost,
            estimatedTableScanCost: tableScanCost,
            reason: isBeneficial
                ? "Skip scan is \(String(format: "%.1fx", tableScanCost / skipScanCost)) faster than table scan"
                : "Skip scan cost exceeds table scan"
        )
    }

    /// Estimate cost of skip scan
    private func estimateSkipScanCost(
        distinctValues: Int,
        conditions: [any FieldConditionProtocol<T>],
        analysis: QueryAnalysis<T>
    ) -> Double {
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))

        // Estimate selectivity of the constrained conditions
        var selectivity = 1.0
        for condition in conditions {
            let condSelectivity = estimateSelectivity(condition)
            selectivity *= condSelectivity
        }

        // Cost = (distinct values) × (seek cost + per-range scan cost)
        let rangeInitCost = Double(distinctValues) * costModel.rangeInitiationWeight
        let entriesPerRange = totalRows * selectivity / Double(distinctValues)
        let indexReadCost = Double(distinctValues) * entriesPerRange * costModel.indexReadWeight
        let recordFetchCost = totalRows * selectivity * costModel.recordFetchWeight

        return rangeInitCost + indexReadCost + recordFetchCost
    }

    /// Estimate cost of table scan
    private func estimateTableScanCost(analysis: QueryAnalysis<T>) -> Double {
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))
        return totalRows * costModel.recordFetchWeight + costModel.rangeInitiationWeight
    }

    /// Estimate selectivity for a condition
    private func estimateSelectivity(_ condition: any FieldConditionProtocol<T>) -> Double {
        if condition.isEquality {
            return statistics.equalitySelectivity(field: condition.fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity
        } else if condition.isRange {
            // Use range bounds if available
            if let bounds = condition.rangeBoundsAsTupleElements() {
                let rangeBound = RangeBound(
                    lower: bounds.lower.map { RangeBoundComponent(value: $0.0, inclusive: $0.1) },
                    upper: bounds.upper.map { RangeBoundComponent(value: $0.0, inclusive: $0.1) }
                )
                return statistics.rangeSelectivity(field: condition.fieldName, range: rangeBound, type: T.self)
                    ?? costModel.defaultRangeSelectivity
            }
            return costModel.defaultRangeSelectivity
        } else if condition.isNullCheck {
            return statistics.nullSelectivity(field: condition.fieldName, type: T.self)
                ?? costModel.defaultNullSelectivity
        } else {
            return costModel.defaultRangeSelectivity
        }
    }
}

// MARK: - Skip Scan Analysis Result

/// Result of skip scan analysis
public struct SkipScanAnalysis: Sendable {
    /// Whether skip scan is applicable
    public let isApplicable: Bool

    /// Whether skip scan is beneficial (cheaper than alternatives)
    public let isBeneficial: Bool

    /// The index analyzed
    public let index: IndexDescriptor?

    /// Fields that will be skipped
    public let skippedFields: [String]

    /// Fields with conditions
    public let constrainedFields: [String]

    /// Estimated distinct values in skipped fields
    public let estimatedDistinctValues: Int

    /// Estimated skip scan cost
    public let estimatedSkipScanCost: Double

    /// Estimated table scan cost (alternative)
    public let estimatedTableScanCost: Double

    /// Explanation
    public let reason: String

    public init(
        isApplicable: Bool,
        isBeneficial: Bool = false,
        index: IndexDescriptor? = nil,
        skippedFields: [String] = [],
        constrainedFields: [String] = [],
        estimatedDistinctValues: Int = 0,
        estimatedSkipScanCost: Double = 0,
        estimatedTableScanCost: Double = 0,
        reason: String
    ) {
        self.isApplicable = isApplicable
        self.isBeneficial = isBeneficial
        self.index = index
        self.skippedFields = skippedFields
        self.constrainedFields = constrainedFields
        self.estimatedDistinctValues = estimatedDistinctValues
        self.estimatedSkipScanCost = estimatedSkipScanCost
        self.estimatedTableScanCost = estimatedTableScanCost
        self.reason = reason
    }
}

// MARK: - Skip Scan Operator

/// Operator for skip scan execution
public struct SkipScanOperator<T: Persistable>: @unchecked Sendable {
    /// The index to use
    public let index: IndexDescriptor

    /// Values to iterate over for skipped fields
    /// Each inner array represents one distinct value combination
    public let skipValues: [[any TupleElement]]

    /// Base bounds (for constrained fields)
    public let baseBounds: IndexScanBounds

    /// Whether to scan in reverse
    public let reverse: Bool

    /// Conditions satisfied by this scan
    public let satisfiedConditions: [any FieldConditionProtocol<T>]

    /// Estimated total entries across all skips
    public let estimatedEntries: Int

    public init(
        index: IndexDescriptor,
        skipValues: [[any TupleElement]],
        baseBounds: IndexScanBounds,
        reverse: Bool = false,
        satisfiedConditions: [any FieldConditionProtocol<T>] = [],
        estimatedEntries: Int
    ) {
        self.index = index
        self.skipValues = skipValues
        self.baseBounds = baseBounds
        self.reverse = reverse
        self.satisfiedConditions = satisfiedConditions
        self.estimatedEntries = estimatedEntries
    }
}

// MARK: - Skip Value Provider

/// Protocol for providing distinct values for skip scan
public protocol SkipValueProvider: Sendable {
    /// Get distinct values for a field
    func getDistinctValues<T: Persistable>(
        field: String,
        type: T.Type,
        index: IndexDescriptor
    ) async throws -> [any TupleElement]
}

/// Default provider that uses sampling or index scan
public struct DefaultSkipValueProvider: SkipValueProvider {

    private let dataStore: any DataStore

    public init(dataStore: any DataStore) {
        self.dataStore = dataStore
    }

    public func getDistinctValues<T: Persistable>(
        field: String,
        type: T.Type,
        index: IndexDescriptor
    ) async throws -> [any TupleElement] {
        // Implementation would scan the index to find distinct values
        // For now, return empty array
        []
    }
}

// MARK: - Skip Scan Plan Creation
//
// Note: Skip scan plan creation should be integrated into PlanEnumerator directly
// rather than via extension, as it requires access to private members (statistics, costModel).
//
// Example integration in PlanEnumerator.enumerate():
//
// ```swift
// // Try skip scan plans for composite indexes
// for index in indexes where index.keyPaths.count >= 2 {
//     let analyzer = IndexSkipScanAnalyzer<T>(statistics: statistics, costModel: costModel)
//     let result = analyzer.analyze(index: index, conditions: analysis.fieldConditions, analysis: analysis)
//     if result.isApplicable && result.isBeneficial {
//         // Create SkipScanOperator
//     }
// }
// ```

// MARK: - Skip Scan Cost Estimation Helper

/// Helper for estimating skip scan costs
public struct SkipScanCostEstimator {

    /// Estimate cost for skip scan operator
    public static func estimateCost(
        distinctValues: Int,
        entriesPerValue: Double,
        requiresSort: Bool,
        costModel: CostModel
    ) -> PlanCost {
        let totalEntries = Double(distinctValues) * entriesPerValue
        let rangeInitCost = Double(distinctValues) * costModel.rangeInitiationWeight

        return PlanCost(
            indexReads: totalEntries,
            recordFetches: totalEntries,
            postFilterCount: 0,
            requiresSort: requiresSort,
            additionalCost: rangeInitCost,
            costModel: costModel
        )
    }
}

