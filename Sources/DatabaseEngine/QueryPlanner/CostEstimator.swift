// CostEstimator.swift
// QueryPlanner - Cost estimation for query plans

import Foundation
import Core

/// Estimates the cost of executing a query plan
public struct CostEstimator<T: Persistable> {

    private let statistics: StatisticsProvider
    private let costModel: CostModel

    public init(statistics: StatisticsProvider, costModel: CostModel = .default) {
        self.statistics = statistics
        self.costModel = costModel
    }

    /// Estimate the cost of a plan
    public func estimate(
        plan: PlanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        switch plan {
        case .tableScan(let op):
            return estimateTableScan(op, analysis: analysis)

        case .indexScan(let op):
            return estimateIndexScan(op, analysis: analysis)

        case .indexSeek(let op):
            return estimateIndexSeek(op, analysis: analysis)

        case .indexOnlyScan(let op):
            return estimateIndexOnlyScan(op, analysis: analysis)

        case .union(let op):
            return estimateUnion(op, analysis: analysis)

        case .intersection(let op):
            return estimateIntersection(op, analysis: analysis)

        case .filter(let op):
            return estimateFilter(op, analysis: analysis)

        case .sort(let op):
            return estimateSort(op, analysis: analysis)

        case .limit(let op):
            return estimateLimit(op, analysis: analysis)

        case .fullTextScan(let op):
            return estimateFullTextScan(op, analysis: analysis)

        case .vectorSearch(let op):
            return estimateVectorSearch(op, analysis: analysis)

        case .spatialScan(let op):
            return estimateSpatialScan(op, analysis: analysis)

        case .aggregation(let op):
            return estimateAggregation(op, analysis: analysis)

        case .project(let op):
            return estimate(plan: op.input, analysis: analysis)
        }
    }

    // MARK: - Table Scan

    private func estimateTableScan(
        _ op: TableScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))

        // selectivity = fraction of rows that PASS the filter (e.g., 0.1 = 10% pass)
        let selectivity = estimatePredicateSelectivity(analysis.originalPredicate)

        // For table scan, we read all rows
        let recordFetches = totalRows

        // Post-filter cost represents rows that are processed by the filter but DON'T pass
        // These rows consume CPU for evaluation but don't contribute to results
        // Higher cost when more rows are filtered out (1 - selectivity)
        let postFilterCount = totalRows * (1 - selectivity)

        return PlanCost(
            indexReads: 0,
            recordFetches: recordFetches,
            postFilterCount: postFilterCount,
            requiresSort: !analysis.sortRequirements.isEmpty,
            costModel: costModel
        )
    }

    // MARK: - Index Scan

    private func estimateIndexScan(
        _ op: IndexScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let indexEntries = Double(op.estimatedEntries)

        // Calculate how much post-filtering is needed
        let satisfiedSelectivity = op.satisfiedConditions.reduce(1.0) { acc, cond in
            acc * estimateConditionSelectivity(cond)
        }
        let totalSelectivity = estimatePredicateSelectivity(analysis.originalPredicate)
        let postFilterRatio = totalSelectivity > 0 ? min(1.0, max(0, 1 - (totalSelectivity / satisfiedSelectivity))) : 0

        // Check if ordering is satisfied
        let orderingSatisfied = checkOrderingSatisfied(op, analysis: analysis)

        // Range initiation cost (pre-weighted, goes to additionalCost)
        let rangeInitCost = costModel.rangeInitiationWeight

        return PlanCost(
            indexReads: indexEntries,
            recordFetches: indexEntries,
            postFilterCount: indexEntries * postFilterRatio,
            requiresSort: !orderingSatisfied && !analysis.sortRequirements.isEmpty,
            additionalCost: rangeInitCost,
            costModel: costModel
        )
    }

    /// Check if index scan satisfies ordering requirements
    private func checkOrderingSatisfied(
        _ op: IndexScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> Bool {
        guard !analysis.sortRequirements.isEmpty else { return true }

        // Get index key paths
        let indexKeyPaths = op.index.keyPaths

        // Check if sort requirements match index order
        for (i, sortDesc) in analysis.sortRequirements.enumerated() {
            guard i < indexKeyPaths.count else { return false }

            let indexFieldName = T.fieldName(for: indexKeyPaths[i])
            if indexFieldName != sortDesc.fieldName {
                return false
            }

            // Check direction (reverse scan flips the order)
            let indexOrder: SortOrder = op.reverse ? .descending : .ascending
            if sortDesc.order != indexOrder {
                return false
            }
        }

        return true
    }

    // MARK: - Index-Only Scan (Covering Index)

    /// Estimate cost for index-only scan (no record fetches needed)
    private func estimateIndexOnlyScan(
        _ op: IndexOnlyScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let indexEntries = Double(op.estimatedEntries)

        // Calculate post-filtering ratio
        let satisfiedSelectivity = op.satisfiedConditions.reduce(1.0) { acc, cond in
            acc * estimateConditionSelectivity(cond)
        }
        let totalSelectivity = estimatePredicateSelectivity(analysis.originalPredicate)
        let postFilterRatio = totalSelectivity > 0 ? min(1.0, max(0, 1 - (totalSelectivity / satisfiedSelectivity))) : 0

        // Check if ordering is satisfied
        let orderingSatisfied = checkIndexOnlyScanOrderingSatisfied(op, analysis: analysis)

        // Range initiation cost
        let rangeInitCost = costModel.rangeInitiationWeight

        // KEY DIFFERENCE: No record fetches for index-only scan!
        // All required data comes from the index
        return PlanCost(
            indexReads: indexEntries,
            recordFetches: 0,  // This is the main cost saving
            postFilterCount: indexEntries * postFilterRatio,
            requiresSort: !orderingSatisfied && !analysis.sortRequirements.isEmpty,
            additionalCost: rangeInitCost,
            costModel: costModel
        )
    }

    /// Check if index-only scan satisfies ordering requirements
    private func checkIndexOnlyScanOrderingSatisfied(
        _ op: IndexOnlyScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> Bool {
        guard !analysis.sortRequirements.isEmpty else { return true }

        let indexKeyPaths = op.index.keyPaths

        for (i, sortDesc) in analysis.sortRequirements.enumerated() {
            guard i < indexKeyPaths.count else { return false }

            let indexFieldName = T.fieldName(for: indexKeyPaths[i])
            if indexFieldName != sortDesc.fieldName {
                return false
            }

            let indexOrder: SortOrder = op.reverse ? .descending : .ascending
            if sortDesc.order != indexOrder {
                return false
            }
        }

        return true
    }

    // MARK: - Index Seek

    private func estimateIndexSeek(
        _ op: IndexSeekOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let seekCount = Double(op.seekValues.count)

        // For unique indexes, assume 1 record per seek
        // For non-unique, estimate based on statistics
        let recordsPerSeek: Double
        if op.index.isUnique {
            recordsPerSeek = 1.0
        } else {
            let avgEntriesPerKey = Double(statistics.estimatedIndexEntries(index: op.index) ?? 10000) /
                                   Double(max(1, statistics.estimatedDistinctValues(field: op.satisfiedConditions.first?.field.fieldName ?? "", type: T.self) ?? 1000))
            recordsPerSeek = avgEntriesPerKey
        }

        let totalRecords = seekCount * recordsPerSeek
        let requiresSort = !analysis.sortRequirements.isEmpty && seekCount > 1

        return PlanCost(
            indexReads: seekCount,
            recordFetches: totalRecords,
            postFilterCount: 0,
            requiresSort: requiresSort,
            costModel: costModel
        )
    }

    // MARK: - Union

    private func estimateUnion(
        _ op: UnionOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let childCosts = op.children.map { estimate(plan: $0, analysis: analysis) }

        // Sum of all child costs
        // Note: Execution is SEQUENTIAL (not parallel) for Sendable safety
        // This means actual execution time = sum of child times (not max)
        let totalCost = childCosts.reduce(PlanCost.zero) { $0 + $1 }

        // Range initiation cost per child (pre-weighted, goes to additionalCost)
        let rangeInitCost = Double(op.children.count) * costModel.rangeInitiationWeight

        // Total records fetched across all children
        let totalRecordFetches = childCosts.reduce(0.0) { $0 + $1.recordFetches }

        // Deduplication cost (pre-weighted, goes to additionalCost)
        // Uses string-based ID comparison for Sendable safety
        let dedupCost = op.deduplicate
            ? costModel.dedupCost(records: totalRecordFetches)
            : 0

        // Union output is UNORDERED - requires sort if ordering is needed
        // Even with sequential execution, child results are interleaved by branch
        let requiresSort = !analysis.sortRequirements.isEmpty

        return PlanCost(
            indexReads: totalCost.indexReads,
            recordFetches: totalRecordFetches,
            postFilterCount: totalCost.postFilterCount,
            requiresSort: requiresSort,
            additionalCost: totalCost.additionalCost + rangeInitCost + dedupCost,
            costModel: costModel
        )
    }

    // MARK: - Intersection

    private func estimateIntersection(
        _ op: IntersectionOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let childCosts = op.children.map { estimate(plan: $0, analysis: analysis) }

        // All children need to be scanned for IDs
        let totalIndexReads = childCosts.reduce(0.0) { $0 + $1.indexReads }

        // Range initiation cost per child (pre-weighted, goes to additionalCost)
        let rangeInitCost = Double(op.children.count) * costModel.rangeInitiationWeight

        // Estimate intersection result size
        let childFetches = childCosts.map { $0.recordFetches }
        let minChildFetches = childFetches.min() ?? 0
        let intersectionRatio = 0.1 // Heuristic: 10% survive intersection
        let estimatedResults = minChildFetches * intersectionRatio

        // Cost for ID set operations (pre-weighted, goes to additionalCost)
        let idSetCost = totalIndexReads * costModel.intersectionWeight

        // Cost for fetching final records (pre-weighted, goes to additionalCost)
        let fetchCost = estimatedResults * costModel.intersectionFetchWeight

        // Sum up child additional costs
        let childAdditionalCosts = childCosts.reduce(0.0) { $0 + $1.additionalCost }

        return PlanCost(
            indexReads: totalIndexReads,
            recordFetches: estimatedResults,
            postFilterCount: 0,
            requiresSort: !analysis.sortRequirements.isEmpty,
            additionalCost: childAdditionalCosts + rangeInitCost + idSetCost + fetchCost,
            costModel: costModel
        )
    }

    // MARK: - Filter

    private func estimateFilter(
        _ op: FilterOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let inputCost = estimate(plan: op.input, analysis: analysis)

        // Filter reduces record count but adds post-filter cost
        let filteredRecords = inputCost.recordFetches * op.selectivity
        // filterCost is pre-weighted (already includes postFilterWeight), goes to additionalCost
        let filterCostValue = costModel.filterCost(records: inputCost.recordFetches, selectivity: op.selectivity)

        return PlanCost(
            indexReads: inputCost.indexReads,
            recordFetches: filteredRecords,
            postFilterCount: inputCost.postFilterCount,
            requiresSort: inputCost.requiresSort,
            additionalCost: inputCost.additionalCost + filterCostValue,
            costModel: costModel
        )
    }

    // MARK: - Sort

    private func estimateSort(
        _ op: SortOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let inputCost = estimate(plan: op.input, analysis: analysis)

        // sortCost is pre-weighted (already includes sortWeight), goes to additionalCost
        let sortCostValue = costModel.sortCost(records: inputCost.recordFetches)

        return PlanCost(
            indexReads: inputCost.indexReads,
            recordFetches: inputCost.recordFetches,
            postFilterCount: inputCost.postFilterCount,
            requiresSort: false, // Sort operator satisfies sort requirement
            additionalCost: inputCost.additionalCost + sortCostValue,
            costModel: costModel
        )
    }

    // MARK: - Limit

    private func estimateLimit(
        _ op: LimitOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let inputCost = estimate(plan: op.input, analysis: analysis)

        // Limit can reduce the number of records we need to process
        let limitedRecords: Double
        if let limit = op.limit {
            let offset = Double(op.offset ?? 0)
            limitedRecords = min(inputCost.recordFetches, Double(limit) + offset)
        } else {
            limitedRecords = inputCost.recordFetches
        }

        // Ratio of reduction
        let ratio = limitedRecords / max(1, inputCost.recordFetches)

        // Early termination is ONLY possible when input is already sorted
        // If requiresSort is true, we must scan ALL records, sort them, THEN apply limit
        let canEarlyTerminate = !inputCost.requiresSort

        if canEarlyTerminate {
            // Early termination: reduce all costs proportionally
            return PlanCost(
                indexReads: inputCost.indexReads * ratio,
                recordFetches: limitedRecords,
                postFilterCount: inputCost.postFilterCount * ratio,
                requiresSort: false,
                additionalCost: inputCost.additionalCost * ratio,
                costModel: costModel
            )
        } else {
            // Must process all records before limiting
            // Index reads and post-filtering happen on full dataset
            // Only final record output is limited
            return PlanCost(
                indexReads: inputCost.indexReads,
                recordFetches: inputCost.recordFetches,
                postFilterCount: inputCost.postFilterCount,
                requiresSort: true, // Still requires sort
                additionalCost: inputCost.additionalCost,
                costModel: costModel
            )
        }
    }

    // MARK: - Full Text Scan

    private func estimateFullTextScan(
        _ op: FullTextScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let estimatedResults = Double(op.estimatedResults)

        return PlanCost(
            indexReads: estimatedResults,
            recordFetches: estimatedResults,
            postFilterCount: 0,
            requiresSort: !analysis.sortRequirements.isEmpty, // FT doesn't preserve order
            costModel: costModel
        )
    }

    // MARK: - Vector Search

    private func estimateVectorSearch(
        _ op: VectorSearchOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let k = Double(op.k)
        let efSearch = Double(op.efSearch ?? op.k * 10)

        // HNSW search cost is approximately O(log(N) * ef_search)
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))
        let searchCost = log2(max(2, totalRows)) * efSearch * 0.1

        return PlanCost(
            indexReads: searchCost,
            recordFetches: k,
            postFilterCount: 0,
            requiresSort: false, // Results ordered by similarity
            costModel: costModel
        )
    }

    // MARK: - Spatial Scan

    private func estimateSpatialScan(
        _ op: SpatialScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let estimatedResults = Double(op.estimatedResults)

        return PlanCost(
            indexReads: estimatedResults * 2, // R-tree traversal
            recordFetches: estimatedResults,
            postFilterCount: 0,
            requiresSort: !analysis.sortRequirements.isEmpty,
            costModel: costModel
        )
    }

    // MARK: - Aggregation

    private func estimateAggregation(
        _ op: AggregationOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        // Aggregation indexes are pre-computed, very cheap
        let groupCount = Double(op.groupByFields.count + 1)

        return PlanCost(
            indexReads: groupCount,
            recordFetches: 0, // No record fetches needed
            postFilterCount: 0,
            requiresSort: false,
            costModel: costModel
        )
    }

    // MARK: - Selectivity Estimation

    /// Estimate selectivity of a predicate
    private func estimatePredicateSelectivity(_ predicate: Predicate<T>?) -> Double {
        guard let predicate = predicate else { return 1.0 }

        switch predicate {
        case .comparison(let comparison):
            return estimateComparisonSelectivity(comparison)

        case .and(let predicates):
            // AND: multiply selectivities
            return predicates.reduce(1.0) { acc, pred in
                acc * estimatePredicateSelectivity(pred)
            }

        case .or(let predicates):
            // OR: 1 - (1-s1) * (1-s2) * ...
            let inverseProduct = predicates.reduce(1.0) { acc, pred in
                acc * (1 - estimatePredicateSelectivity(pred))
            }
            return 1 - inverseProduct

        case .not(let inner):
            return 1 - estimatePredicateSelectivity(inner)

        case .true:
            return 1.0

        case .false:
            return 0.0
        }
    }

    /// Estimate selectivity of a comparison
    private func estimateComparisonSelectivity(_ comparison: FieldComparison<T>) -> Double {
        let fieldName = comparison.fieldName

        switch comparison.op {
        case .equal:
            return statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity

        case .notEqual:
            let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity
            return 1 - eqSelectivity

        case .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual:
            return costModel.defaultRangeSelectivity

        case .contains, .hasPrefix, .hasSuffix:
            return costModel.defaultPatternSelectivity

        case .in:
            let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity
            // IN with n values: clamp to 1.0
            let valueCount = extractArrayCount(from: comparison.value.value)
            if valueCount > 0 {
                return min(1.0, eqSelectivity * Double(valueCount))
            }
            return eqSelectivity

        case .isNil:
            return statistics.nullSelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultNullSelectivity

        case .isNotNil:
            let nullSelectivity = statistics.nullSelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultNullSelectivity
            return 1 - nullSelectivity
        }
    }

    /// Estimate selectivity of a field condition
    public func estimateConditionSelectivity(_ condition: FieldCondition<T>) -> Double {
        let fieldName = condition.field.fieldName

        switch condition.constraint {
        case .equals:
            return statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity

        case .notEquals:
            let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity
            return 1 - eqSelectivity

        case .range(let range):
            return statistics.rangeSelectivity(field: fieldName, range: range, type: T.self)
                ?? costModel.defaultRangeSelectivity

        case .in(let values):
            let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity
            return min(1.0, eqSelectivity * Double(values.count))

        case .notIn(let values):
            // NOT IN excludes specific values: 1 - (selectivity * count)
            let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultEqualitySelectivity
            return max(0.0, 1.0 - min(1.0, eqSelectivity * Double(values.count)))

        case .isNull(let isNull):
            let nullSelectivity = statistics.nullSelectivity(field: fieldName, type: T.self)
                ?? costModel.defaultNullSelectivity
            return isNull ? nullSelectivity : (1 - nullSelectivity)

        case .textSearch:
            return costModel.defaultTextSearchSelectivity

        case .spatial:
            return costModel.defaultSpatialSelectivity

        case .vectorSimilarity:
            return costModel.defaultVectorSelectivity

        case .stringPattern:
            return costModel.defaultPatternSelectivity
        }
    }

    // MARK: - Helpers

    /// Extract array count from a value that might be an array
    ///
    /// Handles various array representations:
    /// - `[AnySendable]` arrays
    /// - Arrays accessed via Mirror reflection
    private func extractArrayCount(from value: Any) -> Int {
        // Try AnySendable array first
        if let anySendableArray = value as? [AnySendable] {
            return anySendableArray.count
        }

        // Use Mirror to check if value is a collection
        let mirror = Mirror(reflecting: value)
        if mirror.displayStyle == .collection {
            return mirror.children.count
        }

        return 0
    }
}
