// QueryPlanner.swift
// QueryPlanner - Main entry point for query planning

import Foundation
import Core

/// Main query planner that coordinates analysis, planning, and optimization
public final class QueryPlanner<T: Persistable>: @unchecked Sendable {

    /// Available indexes for the type
    private let availableIndexes: [IndexDescriptor]

    /// Statistics provider for cost estimation
    private let statistics: StatisticsProvider

    /// Cost model configuration
    private let costModel: CostModel

    /// Index-specific planning strategies
    private let strategyRegistry: IndexStrategyRegistry

    public init(
        indexes: [IndexDescriptor],
        statistics: StatisticsProvider = DefaultStatisticsProvider(),
        costModel: CostModel = .default,
        strategyRegistry: IndexStrategyRegistry = IndexStrategyRegistry()
    ) {
        self.availableIndexes = indexes
        self.statistics = statistics
        self.costModel = costModel
        self.strategyRegistry = strategyRegistry
    }

    /// Plan a query and return the optimal execution plan
    public func plan(query: Query<T>) throws -> QueryPlan<T> {
        // 1. Analyze the query
        let analyzer = QueryAnalyzer<T>()
        let analysis = try analyzer.analyze(query)

        // 2. Enumerate candidate plans
        let enumerator = PlanEnumerator<T>(
            indexes: availableIndexes,
            strategyRegistry: strategyRegistry,
            statistics: statistics,
            costModel: costModel
        )
        let candidates = enumerator.enumerate(analysis: analysis)

        // 3. Estimate costs for each plan
        let estimator = CostEstimator<T>(
            statistics: statistics,
            costModel: costModel
        )
        let costedPlans = candidates.map { plan in
            (plan, estimator.estimate(plan: plan, analysis: analysis))
        }

        // 4. Select optimal plan
        let optimizer = PlanOptimizer<T>(costModel: costModel)
        guard let (optimalPlan, cost) = optimizer.selectBest(costedPlans) else {
            // Fallback to table scan
            return createTableScanPlan(query: query, analysis: analysis)
        }

        return QueryPlan(
            id: UUID(),
            rootOperator: optimalPlan,
            estimatedCost: cost,
            usedFields: analysis.referencedFields,
            usedIndexes: extractUsedIndexes(optimalPlan),
            orderingSatisfied: checkOrderingSatisfied(optimalPlan, analysis: analysis),
            postFilterPredicate: computePostFilter(optimalPlan, analysis: analysis)
        )
    }

    /// Plan a query with hints
    public func plan(query: Query<T>, hints: QueryHints) throws -> QueryPlan<T> {
        // 1. Analyze the query
        let analyzer = QueryAnalyzer<T>()
        let analysis = try analyzer.analyze(query)

        // Handle force table scan hint
        if hints.forceTableScan {
            return createTableScanPlan(query: query, analysis: analysis)
        }

        // 2. Enumerate candidate plans
        let enumerator = PlanEnumerator<T>(
            indexes: availableIndexes,
            strategyRegistry: strategyRegistry,
            statistics: statistics,
            costModel: costModel
        )
        let candidates = enumerator.enumerate(analysis: analysis)

        // 3. Estimate costs for each plan
        let estimator = CostEstimator<T>(
            statistics: statistics,
            costModel: costModel
        )
        let costedPlans = candidates.map { plan in
            (plan, estimator.estimate(plan: plan, analysis: analysis))
        }

        // 4. Select optimal plan with hints
        let optimizer = PlanOptimizer<T>(costModel: costModel)
        guard let (optimalPlan, cost) = optimizer.selectBest(costedPlans, hints: hints) else {
            return createTableScanPlan(query: query, analysis: analysis)
        }

        return QueryPlan(
            id: UUID(),
            rootOperator: optimalPlan,
            estimatedCost: cost,
            usedFields: analysis.referencedFields,
            usedIndexes: extractUsedIndexes(optimalPlan),
            orderingSatisfied: checkOrderingSatisfied(optimalPlan, analysis: analysis),
            postFilterPredicate: computePostFilter(optimalPlan, analysis: analysis)
        )
    }

    /// Explain the plan without executing
    public func explain(query: Query<T>) throws -> PlanExplanation {
        let plan = try plan(query: query)
        return PlanExplanation(plan: plan)
    }

    /// Explain the plan with hints
    public func explain(query: Query<T>, hints: QueryHints) throws -> PlanExplanation {
        let plan = try plan(query: query, hints: hints)
        return PlanExplanation(plan: plan)
    }

    // MARK: - Private Helpers

    /// Create a fallback table scan plan
    ///
    /// Note: Filter predicate is stored in TableScanOperator for execution.
    /// We do NOT wrap with a separate Filter operator to avoid double filtering.
    private func createTableScanPlan(query: Query<T>, analysis: QueryAnalysis<T>) -> QueryPlan<T> {
        let estimatedRows = statistics.estimatedRowCount(for: T.self)

        let normalizer = PredicateNormalizer<T>()
        let combinedPredicate = normalizer.combinePredicates(query.predicates)

        // TableScan handles filtering internally during execution
        // Do NOT wrap with Filter operator - that would cause double filtering
        var rootOp: PlanOperator<T> = .tableScan(TableScanOperator(
            estimatedRows: estimatedRows,
            filterPredicate: combinedPredicate
        ))

        // Add sort if needed
        if !query.sortDescriptors.isEmpty {
            rootOp = .sort(SortOperator(
                input: rootOp,
                sortDescriptors: query.sortDescriptors,
                estimatedInputSize: estimatedRows
            ))
        }

        // Add limit if needed
        if query.fetchLimit != nil || query.fetchOffset != nil {
            rootOp = .limit(LimitOperator(
                input: rootOp,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            ))
        }

        let cost = CostEstimator<T>(statistics: statistics, costModel: costModel)
            .estimate(plan: rootOp, analysis: analysis)

        return QueryPlan(
            id: UUID(),
            rootOperator: rootOp,
            estimatedCost: cost,
            usedFields: analysis.referencedFields,
            usedIndexes: [],
            orderingSatisfied: false,
            postFilterPredicate: nil
        )
    }

    /// Extract all indexes used in a plan
    private func extractUsedIndexes(_ plan: PlanOperator<T>) -> [IndexDescriptor] {
        var indexes: [IndexDescriptor] = []

        switch plan {
        case .indexScan(let op):
            indexes.append(op.index)

        case .indexSeek(let op):
            indexes.append(op.index)

        case .indexOnlyScan(let op):
            indexes.append(op.index)

        case .fullTextScan(let op):
            indexes.append(op.index)

        case .vectorSearch(let op):
            indexes.append(op.index)

        case .spatialScan(let op):
            indexes.append(op.index)

        case .aggregation(let op):
            indexes.append(op.index)

        case .union(let op):
            for child in op.children {
                indexes.append(contentsOf: extractUsedIndexes(child))
            }

        case .intersection(let op):
            for child in op.children {
                indexes.append(contentsOf: extractUsedIndexes(child))
            }

        case .filter(let op):
            indexes.append(contentsOf: extractUsedIndexes(op.input))

        case .sort(let op):
            indexes.append(contentsOf: extractUsedIndexes(op.input))

        case .limit(let op):
            indexes.append(contentsOf: extractUsedIndexes(op.input))

        case .project(let op):
            indexes.append(contentsOf: extractUsedIndexes(op.input))

        case .tableScan:
            break

        case .inUnion(let op):
            indexes.append(op.index)

        case .inJoin(let op):
            indexes.append(op.index)
        }

        return indexes
    }

    /// Check if ordering is satisfied by the plan
    private func checkOrderingSatisfied(_ plan: PlanOperator<T>, analysis: QueryAnalysis<T>) -> Bool {
        guard !analysis.sortRequirements.isEmpty else { return true }

        switch plan {
        case .indexScan(let op):
            return indexSatisfiesOrdering(op.index, reverse: op.reverse, sortRequirements: analysis.sortRequirements)

        case .indexSeek(let op):
            return op.seekValues.count <= 1

        case .indexOnlyScan(let op):
            return indexSatisfiesOrdering(op.index, reverse: op.reverse, sortRequirements: analysis.sortRequirements)

        case .vectorSearch:
            return true

        case .sort:
            return true

        case .filter(let op):
            return checkOrderingSatisfied(op.input, analysis: analysis)

        case .limit(let op):
            return checkOrderingSatisfied(op.input, analysis: analysis)

        case .project(let op):
            return checkOrderingSatisfied(op.input, analysis: analysis)

        default:
            return false
        }
    }

    /// Check if an index satisfies ordering requirements
    private func indexSatisfiesOrdering(
        _ index: IndexDescriptor,
        reverse: Bool,
        sortRequirements: [SortDescriptor<T>]
    ) -> Bool {
        for (i, sortDesc) in sortRequirements.enumerated() {
            guard i < index.keyPaths.count else { return false }

            let indexFieldName = T.fieldName(for: index.keyPaths[i])
            if indexFieldName != sortDesc.fieldName {
                return false
            }

            let indexOrder: SortOrder = reverse ? .descending : .ascending
            if sortDesc.order != indexOrder {
                return false
            }
        }
        return true
    }

    /// Compute the post-filter predicate for conditions not satisfied by the plan
    private func computePostFilter(_ plan: PlanOperator<T>, analysis: QueryAnalysis<T>) -> Predicate<T>? {
        // Collect satisfied condition identifiers from the plan
        // Using identifiers instead of just field names allows proper tracking
        // of multiple conditions on the same field (e.g., age > 20 AND age < 50)
        let satisfiedIdentifiers = collectSatisfiedConditionIdentifiers(plan)

        // Find unsatisfied conditions by comparing identifiers
        let unsatisfied = analysis.fieldConditions.filter { condition in
            !satisfiedIdentifiers.contains(condition.identifier)
        }

        guard !unsatisfied.isEmpty else { return nil }

        // Rebuild predicate from unsatisfied conditions
        let predicates = unsatisfied.compactMap { $0.predicate }

        if predicates.count == 1 {
            return predicates[0]
        } else if predicates.count > 1 {
            return .and(predicates)
        }

        return nil
    }

    /// Collect condition identifiers satisfied by the plan
    ///
    /// Uses condition identifiers (fieldName:constraintType:value) instead of just field names
    /// to properly distinguish between multiple conditions on the same field.
    ///
    /// Example: Query "age > 20 AND age < 50" has two separate conditions on "age".
    /// If the index only satisfies "age > 20", using field names would incorrectly
    /// mark both conditions as satisfied. Using identifiers correctly tracks which
    /// specific condition is satisfied.
    private func collectSatisfiedConditionIdentifiers(_ plan: PlanOperator<T>) -> Set<String> {
        var identifiers: Set<String> = []

        switch plan {
        case .indexScan(let op):
            for condition in op.satisfiedConditions {
                identifiers.insert(condition.identifier)
            }

        case .indexSeek(let op):
            for condition in op.satisfiedConditions {
                identifiers.insert(condition.identifier)
            }

        case .indexOnlyScan(let op):
            for condition in op.satisfiedConditions {
                identifiers.insert(condition.identifier)
            }

        case .fullTextScan(let op):
            // Full-text scan satisfies text search conditions
            // Generate identifier matching the text search constraint format
            if let firstKeyPath = op.index.keyPaths.first {
                let fieldName = T.fieldName(for: firstKeyPath)
                let terms = op.searchTerms.joined(separator: ",")
                identifiers.insert("\(fieldName):text:\(terms):\(op.matchMode)")
            }

        case .vectorSearch(let op):
            // Vector search satisfies similarity conditions
            if let firstKeyPath = op.index.keyPaths.first {
                let fieldName = T.fieldName(for: firstKeyPath)
                identifiers.insert("\(fieldName):vector:k=\(op.k)")
            }

        case .spatialScan(let op):
            // Spatial scan satisfies spatial conditions
            if let firstKeyPath = op.index.keyPaths.first {
                let fieldName = T.fieldName(for: firstKeyPath)
                identifiers.insert("\(fieldName):spatial:\(op.constraint.type)")
            }

        case .filter(let op):
            identifiers.formUnion(collectSatisfiedConditionIdentifiers(op.input))

        case .sort(let op):
            identifiers.formUnion(collectSatisfiedConditionIdentifiers(op.input))

        case .limit(let op):
            identifiers.formUnion(collectSatisfiedConditionIdentifiers(op.input))

        case .project(let op):
            identifiers.formUnion(collectSatisfiedConditionIdentifiers(op.input))

        case .union(let op):
            // Union: collect satisfied conditions from ALL children
            //
            // For OR queries, each branch independently filters its results.
            // If child 1 satisfies condition A and child 2 satisfies condition B,
            // both are satisfied because:
            // - Records from child 1 already passed condition A
            // - Records from child 2 already passed condition B
            // No post-filter is needed for A or B.
            for child in op.children {
                identifiers.formUnion(collectSatisfiedConditionIdentifiers(child))
            }

        case .intersection(let op):
            // Intersection: union of all child conditions
            // All children must produce matching records, so any condition
            // satisfied by any child contributes to the overall filter.
            for child in op.children {
                identifiers.formUnion(collectSatisfiedConditionIdentifiers(child))
            }

        case .tableScan, .aggregation:
            // Table scan doesn't satisfy any conditions via index
            // Aggregation operates on pre-computed values
            break

        case .inUnion(let op):
            // IN-Union satisfies the IN condition on the field
            identifiers.insert("\(op.fieldPath):in:\(op.valueCount)")

        case .inJoin(let op):
            // IN-Join satisfies the IN condition on the field
            identifiers.insert("\(op.fieldPath):in:\(op.valueCount)")
        }

        return identifiers
    }
}
