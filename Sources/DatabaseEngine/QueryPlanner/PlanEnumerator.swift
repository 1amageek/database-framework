// PlanEnumerator.swift
// QueryPlanner - Plan enumeration and generation

import Core

/// Generates candidate execution plans for a query
public struct PlanEnumerator<T: Persistable> {

    private let indexes: [IndexDescriptor]
    private let strategyRegistry: IndexStrategyRegistry
    private let statistics: StatisticsProvider
    private let costModel: CostModel

    public init(
        indexes: [IndexDescriptor],
        strategyRegistry: IndexStrategyRegistry = IndexStrategyRegistry(),
        statistics: StatisticsProvider,
        costModel: CostModel = .default
    ) {
        self.indexes = indexes
        self.strategyRegistry = strategyRegistry
        self.statistics = statistics
        self.costModel = costModel
    }

    /// Enumerate all reasonable candidate plans
    public func enumerate(analysis: QueryAnalysis<T>) -> [PlanOperator<T>] {
        var candidates: [PlanOperator<T>] = []

        // Always include table scan as fallback
        candidates.append(createTableScan(analysis: analysis))

        // Try single-index plans
        for index in indexes {
            if let plan = tryCreateIndexPlan(index: index, analysis: analysis) {
                candidates.append(plan)
            }
        }

        // Try index-only scan plans (covering indexes)
        candidates.append(contentsOf: tryIndexOnlyScanPlans(analysis: analysis))

        // Try composite index plans (covering multiple conditions)
        candidates.append(contentsOf: tryCompositeIndexPlans(analysis: analysis))

        // Try index intersection plans (AND with multiple indexes)
        let equalityCount = analysis.fieldConditions.filter { $0.constraint.isEquality }.count
        if equalityCount >= 2 {
            candidates.append(contentsOf: tryIntersectionPlans(analysis: analysis))
        }

        // Try index union plans (OR conditions)
        if case .disjunction = analysis.normalizedCondition {
            candidates.append(contentsOf: tryUnionPlans(analysis: analysis))
        }

        // Apply sort operator if needed
        candidates = candidates.map { plan in
            wrapWithSort(plan: plan, analysis: analysis)
        }

        // Apply limit/offset if needed
        candidates = candidates.map { plan in
            wrapWithLimit(plan: plan, analysis: analysis)
        }

        return candidates
    }

    // MARK: - Table Scan

    /// Create a table scan plan
    ///
    /// Note: The filter predicate is stored in TableScanOperator for execution,
    /// NOT wrapped with a separate Filter operator to avoid double filtering.
    private func createTableScan(analysis: QueryAnalysis<T>) -> PlanOperator<T> {
        let estimatedRows = statistics.estimatedRowCount(for: T.self)

        // TableScan handles filtering internally during execution
        // Do NOT wrap with Filter operator - that would cause double filtering
        return .tableScan(TableScanOperator(
            estimatedRows: estimatedRows,
            filterPredicate: analysis.originalPredicate
        ))
    }

    // MARK: - Single Index Plans

    /// Try to create an index plan for a single index
    private func tryCreateIndexPlan(
        index: IndexDescriptor,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T>? {
        // Get the appropriate strategy for this index type
        guard let strategy = strategyRegistry.strategy(for: index) else {
            return nil
        }

        // Check if index can satisfy any conditions
        let matchResult = strategy.matchConditions(
            index: index,
            conditions: analysis.fieldConditions,
            statistics: statistics
        )

        guard !matchResult.satisfiedConditions.isEmpty else {
            return nil
        }

        // Create the index operator
        let indexOp = strategy.createOperator(
            index: index,
            matchResult: matchResult,
            analysis: analysis
        )

        // Wrap with post-filter if needed
        let unsatisfied = analysis.fieldConditions.filter { condition in
            !matchResult.satisfiedConditions.contains(where: {
                $0.field.fieldName == condition.field.fieldName
            })
        }

        if unsatisfied.isEmpty {
            return indexOp
        } else {
            let filterPredicate = rebuildPredicate(from: unsatisfied)
            let estimator = CostEstimator<T>(statistics: statistics, costModel: costModel)
            let selectivity = unsatisfied.reduce(1.0) { acc, cond in
                acc * estimator.estimateConditionSelectivity(cond)
            }

            return .filter(FilterOperator(
                input: indexOp,
                predicate: filterPredicate,
                selectivity: selectivity
            ))
        }
    }

    // MARK: - Composite Index Plans

    /// Try to use composite indexes that cover multiple conditions
    private func tryCompositeIndexPlans(analysis: QueryAnalysis<T>) -> [PlanOperator<T>] {
        var plans: [PlanOperator<T>] = []

        // Find indexes with multiple key paths
        let compositeIndexes = indexes.filter { $0.keyPaths.count > 1 }

        for index in compositeIndexes {
            guard let strategy = strategyRegistry.strategy(for: index) else { continue }

            let matchResult = strategy.matchConditions(
                index: index,
                conditions: analysis.fieldConditions,
                statistics: statistics
            )

            // Only use if it satisfies more than one condition
            guard matchResult.satisfiedConditions.count > 1 else { continue }

            let indexOp = strategy.createOperator(
                index: index,
                matchResult: matchResult,
                analysis: analysis
            )

            // Add post-filter if needed
            let unsatisfied = analysis.fieldConditions.filter { condition in
                !matchResult.satisfiedConditions.contains(where: {
                    $0.field.fieldName == condition.field.fieldName
                })
            }

            if unsatisfied.isEmpty {
                plans.append(indexOp)
            } else {
                let filterPredicate = rebuildPredicate(from: unsatisfied)
                let estimator = CostEstimator<T>(statistics: statistics, costModel: costModel)
                let selectivity = unsatisfied.reduce(1.0) { acc, cond in
                    acc * estimator.estimateConditionSelectivity(cond)
                }

                plans.append(.filter(FilterOperator(
                    input: indexOp,
                    predicate: filterPredicate,
                    selectivity: selectivity
                )))
            }
        }

        return plans
    }

    // MARK: - Intersection Plans

    /// Maximum number of indexes to use in an intersection
    ///
    /// Higher values may find better plans but increase planning time and
    /// intersection execution overhead. The cost of intersection grows with
    /// more children due to ID set operations.
    private var maxIntersectionIndexes: Int { 3 }

    /// Try to use multiple indexes together with intersection
    ///
    /// Uses scoring to rank candidate indexes by selectivity and uniqueness,
    /// preferring more selective indexes for intersection operations.
    ///
    /// **Algorithm**:
    /// 1. Find all indexes that can satisfy equality conditions
    /// 2. Score each (condition, index) pair by uniqueness and selectivity
    /// 3. Greedily select best index per condition up to `maxIntersectionIndexes`
    /// 4. Create intersection plan if at least 2 indexes selected
    ///
    /// **Scoring factors**:
    /// - Unique indexes: 100x (guaranteed single result)
    /// - First-field match: 10x (optimal prefix usage)
    /// - Selectivity: inverse (lower selectivity = better)
    private func tryIntersectionPlans(analysis: QueryAnalysis<T>) -> [PlanOperator<T>] {
        let equalityConditions = analysis.fieldConditions.filter {
            if case .equals = $0.constraint { return true }
            return false
        }

        guard equalityConditions.count >= 2 else { return [] }

        // Build scored candidates: (condition, index, score)
        var rankedCandidates: [(condition: FieldCondition<T>, index: IndexDescriptor, score: Double)] = []

        for condition in equalityConditions {
            let candidates = findCandidateIndexes(for: condition)
            for index in candidates {
                let score = scoreIndex(index, for: condition)
                rankedCandidates.append((condition, index, score))
            }
        }

        // Sort by score descending (higher = better)
        rankedCandidates.sort { $0.score > $1.score }

        // Greedily select best index per condition (no duplicates)
        var usedConditionFields: Set<String> = []
        var selectedPlans: [PlanOperator<T>] = []

        for (condition, index, _) in rankedCandidates {
            let fieldName = condition.field.fieldName
            if usedConditionFields.contains(fieldName) { continue }

            if let plan = tryCreateSingleIndexPlan(index: index, condition: condition) {
                usedConditionFields.insert(fieldName)
                selectedPlans.append(plan)
            }

            // Limit intersection size to control execution overhead
            // More indexes = more ID set operations = higher cost
            if selectedPlans.count >= maxIntersectionIndexes { break }
        }

        // Need at least 2 indexes for intersection
        guard selectedPlans.count >= 2 else { return [] }

        return [.intersection(IntersectionOperator(children: selectedPlans))]
    }

    /// Find all indexes that could satisfy a condition
    private func findCandidateIndexes(for condition: FieldCondition<T>) -> [IndexDescriptor] {
        indexes.filter { index in
            guard let firstKeyPath = index.keyPaths.first else { return false }
            return T.fieldName(for: firstKeyPath) == condition.field.fieldName
        }
    }

    /// Score an index for intersection planning
    ///
    /// Higher scores indicate more desirable indexes:
    /// - Unique indexes: 100x multiplier (guaranteed single result)
    /// - First field match: 10x multiplier (optimal prefix usage)
    /// - Higher selectivity: inversely proportional to selectivity
    private func scoreIndex(
        _ index: IndexDescriptor,
        for condition: FieldCondition<T>
    ) -> Double {
        var score: Double = 1.0

        // Strongly prefer unique indexes (single result guaranteed)
        if index.isUnique {
            score *= 100.0
        }

        // Prefer indexes where condition field is the first key
        if let firstKeyPath = index.keyPaths.first,
           T.fieldName(for: firstKeyPath) == condition.field.fieldName {
            score *= 10.0
        }

        // Factor in estimated selectivity (lower = more selective = better)
        if let selectivity = statistics.equalitySelectivity(
            field: condition.field.fieldName,
            type: T.self
        ) {
            // Inverse: selectivity 0.01 → score multiplier 100
            score *= (1.0 / max(selectivity, 0.0001))
        }

        return score
    }

    /// Create an index plan for a single condition
    private func tryCreateSingleIndexPlan(
        index: IndexDescriptor,
        condition: FieldCondition<T>
    ) -> PlanOperator<T>? {
        guard let strategy = strategyRegistry.strategy(for: index) else {
            return nil
        }

        let matchResult = strategy.matchConditions(
            index: index,
            conditions: [condition],
            statistics: statistics
        )

        guard !matchResult.satisfiedConditions.isEmpty else {
            return nil
        }

        // Create a minimal analysis for this single condition
        let minimalAnalysis = QueryAnalysis<T>(
            originalPredicate: condition.sourcePredicate,
            normalizedCondition: .field(condition),
            fieldConditions: [condition],
            fieldRequirements: [:],
            sortRequirements: [],
            limit: nil,
            offset: nil,
            detectedPatterns: [],
            referencedFields: [condition.field.fieldName]
        )

        return strategy.createOperator(
            index: index,
            matchResult: matchResult,
            analysis: minimalAnalysis
        )
    }

    // MARK: - Union Plans

    /// Try to use union for OR conditions
    private func tryUnionPlans(analysis: QueryAnalysis<T>) -> [PlanOperator<T>] {
        guard case .disjunction(let disjuncts) = analysis.normalizedCondition else {
            return []
        }

        var childPlans: [PlanOperator<T>] = []

        for disjunct in disjuncts {
            let conditions = extractConditions(from: disjunct)

            // Create sub-analysis for this disjunct
            let subAnalysis = QueryAnalysis<T>(
                originalPredicate: analysis.originalPredicate,
                normalizedCondition: disjunct,
                fieldConditions: conditions,
                fieldRequirements: [:],
                sortRequirements: [],
                limit: nil,
                offset: nil,
                detectedPatterns: [],
                referencedFields: Set(conditions.map { $0.field.fieldName })
            )

            // Try to find an index plan for this disjunct
            var foundPlan = false
            for index in indexes {
                if let plan = tryCreateIndexPlan(index: index, analysis: subAnalysis) {
                    childPlans.append(plan)
                    foundPlan = true
                    break
                }
            }

            // If no index found, use table scan for this disjunct
            if !foundPlan {
                childPlans.append(createTableScan(analysis: subAnalysis))
            }
        }

        // Only use union if we have multiple children
        guard childPlans.count > 1 else { return [] }

        return [.union(UnionOperator(children: childPlans, deduplicate: true))]
    }

    /// Extract conditions from a QueryCondition
    private func extractConditions(from condition: QueryCondition<T>) -> [FieldCondition<T>] {
        switch condition {
        case .field(let fieldCondition):
            return [fieldCondition]
        case .conjunction(let conditions):
            return conditions.flatMap { extractConditions(from: $0) }
        case .disjunction(let conditions):
            return conditions.flatMap { extractConditions(from: $0) }
        case .alwaysTrue, .alwaysFalse:
            return []
        }
    }

    // MARK: - Sort and Limit Wrapping

    /// Wrap plan with sort if needed
    private func wrapWithSort(
        plan: PlanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard !analysis.sortRequirements.isEmpty else { return plan }

        // Check if plan already provides required ordering
        if planProvidesOrdering(plan, sortRequirements: analysis.sortRequirements) {
            return plan
        }

        let estimatedSize = estimateOutputSize(plan: plan, analysis: analysis)

        return .sort(SortOperator(
            input: plan,
            sortDescriptors: analysis.sortRequirements,
            estimatedInputSize: estimatedSize
        ))
    }

    /// Check if a plan provides the required ordering
    private func planProvidesOrdering(
        _ plan: PlanOperator<T>,
        sortRequirements: [SortDescriptor<T>]
    ) -> Bool {
        guard !sortRequirements.isEmpty else { return true }

        switch plan {
        case .indexScan(let op):
            return checkIndexProvidesOrdering(op.index, reverse: op.reverse, sortRequirements: sortRequirements)

        case .indexSeek(let op):
            return op.seekValues.count <= 1 // Single seek preserves order

        case .vectorSearch:
            return true // Vector search results are ordered by similarity

        case .sort:
            return true // Sort operator provides ordering

        case .filter(let op):
            return planProvidesOrdering(op.input, sortRequirements: sortRequirements)

        case .project(let op):
            return planProvidesOrdering(op.input, sortRequirements: sortRequirements)

        case .limit(let op):
            return planProvidesOrdering(op.input, sortRequirements: sortRequirements)

        case .union, .intersection, .tableScan, .fullTextScan, .spatialScan, .aggregation:
            return false
        }
    }

    /// Check if an index provides the required ordering
    private func checkIndexProvidesOrdering(
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

            // Check direction
            let indexOrder: SortOrder = reverse ? .descending : .ascending
            if sortDesc.order != indexOrder {
                return false
            }
        }

        return true
    }

    /// Wrap plan with limit if needed
    private func wrapWithLimit(
        plan: PlanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard analysis.limit != nil || analysis.offset != nil else { return plan }

        return .limit(LimitOperator(
            input: plan,
            limit: analysis.limit,
            offset: analysis.offset
        ))
    }

    /// Estimate output size of a plan
    private func estimateOutputSize(plan: PlanOperator<T>, analysis: QueryAnalysis<T>) -> Int {
        let estimator = CostEstimator<T>(statistics: statistics, costModel: costModel)
        let cost = estimator.estimate(plan: plan, analysis: analysis)
        return Int(cost.recordFetches)
    }

    // MARK: - Index-Only Scan Plans (Covering Index)

    /// Try to create index-only scan plans for covering indexes
    ///
    /// An index-only scan is possible when all required fields are available
    /// in the index, eliminating the need to fetch the actual record.
    private func tryIndexOnlyScanPlans(analysis: QueryAnalysis<T>) -> [PlanOperator<T>] {
        var plans: [PlanOperator<T>] = []
        let analyzer = IndexOnlyScanAnalyzer<T>()

        for index in indexes {
            // Check if this index can satisfy conditions
            guard let strategy = strategyRegistry.strategy(for: index) else { continue }

            let matchResult = strategy.matchConditions(
                index: index,
                conditions: analysis.fieldConditions,
                statistics: statistics
            )

            // Need at least one satisfied condition
            guard !matchResult.satisfiedConditions.isEmpty else { continue }

            // Analyze if index covers all required fields
            let coveringResult = analyzer.analyze(
                query: Query<T>(),  // Empty query, we use analysis directly
                analysis: analysis,
                index: index
            )

            guard coveringResult.canUseIndexOnlyScan else { continue }

            // Create index-only scan operator
            let bounds = computeBounds(for: matchResult, index: index)
            let estimatedEntries = estimateMatchingEntries(
                index: index,
                matchResult: matchResult
            )

            let indexOnlyOp: PlanOperator<T> = .indexOnlyScan(IndexOnlyScanOperator(
                index: index,
                bounds: bounds,
                reverse: shouldScanReverse(index: index, analysis: analysis),
                projectedFields: coveringResult.coveredFields,
                satisfiedConditions: matchResult.satisfiedConditions,
                estimatedEntries: estimatedEntries
            ))

            // Add post-filter if needed
            let unsatisfied = analysis.fieldConditions.filter { condition in
                !matchResult.satisfiedConditions.contains(where: {
                    $0.field.fieldName == condition.field.fieldName
                })
            }

            if unsatisfied.isEmpty {
                plans.append(indexOnlyOp)
            } else {
                let filterPredicate = rebuildPredicate(from: unsatisfied)
                let estimator = CostEstimator<T>(statistics: statistics, costModel: costModel)
                let selectivity = unsatisfied.reduce(1.0) { acc, cond in
                    acc * estimator.estimateConditionSelectivity(cond)
                }

                plans.append(.filter(FilterOperator(
                    input: indexOnlyOp,
                    predicate: filterPredicate,
                    selectivity: selectivity
                )))
            }
        }

        return plans
    }

    /// Compute bounds for index scan based on match result
    private func computeBounds(
        for matchResult: IndexMatchResult<T>,
        index: IndexDescriptor
    ) -> IndexScanBounds {
        var startComponents: [IndexScanBounds.BoundComponent] = []
        var endComponents: [IndexScanBounds.BoundComponent] = []

        for (i, keyPath) in index.keyPaths.enumerated() {
            let fieldName = T.fieldName(for: keyPath)

            // Find condition for this key path position
            guard let condition = matchResult.satisfiedConditions.first(where: {
                $0.field.fieldName == fieldName
            }) else {
                break // Stop at first missing condition (prefix matching)
            }

            switch condition.constraint {
            case .equals(let value):
                startComponents.append(.init(value: value, inclusive: true))
                endComponents.append(.init(value: value, inclusive: true))

            case .range(let bound):
                if let lower = bound.lower {
                    startComponents.append(.init(value: lower.value, inclusive: lower.inclusive))
                }
                if let upper = bound.upper {
                    endComponents.append(.init(value: upper.value, inclusive: upper.inclusive))
                }

            default:
                break
            }

            // For multi-column index, stop after first non-equality
            if i > 0 && !condition.constraint.isEquality {
                break
            }
        }

        return IndexScanBounds(start: startComponents, end: endComponents)
    }

    /// Estimate number of matching entries
    private func estimateMatchingEntries(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>
    ) -> Int {
        let totalRows = statistics.estimatedRowCount(for: T.self)

        // Calculate combined selectivity
        var selectivity = 1.0
        for condition in matchResult.satisfiedConditions {
            let fieldSelectivity: Double
            switch condition.constraint {
            case .equals:
                fieldSelectivity = statistics.equalitySelectivity(
                    field: condition.field.fieldName,
                    type: T.self
                ) ?? 0.01
            case .range(let bound):
                fieldSelectivity = statistics.rangeSelectivity(
                    field: condition.field.fieldName,
                    range: bound,
                    type: T.self
                ) ?? 0.3
            default:
                fieldSelectivity = 0.5
            }
            selectivity *= fieldSelectivity
        }

        return max(1, Int(Double(totalRows) * selectivity))
    }

    /// Determine if scan should be in reverse
    private func shouldScanReverse(
        index: IndexDescriptor,
        analysis: QueryAnalysis<T>
    ) -> Bool {
        guard let firstSort = analysis.sortRequirements.first,
              let firstKeyPath = index.keyPaths.first else {
            return false
        }

        let indexField = T.fieldName(for: firstKeyPath)
        if indexField == firstSort.fieldName && firstSort.order == .descending {
            return true
        }

        return false
    }

    // MARK: - Predicate Rebuilding

    /// Rebuild a predicate from field conditions
    private func rebuildPredicate(from conditions: [FieldCondition<T>]) -> Predicate<T> {
        let predicates = conditions.compactMap { $0.sourcePredicate }

        if predicates.count == 1 {
            return predicates[0]
        } else if predicates.count > 1 {
            return .and(predicates)
        } else {
            return .true
        }
    }
}
