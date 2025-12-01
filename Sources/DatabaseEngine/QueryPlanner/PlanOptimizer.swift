// PlanOptimizer.swift
// QueryPlanner - Plan optimization and selection

import Core

/// Selects the best plan from candidates and applies optimizations
public struct PlanOptimizer<T: Persistable> {

    private let costModel: CostModel
    private let rules: [any OptimizationRule<T>]

    public init(
        costModel: CostModel = .default,
        rules: [any OptimizationRule<T>]? = nil
    ) {
        self.costModel = costModel
        self.rules = rules ?? Self.defaultRules()
    }

    /// Select the best plan from candidates
    public func selectBest(
        _ candidates: [(PlanOperator<T>, PlanCost)]
    ) -> (PlanOperator<T>, PlanCost)? {
        guard !candidates.isEmpty else { return nil }

        // Apply optimization rules to each candidate
        var optimized = candidates.map { (plan, cost) in
            var currentPlan = plan
            for rule in rules {
                currentPlan = rule.apply(to: currentPlan)
            }
            return (currentPlan, cost)
        }

        // Sort by cost
        optimized.sort { $0.1 < $1.1 }

        return optimized.first
    }

    /// Select and optimize a plan with query hints
    public func selectBest(
        _ candidates: [(PlanOperator<T>, PlanCost)],
        hints: QueryHints
    ) -> (PlanOperator<T>, PlanCost)? {
        guard !candidates.isEmpty else { return nil }

        var filtered = candidates

        // Apply hints
        if hints.forceTableScan {
            // Only keep table scan plans
            filtered = candidates.filter { plan, _ in
                if case .tableScan = plan { return true }
                if case .filter(let op) = plan, case .tableScan = op.input { return true }
                return false
            }
        }

        if let preferredIndex = hints.preferredIndex {
            // Prefer the specified index
            let preferred = candidates.filter { plan, _ in
                planUsesIndex(plan, named: preferredIndex)
            }
            if !preferred.isEmpty {
                filtered = preferred
            }
        }

        if let maxCost = hints.maxIndexCost {
            // Filter out plans exceeding max cost
            filtered = filtered.filter { _, cost in
                cost.totalCost <= maxCost
            }
        }

        // Fall back to all candidates if filtering left nothing
        if filtered.isEmpty {
            filtered = candidates
        }

        return selectBest(filtered)
    }

    /// Check if a plan uses a specific index
    private func planUsesIndex(_ plan: PlanOperator<T>, named indexName: String) -> Bool {
        switch plan {
        case .indexScan(let op):
            return op.index.name == indexName
        case .indexSeek(let op):
            return op.index.name == indexName
        case .indexOnlyScan(let op):
            return op.index.name == indexName
        case .fullTextScan(let op):
            return op.index.name == indexName
        case .vectorSearch(let op):
            return op.index.name == indexName
        case .spatialScan(let op):
            return op.index.name == indexName
        case .aggregation(let op):
            return op.index.name == indexName
        case .union(let op):
            return op.children.contains { planUsesIndex($0, named: indexName) }
        case .intersection(let op):
            return op.children.contains { planUsesIndex($0, named: indexName) }
        case .filter(let op):
            return planUsesIndex(op.input, named: indexName)
        case .sort(let op):
            return planUsesIndex(op.input, named: indexName)
        case .limit(let op):
            return planUsesIndex(op.input, named: indexName)
        case .project(let op):
            return planUsesIndex(op.input, named: indexName)
        case .tableScan:
            return false
        }
    }

    /// Default optimization rules
    public static func defaultRules() -> [any OptimizationRule<T>] {
        [
            EliminateRedundantSortRule<T>(),
            PushDownLimitRule<T>(),
            SimplifyFilterRule<T>()
        ]
    }
}

// MARK: - Optimization Rule Protocol

/// Rule for plan optimization
public protocol OptimizationRule<T>: Sendable {
    associatedtype T: Persistable

    /// Apply this rule to transform a plan
    func apply(to plan: PlanOperator<T>) -> PlanOperator<T>
}

// MARK: - Eliminate Redundant Sort Rule

/// Remove sort operators when the input already provides the required ordering
public struct EliminateRedundantSortRule<T: Persistable>: OptimizationRule {

    public init() {}

    public func apply(to plan: PlanOperator<T>) -> PlanOperator<T> {
        switch plan {
        case .sort(let sortOp):
            // Check if input already provides the ordering
            if inputProvidesOrdering(sortOp.input, sortDescriptors: sortOp.sortDescriptors) {
                // Sort is redundant, return the input
                return apply(to: sortOp.input)
            }
            // Recurse into input
            return .sort(SortOperator(
                input: apply(to: sortOp.input),
                sortDescriptors: sortOp.sortDescriptors,
                estimatedInputSize: sortOp.estimatedInputSize
            ))

        case .filter(let op):
            return .filter(FilterOperator(
                input: apply(to: op.input),
                predicate: op.predicate,
                selectivity: op.selectivity
            ))

        case .limit(let op):
            return .limit(LimitOperator(
                input: apply(to: op.input),
                limit: op.limit,
                offset: op.offset
            ))

        case .project(let op):
            return .project(ProjectOperator(
                input: apply(to: op.input),
                fields: op.fields
            ))

        case .union(let op):
            return .union(UnionOperator(
                children: op.children.map { apply(to: $0) },
                deduplicate: op.deduplicate
            ))

        case .intersection(let op):
            return .intersection(IntersectionOperator(
                children: op.children.map { apply(to: $0) }
            ))

        default:
            return plan
        }
    }

    private func inputProvidesOrdering(
        _ input: PlanOperator<T>,
        sortDescriptors: [SortDescriptor<T>]
    ) -> Bool {
        guard !sortDescriptors.isEmpty else { return true }

        switch input {
        case .indexScan(let op):
            return checkIndexProvidesOrdering(op.index, reverse: op.reverse, sortDescriptors: sortDescriptors)

        case .indexSeek(let op):
            return op.seekValues.count <= 1

        case .indexOnlyScan(let op):
            return checkIndexProvidesOrdering(op.index, reverse: op.reverse, sortDescriptors: sortDescriptors)

        case .vectorSearch:
            return true

        case .sort:
            return true

        case .filter(let op):
            return inputProvidesOrdering(op.input, sortDescriptors: sortDescriptors)

        case .project(let op):
            return inputProvidesOrdering(op.input, sortDescriptors: sortDescriptors)

        default:
            return false
        }
    }

    private func checkIndexProvidesOrdering(
        _ index: IndexDescriptor,
        reverse: Bool,
        sortDescriptors: [SortDescriptor<T>]
    ) -> Bool {
        for (i, sortDesc) in sortDescriptors.enumerated() {
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
}

// MARK: - Push Down Limit Rule

/// Apply limit early when possible to reduce data flow
public struct PushDownLimitRule<T: Persistable>: OptimizationRule {

    public init() {}

    public func apply(to plan: PlanOperator<T>) -> PlanOperator<T> {
        switch plan {
        case .limit(let limitOp):
            // First optimize the child operators
            let optimizedChild = apply(to: limitOp.input)

            // Try to push limit into the optimized child
            // tryPushLimit returns the plan wrapped in .limit if needed
            return tryPushLimit(
                limit: limitOp.limit,
                offset: limitOp.offset,
                into: optimizedChild
            )

        case .filter(let op):
            return .filter(FilterOperator(
                input: apply(to: op.input),
                predicate: op.predicate,
                selectivity: op.selectivity
            ))

        case .sort(let op):
            return .sort(SortOperator(
                input: apply(to: op.input),
                sortDescriptors: op.sortDescriptors,
                estimatedInputSize: op.estimatedInputSize
            ))

        case .project(let op):
            return .project(ProjectOperator(
                input: apply(to: op.input),
                fields: op.fields
            ))

        default:
            return plan
        }
    }

    private func tryPushLimit(
        limit: Int?,
        offset: Int?,
        into plan: PlanOperator<T>
    ) -> PlanOperator<T> {
        switch plan {
        case .indexScan:
            // Can push limit into index scan if ordering matches
            // For now, just return original - full optimization would check ordering
            return .limit(LimitOperator(
                input: plan,
                limit: limit,
                offset: offset
            ))

        case .filter:
            // Can't push limit past filter (filter might remove rows)
            return .limit(LimitOperator(
                input: plan,
                limit: limit,
                offset: offset
            ))

        case .sort:
            // Limit after sort is a top-N query, can't push further
            return .limit(LimitOperator(
                input: plan,
                limit: limit,
                offset: offset
            ))

        default:
            return .limit(LimitOperator(
                input: plan,
                limit: limit,
                offset: offset
            ))
        }
    }
}

// MARK: - Simplify Filter Rule

/// Simplify filter expressions and combine consecutive filters
public struct SimplifyFilterRule<T: Persistable>: OptimizationRule {

    public init() {}

    public func apply(to plan: PlanOperator<T>) -> PlanOperator<T> {
        switch plan {
        case .filter(let outerFilter):
            let optimizedInput = apply(to: outerFilter.input)

            // Check if input is also a filter - combine them
            if case .filter(let innerFilter) = optimizedInput {
                let combinedPredicate: Predicate<T> = .and([innerFilter.predicate, outerFilter.predicate])
                let combinedSelectivity = innerFilter.selectivity * outerFilter.selectivity

                return .filter(FilterOperator(
                    input: innerFilter.input,
                    predicate: combinedPredicate,
                    selectivity: combinedSelectivity
                ))
            }

            return .filter(FilterOperator(
                input: optimizedInput,
                predicate: outerFilter.predicate,
                selectivity: outerFilter.selectivity
            ))

        case .sort(let op):
            return .sort(SortOperator(
                input: apply(to: op.input),
                sortDescriptors: op.sortDescriptors,
                estimatedInputSize: op.estimatedInputSize
            ))

        case .limit(let op):
            return .limit(LimitOperator(
                input: apply(to: op.input),
                limit: op.limit,
                offset: op.offset
            ))

        case .project(let op):
            return .project(ProjectOperator(
                input: apply(to: op.input),
                fields: op.fields
            ))

        case .union(let op):
            return .union(UnionOperator(
                children: op.children.map { apply(to: $0) },
                deduplicate: op.deduplicate
            ))

        case .intersection(let op):
            return .intersection(IntersectionOperator(
                children: op.children.map { apply(to: $0) }
            ))

        default:
            return plan
        }
    }
}
