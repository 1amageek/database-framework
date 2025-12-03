// IndexPlanningStrategy.swift
// QueryPlanner - Index-specific planning strategies

import Foundation
import Core
import FoundationDB

/// Protocol for index-specific planning logic
public protocol IndexPlanningStrategy: Sendable {
    /// The index kind this strategy handles
    var indexKindIdentifier: String { get }

    /// Check which conditions this index can satisfy
    func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [any FieldConditionProtocol<T>],
        statistics: StatisticsProvider
    ) -> IndexMatchResult<T>

    /// Create the appropriate operator for this index
    func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T>

    /// Estimate the cost of using this index
    func estimateCost<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        statistics: StatisticsProvider,
        costModel: CostModel
    ) -> Double
}

// MARK: - Index Match Result

/// Result of matching conditions to an index
public struct IndexMatchResult<T: Persistable>: @unchecked Sendable {
    /// Conditions that can be fully satisfied by the index
    public let satisfiedConditions: [any FieldConditionProtocol<T>]

    /// Conditions that can be partially satisfied (filter during scan)
    public let partialConditions: [any FieldConditionProtocol<T>]

    /// Whether the index can satisfy ordering requirements
    public let satisfiesOrdering: Bool

    /// The scan bounds to use
    public let scanBounds: IndexScanBounds

    /// Estimated selectivity
    public let selectivity: Double

    /// Estimated number of entries to scan
    public let estimatedEntries: Int

    public init(
        satisfiedConditions: [any FieldConditionProtocol<T>],
        partialConditions: [any FieldConditionProtocol<T>] = [],
        satisfiesOrdering: Bool,
        scanBounds: IndexScanBounds,
        selectivity: Double,
        estimatedEntries: Int
    ) {
        self.satisfiedConditions = satisfiedConditions
        self.partialConditions = partialConditions
        self.satisfiesOrdering = satisfiesOrdering
        self.scanBounds = scanBounds
        self.selectivity = selectivity
        self.estimatedEntries = estimatedEntries
    }

    /// Empty match result (no conditions satisfied)
    public static func empty() -> IndexMatchResult<T> {
        IndexMatchResult<T>(
            satisfiedConditions: [],
            partialConditions: [],
            satisfiesOrdering: false,
            scanBounds: IndexScanBounds.unbounded,
            selectivity: 1.0,
            estimatedEntries: 0
        )
    }
}

// MARK: - Scalar Index Strategy

/// Planning strategy for scalar (B-tree style) indexes
public struct ScalarIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "scalar" }

    public init() {}

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [any FieldConditionProtocol<T>],
        statistics: StatisticsProvider
    ) -> IndexMatchResult<T> {
        var satisfied: [any FieldConditionProtocol<T>] = []
        var startBounds: [IndexScanBounds.BoundComponent] = []
        var endBounds: [IndexScanBounds.BoundComponent] = []
        var selectivity: Double = 1.0

        // Get the index key paths in order
        let indexKeyPaths = index.keyPaths

        // Match conditions to index prefix
        var prefixMatched = 0
        var rangeFound = false

        for keyPath in indexKeyPaths {
            if rangeFound { break } // Can't match past a range condition

            let fieldName = T.fieldName(for: keyPath)

            // Find condition for this field
            guard let condition = conditions.first(where: {
                $0.fieldName == fieldName
            }) else {
                break // Can't skip fields in index prefix
            }

            if condition.isEquality {
                // Equality extends the prefix
                satisfied.append(condition)
                let values = condition.constraintToTupleElements()
                if let value = values.first {
                    startBounds.append(IndexScanBounds.BoundComponent(value: value, inclusive: true))
                    endBounds.append(IndexScanBounds.BoundComponent(value: value, inclusive: true))
                }

                let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self) ?? 0.01
                selectivity *= eqSelectivity
                prefixMatched += 1

            } else if condition.isRange {
                // Range can only be on last matched field
                satisfied.append(condition)

                if let bounds = condition.rangeBoundsAsTupleElements() {
                    if let lower = bounds.lower {
                        startBounds.append(IndexScanBounds.BoundComponent(
                            value: lower.0,
                            inclusive: lower.1
                        ))
                    }
                    if let upper = bounds.upper {
                        endBounds.append(IndexScanBounds.BoundComponent(
                            value: upper.0,
                            inclusive: upper.1
                        ))
                    }

                    let rangeBound = RangeBound(
                        lower: bounds.lower.map { RangeBoundComponent(value: $0.0, inclusive: $0.1) },
                        upper: bounds.upper.map { RangeBoundComponent(value: $0.0, inclusive: $0.1) }
                    )
                    let rangeSelectivity = statistics.rangeSelectivity(field: fieldName, range: rangeBound, type: T.self) ?? 0.3
                    selectivity *= rangeSelectivity
                }
                prefixMatched += 1
                rangeFound = true

            } else if condition.isIn {
                // IN becomes multiple seeks - handle as multiple equality
                satisfied.append(condition)

                let values = condition.constraintToTupleElements()
                // For bounds, use first and last value (simplified)
                if let first = values.first {
                    startBounds.append(IndexScanBounds.BoundComponent(value: first, inclusive: true))
                }
                if let last = values.last {
                    endBounds.append(IndexScanBounds.BoundComponent(value: last, inclusive: true))
                }

                let eqSelectivity = statistics.equalitySelectivity(field: fieldName, type: T.self) ?? 0.01
                // Clamp IN selectivity to prevent distortion
                selectivity *= min(1.0, eqSelectivity * Double(condition.inValuesCount))
                prefixMatched += 1

            } else if let patternCondition = condition as? StringPatternFieldCondition<T>,
                      patternCondition.constraint.type == .prefix {
                // Prefix match can use B-tree index
                satisfied.append(condition)

                let prefixValue = patternCondition.constraint.pattern
                let endPrefix = patternCondition.constraint.pattern + "\u{FFFF}" // High character

                startBounds.append(IndexScanBounds.BoundComponent(value: prefixValue, inclusive: true))
                endBounds.append(IndexScanBounds.BoundComponent(value: endPrefix, inclusive: false))

                selectivity *= 0.1 // Rough estimate for prefix match
                prefixMatched += 1
                rangeFound = true

            } else {
                break // Other constraints can't use B-tree index
            }
        }

        let bounds = IndexScanBounds(start: startBounds, end: endBounds)
        let totalRows = statistics.estimatedRowCount(for: T.self)
        let estimatedEntries = max(1, Int(Double(totalRows) * selectivity))

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: prefixMatched > 0,
            scanBounds: bounds,
            selectivity: selectivity,
            estimatedEntries: estimatedEntries
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        // Check for IN condition -> might need union of seeks
        let hasInCondition = matchResult.satisfiedConditions.contains { $0.isIn }

        // Check for point lookup (all equality on full prefix)
        let isPointLookup = matchResult.satisfiedConditions.allSatisfy { $0.isEquality } &&
            matchResult.satisfiedConditions.count == index.keyPaths.count

        if isPointLookup && !hasInCondition {
            // Single point lookup
            let seekValues: [any TupleElement] = matchResult.satisfiedConditions.flatMap { $0.constraintToTupleElements() }
            return .indexSeek(IndexSeekOperator(
                index: index,
                seekValues: [seekValues],
                satisfiedConditions: matchResult.satisfiedConditions
            ))
        }

        if hasInCondition {
            // Create multi-seek for IN condition
            return createMultiSeekOperator(index: index, matchResult: matchResult)
        }

        // Range scan
        let reverse = shouldScanReverse(analysis: analysis, index: index)
        return .indexScan(IndexScanOperator(
            index: index,
            bounds: matchResult.scanBounds,
            reverse: reverse,
            satisfiedConditions: matchResult.satisfiedConditions,
            estimatedEntries: matchResult.estimatedEntries
        ))
    }

    public func estimateCost<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        statistics: StatisticsProvider,
        costModel: CostModel
    ) -> Double {
        let entries = Double(matchResult.estimatedEntries)
        return costModel.indexCost(entries: entries) + costModel.fetchCost(records: entries)
    }

    // MARK: - Private Helpers

    private func createMultiSeekOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>
    ) -> PlanOperator<T> {
        // Build seek combinations: each combination is (seekValues, conditions)
        // where conditions are the specific conditions for that seek
        var combinations: [([any TupleElement], [any FieldConditionProtocol<T>])] = [([], [])]

        for condition in matchResult.satisfiedConditions {
            if condition.isEquality {
                // Append value and condition to all existing combinations
                let values = condition.constraintToTupleElements()
                combinations = combinations.map { (existingValues, existingConditions) in
                    (existingValues + values, existingConditions + [condition])
                }
            } else if condition.isIn {
                // Expand: for each existing combination, create one per IN value
                let inValues = condition.constraintToTupleElements()
                var newCombinations: [([any TupleElement], [any FieldConditionProtocol<T>])] = []
                for (existingValues, existingConditions) in combinations {
                    for value in inValues {
                        // Create an equality condition for this specific value
                        let eqCondition = ScalarFieldCondition<T>.equals(
                            field: FieldReference<T>(anyKeyPath: condition.keyPath, fieldName: condition.fieldName),
                            value: value,
                            predicate: condition.predicate
                        )
                        newCombinations.append((
                            existingValues + [value],
                            existingConditions + [eqCondition]
                        ))
                    }
                }
                combinations = newCombinations
            } else {
                // Other constraints don't contribute to seek values
                // but should be included in conditions for each seek
                combinations = combinations.map { (values, conditions) in
                    (values, conditions + [condition])
                }
            }
        }

        if combinations.count == 1 {
            // Single seek
            let (seekValues, conditions) = combinations[0]
            return .indexSeek(IndexSeekOperator(
                index: index,
                seekValues: [seekValues],
                satisfiedConditions: conditions
            ))
        }

        // Multiple seeks - wrap in union to combine results
        // Note: deduplicate=false because each IN value targets distinct index entries
        // and won't produce duplicates (unlike OR conditions that may overlap)
        let seeks = combinations.map { (seekValues, conditions) in
            PlanOperator<T>.indexSeek(IndexSeekOperator(
                index: index,
                seekValues: [seekValues],
                satisfiedConditions: conditions
            ))
        }

        return .union(UnionOperator(children: seeks, deduplicate: false))
    }

    private func shouldScanReverse<T: Persistable>(
        analysis: QueryAnalysis<T>,
        index: IndexDescriptor
    ) -> Bool {
        guard let firstSort = analysis.sortRequirements.first else { return false }

        // Check if first sort field matches first index field
        guard let firstIndexKeyPath = index.keyPaths.first else { return false }
        let firstIndexField = T.fieldName(for: firstIndexKeyPath)

        if firstSort.fieldName == firstIndexField && firstSort.order == .descending {
            return true
        }

        return false
    }
}

// MARK: - Full Text Index Strategy

/// Planning strategy for full-text indexes
public struct FullTextIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "fulltext" }

    public init() {}

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [any FieldConditionProtocol<T>],
        statistics: StatisticsProvider
    ) -> IndexMatchResult<T> {
        var satisfied: [any FieldConditionProtocol<T>] = []

        guard let firstKeyPath = index.keyPaths.first else {
            return .empty()
        }
        let indexedField = T.fieldName(for: firstKeyPath)

        for condition in conditions {
            guard condition.fieldName == indexedField else { continue }

            // Check for text search condition
            if condition is TextSearchFieldCondition<T> {
                satisfied.append(condition)
            }
            // Check for string pattern with contains type
            else if let patternCondition = condition as? StringPatternFieldCondition<T>,
                    patternCondition.constraint.type == .contains {
                satisfied.append(condition)
            }
        }

        guard !satisfied.isEmpty else { return .empty() }

        let totalRows = statistics.estimatedRowCount(for: T.self)
        let selectivity = 0.05 // Rough estimate for text search
        let estimatedEntries = max(1, Int(Double(totalRows) * selectivity))

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: false, // Full-text doesn't preserve order
            scanBounds: .unbounded,
            selectivity: selectivity,
            estimatedEntries: estimatedEntries
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard let condition = matchResult.satisfiedConditions.first else {
            fatalError("FullTextIndexStrategy requires at least one condition")
        }

        let searchTerms: [String]
        let matchMode: TextMatchMode

        if let textCondition = condition as? TextSearchFieldCondition<T> {
            searchTerms = textCondition.constraint.terms
            matchMode = textCondition.constraint.matchMode
        } else if let patternCondition = condition as? StringPatternFieldCondition<T> {
            searchTerms = [patternCondition.constraint.pattern]
            matchMode = .any
        } else {
            fatalError("Unexpected constraint type for full-text index")
        }

        return .fullTextScan(FullTextScanOperator(
            index: index,
            searchTerms: searchTerms,
            matchMode: matchMode,
            estimatedResults: matchResult.estimatedEntries
        ))
    }

    public func estimateCost<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        statistics: StatisticsProvider,
        costModel: CostModel
    ) -> Double {
        let entries = Double(matchResult.estimatedEntries)
        return costModel.indexCost(entries: entries) + costModel.fetchCost(records: entries)
    }
}

// MARK: - Vector Index Strategy

/// Planning strategy for vector similarity indexes
public struct VectorIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "vector" }

    public init() {}

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [any FieldConditionProtocol<T>],
        statistics: StatisticsProvider
    ) -> IndexMatchResult<T> {
        var satisfied: [any FieldConditionProtocol<T>] = []

        guard let firstKeyPath = index.keyPaths.first else {
            return .empty()
        }
        let indexedField = T.fieldName(for: firstKeyPath)

        for condition in conditions {
            guard condition.fieldName == indexedField else { continue }

            if condition is VectorFieldCondition<T> {
                satisfied.append(condition)
            }
        }

        guard !satisfied.isEmpty else { return .empty() }

        // Vector search returns exactly k results
        let k: Int
        if let vectorCondition = satisfied.first as? VectorFieldCondition<T> {
            k = vectorCondition.constraint.k
        } else {
            k = 10
        }

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: true, // Results ordered by similarity
            scanBounds: .unbounded,
            selectivity: 1.0, // Always returns k results
            estimatedEntries: k
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard let vectorCondition = matchResult.satisfiedConditions.first as? VectorFieldCondition<T> else {
            fatalError("VectorIndexStrategy requires vector similarity condition")
        }

        return .vectorSearch(VectorSearchOperator(
            index: index,
            queryVector: vectorCondition.constraint.queryVector,
            k: vectorCondition.constraint.k,
            distanceMetric: vectorCondition.constraint.metric,
            efSearch: vectorCondition.constraint.efSearch
        ))
    }

    public func estimateCost<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        statistics: StatisticsProvider,
        costModel: CostModel
    ) -> Double {
        // HNSW search is efficient - log(N) * ef_search
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))
        let k = Double(matchResult.estimatedEntries)
        let efSearch = k * 10 // Default ef_search

        let searchCost = log2(max(2, totalRows)) * efSearch * 0.1
        return searchCost + costModel.fetchCost(records: k)
    }
}

// MARK: - Spatial Index Strategy

/// Planning strategy for spatial indexes
public struct SpatialIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "spatial" }

    public init() {}

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [any FieldConditionProtocol<T>],
        statistics: StatisticsProvider
    ) -> IndexMatchResult<T> {
        var satisfied: [any FieldConditionProtocol<T>] = []

        guard let firstKeyPath = index.keyPaths.first else {
            return .empty()
        }
        let indexedField = T.fieldName(for: firstKeyPath)

        for condition in conditions {
            guard condition.fieldName == indexedField else { continue }

            if condition is SpatialFieldCondition<T> {
                satisfied.append(condition)
            }
        }

        guard !satisfied.isEmpty else { return .empty() }

        let totalRows = statistics.estimatedRowCount(for: T.self)
        let selectivity = 0.1 // Rough estimate for spatial queries
        let estimatedEntries = max(1, Int(Double(totalRows) * selectivity))

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: false,
            scanBounds: .unbounded,
            selectivity: selectivity,
            estimatedEntries: estimatedEntries
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard let spatialCondition = matchResult.satisfiedConditions.first as? SpatialFieldCondition<T> else {
            fatalError("SpatialIndexStrategy requires spatial condition")
        }

        return .spatialScan(SpatialScanOperator(
            index: index,
            constraint: spatialCondition.constraint,
            estimatedResults: matchResult.estimatedEntries
        ))
    }

    public func estimateCost<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        statistics: StatisticsProvider,
        costModel: CostModel
    ) -> Double {
        let entries = Double(matchResult.estimatedEntries)
        // R-tree traversal cost
        return costModel.indexCost(entries: entries * 2) + costModel.fetchCost(records: entries)
    }
}

// MARK: - Strategy Registry

/// Registry for index planning strategies
public struct IndexStrategyRegistry: Sendable {
    /// Available strategies by kind identifier
    private let strategies: [String: any IndexPlanningStrategy]

    public init(strategies: [any IndexPlanningStrategy] = Self.defaultStrategies()) {
        var dict: [String: any IndexPlanningStrategy] = [:]
        for strategy in strategies {
            dict[strategy.indexKindIdentifier] = strategy
        }
        self.strategies = dict
    }

    /// Get strategy for an index
    public func strategy(for index: IndexDescriptor) -> (any IndexPlanningStrategy)? {
        strategies[index.kindIdentifier]
    }

    /// Get strategy by kind identifier
    public func strategy(forKind identifier: String) -> (any IndexPlanningStrategy)? {
        strategies[identifier]
    }

    /// Default set of strategies
    public static func defaultStrategies() -> [any IndexPlanningStrategy] {
        [
            ScalarIndexStrategy(),
            FullTextIndexStrategy(),
            VectorIndexStrategy(),
            SpatialIndexStrategy()
        ]
    }
}
