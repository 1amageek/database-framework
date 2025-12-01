# Query Planner / Optimizer Design

## Overview

This document describes the design of a cost-based Query Planner / Optimizer for database-framework. The design draws inspiration from fdb-record-layer's query system while adapting to Swift's type system and the existing codebase architecture.

## Dependencies

```swift
// QueryPlanner imports types from database-kit
import Core  // IndexDescriptor, Persistable, IndexKind, etc.
```

**Key Type References**:
- `IndexDescriptor` - Defined in `database-kit/Sources/Core/IndexDescriptor.swift`
- `Persistable` - Defined in `database-kit/Sources/Core/Persistable.swift`
  - **Important**: `Persistable.ID` has constraint `Sendable & Hashable & Codable`
  - This enables reliable ID-based deduplication in Union/Intersection operations

## Goals

1. **Automatic Index Selection** - Choose optimal indexes based on predicates and ordering
2. **Cost-Based Optimization** - Estimate execution costs and select the cheapest plan
3. **Composite Index Support** - Leverage multi-field indexes effectively
4. **Extensible Architecture** - Support all 11 index types with specialized strategies
5. **Backward Compatibility** - Integrate seamlessly with existing fluent API

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         User Query (Fluent API)                      │
│  context.fetch(User.self).where(...).orderBy(...).execute()         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      QueryAnalyzer                                   │
│  - Parse predicates into QueryConditions                            │
│  - Normalize logical expressions (CNF/DNF)                          │
│  - Extract field requirements                                        │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PlanEnumerator                                  │
│  - Generate candidate execution plans                                │
│  - Consider: TableScan, IndexScan, IndexSeek, UnionPlan, etc.       │
│  - Apply index-specific strategies                                   │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CostEstimator                                   │
│  - Estimate selectivity of predicates                               │
│  - Calculate I/O costs (reads, seeks)                               │
│  - Account for post-filtering overhead                              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PlanOptimizer                                   │
│  - Rank plans by estimated cost                                     │
│  - Apply heuristic rules                                            │
│  - Select optimal plan                                              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      QueryPlan (Output)                              │
│  - Executable plan with operators                                    │
│  - Estimated cost metrics                                           │
│  - Execution strategy                                               │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PlanExecutor                                    │
│  - Execute the chosen plan                                          │
│  - Stream results efficiently                                        │
│  - Collect execution statistics                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Module Structure

```
Sources/QueryPlanner/
├── QueryPlanner.swift              # Main entry point
├── Analysis/
│   ├── QueryAnalyzer.swift         # Predicate analysis
│   ├── QueryCondition.swift        # Normalized condition representation
│   ├── FieldRequirement.swift      # Field access patterns
│   └── PredicateNormalizer.swift   # CNF/DNF conversion
├── Planning/
│   ├── PlanEnumerator.swift        # Plan generation
│   ├── QueryPlan.swift             # Plan representation
│   ├── PlanOperator.swift          # Execution operators
│   └── IndexMatcher.swift          # Index-predicate matching
├── Costing/
│   ├── CostEstimator.swift         # Cost calculation
│   ├── CostModel.swift             # Cost parameters
│   ├── SelectivityEstimator.swift  # Predicate selectivity
│   └── Statistics.swift            # Table/index statistics
├── Optimization/
│   ├── PlanOptimizer.swift         # Plan selection
│   ├── OptimizationRule.swift      # Heuristic rules
│   └── PlanComparator.swift        # Plan ranking
├── Execution/
│   ├── PlanExecutor.swift          # Plan execution
│   └── ExecutionContext.swift      # Runtime context
└── IndexStrategies/
    ├── ScalarIndexStrategy.swift   # B-tree index strategy
    ├── FullTextIndexStrategy.swift # Inverted index strategy
    ├── VectorIndexStrategy.swift   # Vector similarity strategy
    ├── SpatialIndexStrategy.swift  # Geospatial strategy
    └── AggregationStrategy.swift   # Aggregation index strategy
```

---

## Core Types

### 1. QueryCondition (Normalized Predicate)

```swift
/// Normalized representation of query conditions for planning
public enum QueryCondition<T: Persistable>: Sendable {
    /// Single field condition
    case field(FieldCondition<T>)

    /// Conjunction (AND) - all must be true
    case conjunction([QueryCondition<T>])

    /// Disjunction (OR) - at least one must be true
    case disjunction([QueryCondition<T>])

    /// Always true (no filter)
    case alwaysTrue

    /// Always false (empty result)
    case alwaysFalse
}

/// Represents a condition on a single field
public struct FieldCondition<T: Persistable>: Sendable {
    /// The field being compared
    public let field: FieldReference<T>

    /// The type of condition
    public let constraint: FieldConstraint

    /// Original predicate for post-filtering if needed
    public let sourcePredicate: Predicate<T>?
}

/// Reference to a field in a model
public struct FieldReference<T: Persistable>: Sendable, Hashable {
    public let keyPath: AnyKeyPath
    public let fieldName: String
    public let fieldType: Any.Type

    public init<V>(_ keyPath: KeyPath<T, V>) {
        self.keyPath = keyPath
        self.fieldName = T.fieldName(for: keyPath)
        self.fieldType = V.self
    }
}

/// Types of constraints on a field
public enum FieldConstraint: Sendable {
    /// Exact equality: field = value
    case equals(AnySendable)

    /// Not equal: field != value
    case notEquals(AnySendable)

    /// Range: field > lower AND field < upper
    case range(Range)

    /// Membership: field IN [values]
    case `in`([AnySendable])

    /// Null check: field IS NULL / IS NOT NULL
    case isNull(Bool)

    /// Text search: full-text match
    case textSearch(TextSearchConstraint)

    /// Spatial: within distance/bounds
    case spatial(SpatialConstraint)

    /// Vector similarity: nearest neighbors
    case vectorSimilarity(VectorConstraint)

    /// String pattern: LIKE, PREFIX, SUFFIX, CONTAINS
    case stringPattern(StringPatternConstraint)
}

/// Range constraint with bounds
public struct Range: Sendable {
    public let lower: Bound?
    public let upper: Bound?

    public struct Bound: Sendable {
        public let value: AnySendable
        public let inclusive: Bool
    }
}
```

### 2. QueryPlan (Execution Plan)

```swift
/// Represents an executable query plan
public struct QueryPlan<T: Persistable>: Sendable {
    /// Unique identifier for this plan
    public let id: UUID

    /// Root operator of the plan tree
    public let rootOperator: PlanOperator<T>

    /// Estimated cost metrics
    public let estimatedCost: PlanCost

    /// Fields used in the plan
    public let usedFields: Set<String>

    /// Indexes used in the plan
    public let usedIndexes: [IndexDescriptor]

    /// Whether ordering is satisfied by the plan
    public let orderingSatisfied: Bool

    /// Post-filter predicate (if any conditions couldn't use indexes)
    public let postFilterPredicate: Predicate<T>?

    /// Human-readable explanation
    public var explanation: String { ... }
}

/// Cost metrics for a query plan
public struct PlanCost: Sendable, Comparable {
    /// Estimated number of index entries to read
    public let indexReads: Double

    /// Estimated number of records to fetch
    public let recordFetches: Double

    /// Estimated number of records to post-filter
    public let postFilterCount: Double

    /// Whether in-memory sorting is required
    public let requiresSort: Bool

    /// Total estimated cost (weighted sum)
    public var totalCost: Double {
        let indexReadCost = indexReads * CostModel.indexReadWeight
        let recordFetchCost = recordFetches * CostModel.recordFetchWeight
        let postFilterCost = postFilterCount * CostModel.postFilterWeight
        let sortCost = requiresSort ? (recordFetches * CostModel.sortWeight) : 0
        return indexReadCost + recordFetchCost + postFilterCost + sortCost
    }

    public static func < (lhs: PlanCost, rhs: PlanCost) -> Bool {
        lhs.totalCost < rhs.totalCost
    }
}
```

### 3. PlanOperator (Execution Operators)

```swift
/// Operators that make up a query plan
public indirect enum PlanOperator<T: Persistable>: Sendable {

    // === Scan Operators ===

    /// Full table scan - reads all records
    case tableScan(TableScanOperator<T>)

    /// Index range scan - reads a range of index entries
    case indexScan(IndexScanOperator<T>)

    /// Index seek - point lookup(s) in index
    case indexSeek(IndexSeekOperator<T>)

    // === Join/Combine Operators ===

    /// Union of multiple plans (for OR conditions)
    case union(UnionOperator<T>)

    /// Intersection of multiple plans (for AND with multiple indexes)
    case intersection(IntersectionOperator<T>)

    // === Transform Operators ===

    /// Filter records by predicate
    case filter(FilterOperator<T>)

    /// Sort records
    case sort(SortOperator<T>)

    /// Limit/offset results
    case limit(LimitOperator<T>)

    /// Project specific fields (for covering index optimization)
    case project(ProjectOperator<T>)

    // === Specialized Index Operators ===

    /// Full-text search scan
    case fullTextScan(FullTextScanOperator<T>)

    /// Vector similarity search
    case vectorSearch(VectorSearchOperator<T>)

    /// Spatial region scan
    case spatialScan(SpatialScanOperator<T>)

    /// Aggregation from index
    case aggregation(AggregationOperator<T>)
}

/// Table scan operator
public struct TableScanOperator<T: Persistable>: Sendable {
    /// Estimated row count
    public let estimatedRows: Int

    /// Optional predicate to apply during scan
    public let filterPredicate: Predicate<T>?
}

/// Index scan operator
public struct IndexScanOperator<T: Persistable>: Sendable {
    /// The index to scan
    public let index: IndexDescriptor

    /// Scan bounds
    public let bounds: IndexScanBounds

    /// Whether to scan in reverse
    public let reverse: Bool

    /// Conditions satisfied by this scan
    public let satisfiedConditions: [FieldCondition<T>]

    /// Estimated matching entries
    public let estimatedEntries: Int
}

/// Bounds for index scan
public struct IndexScanBounds: Sendable {
    /// Starting key components (inclusive/exclusive)
    public let start: [BoundComponent]

    /// Ending key components (inclusive/exclusive)
    public let end: [BoundComponent]

    public struct BoundComponent: Sendable {
        public let value: AnySendable?
        public let inclusive: Bool
    }

    /// Full index scan (no bounds)
    public static let unbounded = IndexScanBounds(start: [], end: [])
}

/// Index seek operator (point lookups)
public struct IndexSeekOperator<T: Persistable>: Sendable {
    /// The index to seek in
    public let index: IndexDescriptor

    /// Values to seek
    public let seekValues: [[AnySendable]]

    /// Conditions satisfied by this seek
    public let satisfiedConditions: [FieldCondition<T>]
}

/// Union operator (OR)
///
/// **IMPORTANT**: Union output is UNORDERED. Results from parallel child
/// execution are merged without preserving any specific order. If ordering
/// is required, the PlanEnumerator will wrap this operator with a SortOperator.
///
/// Deduplication uses `Persistable.ID` (which is `Hashable`) to identify
/// duplicate records across children.
public struct UnionOperator<T: Persistable>: Sendable {
    /// Child plans to union
    public let children: [PlanOperator<T>]

    /// Whether to deduplicate results
    /// When true, uses Set<AnyHashable> with item.id for O(1) dedup
    public let deduplicate: Bool
}

/// Intersection operator (AND with multiple indexes)
public struct IntersectionOperator<T: Persistable>: Sendable {
    /// Child plans to intersect
    public let children: [PlanOperator<T>]
}

/// Filter operator
public struct FilterOperator<T: Persistable>: Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Predicate to apply
    public let predicate: Predicate<T>

    /// Estimated selectivity (0.0 - 1.0)
    public let selectivity: Double
}

/// Sort operator
public struct SortOperator<T: Persistable>: Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Sort descriptors
    public let sortDescriptors: [SortDescriptor<T>]

    /// Estimated input size
    public let estimatedInputSize: Int
}

/// Limit operator
public struct LimitOperator<T: Persistable>: Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Maximum rows to return
    public let limit: Int?

    /// Rows to skip
    public let offset: Int?
}
```

### 4. QueryPlanner (Main Entry Point)

```swift
/// Main query planner that coordinates analysis, planning, and optimization
public final class QueryPlanner<T: Persistable>: Sendable {

    /// Available indexes for the type
    private let availableIndexes: [IndexDescriptor]

    /// Statistics provider for cost estimation
    private let statistics: StatisticsProvider

    /// Cost model configuration
    private let costModel: CostModel

    /// Index-specific planning strategies
    private let indexStrategies: [String: any IndexPlanningStrategy]

    public init(
        indexes: [IndexDescriptor],
        statistics: StatisticsProvider = DefaultStatisticsProvider(),
        costModel: CostModel = .default
    ) {
        self.availableIndexes = indexes
        self.statistics = statistics
        self.costModel = costModel
        self.indexStrategies = Self.buildIndexStrategies(for: indexes)
    }

    /// Plan a query and return the optimal execution plan
    public func plan(query: Query<T>) throws -> QueryPlan<T> {
        // 1. Analyze the query
        let analyzer = QueryAnalyzer<T>()
        let analysis = try analyzer.analyze(query)

        // 2. Enumerate candidate plans
        let enumerator = PlanEnumerator(
            indexes: availableIndexes,
            strategies: indexStrategies,
            statistics: statistics
        )
        let candidates = try enumerator.enumerate(analysis: analysis)

        // 3. Estimate costs for each plan
        let estimator = CostEstimator(
            statistics: statistics,
            costModel: costModel
        )
        let costedPlans = candidates.map { plan in
            (plan, estimator.estimate(plan: plan, analysis: analysis))
        }

        // 4. Select optimal plan
        let optimizer = PlanOptimizer(costModel: costModel)
        guard let (optimalPlan, cost) = optimizer.selectBest(costedPlans) else {
            // Fallback to table scan
            return createTableScanPlan(query: query)
        }

        return QueryPlan(
            id: UUID(),
            rootOperator: optimalPlan,
            estimatedCost: cost,
            usedFields: analysis.referencedFields,
            usedIndexes: extractUsedIndexes(optimalPlan),
            orderingSatisfied: checkOrderingSatisfied(optimalPlan, query: query),
            postFilterPredicate: computePostFilter(optimalPlan, analysis: analysis)
        )
    }

    /// Explain the plan without executing
    public func explain(query: Query<T>) throws -> PlanExplanation {
        let plan = try plan(query: query)
        return PlanExplanation(plan: plan)
    }
}
```

---

## Analysis Phase

### QueryAnalyzer

```swift
/// Analyzes a query to extract structured information for planning
public struct QueryAnalyzer<T: Persistable> {

    /// Analyze a query and produce a QueryAnalysis
    public func analyze(_ query: Query<T>) throws -> QueryAnalysis<T> {
        // Combine all predicates
        let combinedPredicate = combinedPredicate(from: query.predicates)

        // Normalize to Conjunctive Normal Form for easier planning
        let normalizer = PredicateNormalizer<T>()
        let normalized = normalizer.toCNF(combinedPredicate)

        // Extract conditions
        let conditions = extractConditions(from: normalized)

        // Identify field requirements
        let fieldRequirements = extractFieldRequirements(
            conditions: conditions,
            sortDescriptors: query.sortDescriptors
        )

        // Detect special query patterns
        let patterns = detectQueryPatterns(conditions: conditions)

        return QueryAnalysis(
            originalPredicate: combinedPredicate,
            normalizedCondition: normalized,
            fieldConditions: conditions,
            fieldRequirements: fieldRequirements,
            sortRequirements: query.sortDescriptors,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            detectedPatterns: patterns,
            referencedFields: extractReferencedFields(conditions: conditions)
        )
    }
}

/// Result of query analysis
public struct QueryAnalysis<T: Persistable>: Sendable {
    /// Original combined predicate
    public let originalPredicate: Predicate<T>?

    /// Normalized condition tree
    public let normalizedCondition: QueryCondition<T>

    /// Flat list of field conditions
    public let fieldConditions: [FieldCondition<T>]

    /// Requirements per field
    public let fieldRequirements: [String: FieldRequirement]

    /// Sort requirements
    public let sortRequirements: [SortDescriptor<T>]

    /// Limit/offset
    public let limit: Int?
    public let offset: Int?

    /// Detected query patterns
    public let detectedPatterns: Set<QueryPattern>

    /// All referenced field names
    public let referencedFields: Set<String>
}

/// Detected query patterns that may influence planning
public enum QueryPattern: Sendable {
    /// Single equality condition (point lookup)
    case pointLookup

    /// Range query on ordered field
    case rangeQuery

    /// Multiple IN conditions (multi-seek)
    case multiValueLookup

    /// Full-text search present
    case fullTextSearch

    /// Vector similarity search present
    case vectorSearch

    /// Spatial query present
    case spatialQuery

    /// Aggregation query (COUNT, SUM, etc.)
    case aggregation(AggregationType)

    /// Top-N query (ORDER BY with LIMIT)
    case topN

    /// Pagination query (OFFSET present)
    case pagination
}

/// Requirements for a specific field
public struct FieldRequirement: Sendable {
    /// Field name
    public let fieldName: String

    /// Types of access needed
    public let accessTypes: Set<FieldAccessType>

    /// Constraints on this field
    public let constraints: [FieldConstraint]

    /// Whether this field is used in ordering
    public let usedInOrdering: Bool

    /// Order direction if used in ordering
    public let orderDirection: SortOrder?
}

public enum FieldAccessType: Sendable {
    case equality      // =
    case inequality    // !=
    case range         // <, <=, >, >=
    case membership    // IN
    case pattern       // LIKE, CONTAINS, PREFIX
    case ordering      // ORDER BY
    case textSearch    // Full-text
    case spatial       // Geo
    case vector        // Similarity
}
```

---

## Plan Enumeration

### PlanEnumerator

```swift
/// Generates candidate execution plans for a query
public struct PlanEnumerator<T: Persistable> {

    private let indexes: [IndexDescriptor]
    private let strategies: [String: any IndexPlanningStrategy]
    private let statistics: StatisticsProvider

    /// Enumerate all reasonable candidate plans
    public func enumerate(analysis: QueryAnalysis<T>) throws -> [PlanOperator<T>] {
        var candidates: [PlanOperator<T>] = []

        // Always include table scan as fallback
        candidates.append(createTableScan(analysis: analysis))

        // Try single-index plans
        for index in indexes {
            if let plan = tryCreateIndexPlan(
                index: index,
                analysis: analysis
            ) {
                candidates.append(plan)
            }
        }

        // Try composite index plans (covering multiple conditions)
        candidates.append(contentsOf: tryCompositeIndexPlans(analysis: analysis))

        // Try index intersection plans (AND with multiple indexes)
        if analysis.detectedPatterns.contains(.pointLookup) {
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

    /// Try to create an index plan for a single index
    private func tryCreateIndexPlan(
        index: IndexDescriptor,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T>? {
        // Get the appropriate strategy for this index type
        guard let strategy = strategies[index.kind.identifier] else {
            return nil
        }

        // Check if index can satisfy any conditions
        let matchResult = strategy.matchConditions(
            index: index,
            conditions: analysis.fieldConditions
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
            !matchResult.satisfiedConditions.contains(where: { $0.field == condition.field })
        }

        if unsatisfied.isEmpty {
            return indexOp
        } else {
            let filterPredicate = rebuildPredicate(from: unsatisfied)
            return .filter(FilterOperator(
                input: indexOp,
                predicate: filterPredicate,
                selectivity: estimateSelectivity(unsatisfied)
            ))
        }
    }

    /// Try to use multiple indexes together with intersection
    ///
    /// Uses scoring to rank candidate indexes by selectivity and uniqueness,
    /// preferring more selective indexes for intersection operations.
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

            // Stop if we have enough for intersection
            if selectedPlans.count >= 3 { break }
        }

        // Need at least 2 indexes for intersection
        guard selectedPlans.count >= 2 else { return [] }

        return [.intersection(IntersectionOperator(children: selectedPlans))]
    }

    /// Find all indexes that could satisfy a condition
    private func findCandidateIndexes<T: Persistable>(
        for condition: FieldCondition<T>
    ) -> [IndexDescriptor] {
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
    private func scoreIndex<T: Persistable>(
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

    /// Try to use union for OR conditions
    private func tryUnionPlans(analysis: QueryAnalysis<T>) -> [PlanOperator<T>] {
        guard case .disjunction(let disjuncts) = analysis.normalizedCondition else {
            return []
        }

        var childPlans: [PlanOperator<T>] = []

        for disjunct in disjuncts {
            let subAnalysis = QueryAnalysis(
                originalPredicate: analysis.originalPredicate,
                normalizedCondition: disjunct,
                fieldConditions: extractConditions(from: disjunct),
                fieldRequirements: analysis.fieldRequirements,
                sortRequirements: [],
                limit: nil,
                offset: nil,
                detectedPatterns: [],
                referencedFields: analysis.referencedFields
            )

            // Try to find an index plan for each disjunct
            for index in indexes {
                if let plan = tryCreateIndexPlan(index: index, analysis: subAnalysis) {
                    childPlans.append(plan)
                    break
                }
            }
        }

        // Only use union if all disjuncts can use indexes
        if childPlans.count == disjuncts.count {
            return [.union(UnionOperator(children: childPlans, deduplicate: true))]
        }

        return []
    }
}
```

---

## Index Planning Strategies

### IndexPlanningStrategy Protocol

```swift
/// Protocol for index-specific planning logic
public protocol IndexPlanningStrategy: Sendable {
    /// The index kind this strategy handles
    var indexKindIdentifier: String { get }

    /// Check which conditions this index can satisfy
    func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [FieldCondition<T>]
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
        statistics: StatisticsProvider
    ) -> Double
}

/// Result of matching conditions to an index
public struct IndexMatchResult<T: Persistable>: Sendable {
    /// Conditions that can be fully satisfied by the index
    public let satisfiedConditions: [FieldCondition<T>]

    /// Conditions that can be partially satisfied (filter during scan)
    public let partialConditions: [FieldCondition<T>]

    /// Whether the index can satisfy ordering requirements
    public let satisfiesOrdering: Bool

    /// The scan bounds to use
    public let scanBounds: IndexScanBounds

    /// Estimated selectivity
    public let selectivity: Double
}
```

### ScalarIndexStrategy

```swift
/// Planning strategy for scalar (B-tree style) indexes
public struct ScalarIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "scalar" }

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [FieldCondition<T>]
    ) -> IndexMatchResult<T> {
        var satisfied: [FieldCondition<T>] = []
        var bounds = IndexScanBounds.unbounded
        var selectivity: Double = 1.0

        // Get the index key paths in order
        let indexKeyPaths = index.keyPaths

        // Match conditions to index prefix
        var prefixMatched = 0
        for (i, keyPath) in indexKeyPaths.enumerated() {
            let fieldName = T.fieldName(for: keyPath)

            // Find condition for this field
            guard let condition = conditions.first(where: {
                $0.field.fieldName == fieldName
            }) else {
                break // Can't skip fields in index prefix
            }

            switch condition.constraint {
            case .equals(let value):
                // Equality extends the prefix
                satisfied.append(condition)
                bounds = extendBounds(bounds, with: value, at: i, equality: true)
                selectivity *= estimateEqualitySelectivity(value)
                prefixMatched += 1

            case .range(let range):
                // Range can only be on last matched field
                satisfied.append(condition)
                bounds = extendBounds(bounds, with: range, at: i)
                selectivity *= estimateRangeSelectivity(range)
                break // Can't match more after range

            case .in(let values):
                // IN becomes multiple seeks or union
                satisfied.append(condition)
                // Clamp selectivity to [0.0, 1.0] to prevent cost distortion
                let inSelectivity = min(1.0, Double(values.count) * estimateEqualitySelectivity(values.first!))
                selectivity *= inSelectivity
                prefixMatched += 1

            default:
                break // Other constraints can't use B-tree index
            }
        }

        // Check if ordering is satisfied
        let satisfiesOrdering = checkOrderingSatisfied(
            indexKeyPaths: indexKeyPaths,
            prefixMatched: prefixMatched
        )

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: satisfiesOrdering,
            scanBounds: bounds,
            selectivity: selectivity
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        // Check for IN condition -> multiple seeks
        let hasInCondition = matchResult.satisfiedConditions.contains { condition in
            if case .in = condition.constraint { return true }
            return false
        }

        if hasInCondition {
            return createMultiSeekOperator(index: index, matchResult: matchResult)
        }

        // Check for point lookup (all equality on full prefix)
        let isPointLookup = matchResult.satisfiedConditions.allSatisfy { condition in
            if case .equals = condition.constraint { return true }
            return false
        } && matchResult.satisfiedConditions.count == index.keyPaths.count

        if isPointLookup {
            return .indexSeek(IndexSeekOperator(
                index: index,
                seekValues: [matchResult.satisfiedConditions.map { $0.constraintValue }],
                satisfiedConditions: matchResult.satisfiedConditions
            ))
        }

        // Range scan
        let reverse = shouldScanReverse(analysis: analysis, index: index)
        return .indexScan(IndexScanOperator(
            index: index,
            bounds: matchResult.scanBounds,
            reverse: reverse,
            satisfiedConditions: matchResult.satisfiedConditions,
            estimatedEntries: estimateEntries(matchResult: matchResult)
        ))
    }
}
```

### FullTextIndexStrategy

```swift
/// Planning strategy for full-text indexes
public struct FullTextIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "fulltext" }

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [FieldCondition<T>]
    ) -> IndexMatchResult<T> {
        var satisfied: [FieldCondition<T>] = []

        for condition in conditions {
            // Check if this condition is on the indexed field
            guard condition.field.fieldName == index.keyPaths.first.map({ T.fieldName(for: $0) }) else {
                continue
            }

            switch condition.constraint {
            case .textSearch(let textConstraint):
                satisfied.append(condition)

            case .stringPattern(let pattern) where pattern.type == .contains:
                // Can use inverted index for contains
                satisfied.append(condition)

            default:
                break
            }
        }

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: false, // Full-text doesn't preserve order
            scanBounds: .unbounded,
            selectivity: estimateTextSelectivity(satisfied)
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard let condition = matchResult.satisfiedConditions.first,
              case .textSearch(let constraint) = condition.constraint else {
            fatalError("FullTextIndexStrategy requires text search condition")
        }

        return .fullTextScan(FullTextScanOperator(
            index: index,
            searchTerms: constraint.terms,
            matchMode: constraint.matchMode,
            estimatedResults: estimateResults(constraint: constraint)
        ))
    }
}
```

### VectorIndexStrategy

```swift
/// Planning strategy for vector similarity indexes
public struct VectorIndexStrategy: IndexPlanningStrategy {

    public var indexKindIdentifier: String { "vector" }

    public func matchConditions<T: Persistable>(
        index: IndexDescriptor,
        conditions: [FieldCondition<T>]
    ) -> IndexMatchResult<T> {
        var satisfied: [FieldCondition<T>] = []

        for condition in conditions {
            guard condition.field.fieldName == index.keyPaths.first.map({ T.fieldName(for: $0) }) else {
                continue
            }

            if case .vectorSimilarity = condition.constraint {
                satisfied.append(condition)
            }
        }

        return IndexMatchResult(
            satisfiedConditions: satisfied,
            partialConditions: [],
            satisfiesOrdering: true, // Results ordered by similarity
            scanBounds: .unbounded,
            selectivity: 1.0 // Always returns top-K
        )
    }

    public func createOperator<T: Persistable>(
        index: IndexDescriptor,
        matchResult: IndexMatchResult<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanOperator<T> {
        guard let condition = matchResult.satisfiedConditions.first,
              case .vectorSimilarity(let constraint) = condition.constraint else {
            fatalError("VectorIndexStrategy requires vector similarity condition")
        }

        return .vectorSearch(VectorSearchOperator(
            index: index,
            queryVector: constraint.queryVector,
            k: constraint.k,
            distanceMetric: constraint.metric,
            efSearch: constraint.efSearch
        ))
    }
}
```

---

## Cost Estimation

### CostModel

```swift
/// Configuration for cost estimation
///
/// **Cost Weight Guidelines**:
/// - Weights represent relative costs, not absolute values
/// - Higher weights = more expensive operations to avoid
/// - Tune based on actual workload characteristics
public struct CostModel: Sendable {
    // === Basic I/O Costs ===

    /// Cost weight for reading an index entry
    public var indexReadWeight: Double = 1.0

    /// Cost weight for fetching a record by primary key
    public var recordFetchWeight: Double = 10.0

    /// Cost weight for post-filtering a record in memory
    public var postFilterWeight: Double = 0.1

    /// Cost weight for in-memory sorting (per record)
    public var sortWeight: Double = 0.01

    // === Range/Scan Costs ===

    /// Cost for initiating a new range scan (FDB range read setup)
    /// Applied once per IndexScan/TableScan operator
    public var rangeInitiationWeight: Double = 50.0

    // === Union/Intersection Costs ===

    /// Cost for deduplicating results in Union (per result item)
    /// Accounts for hash set operations and memory overhead
    public var deduplicationWeight: Double = 0.5

    /// Cost for intersection ID set operations (per ID)
    public var intersectionWeight: Double = 0.3

    /// Additional cost for fetching records after intersection
    /// (records fetched from first child then filtered by intersection)
    public var intersectionFetchWeight: Double = 2.0

    // === Default Selectivity Estimates ===

    /// Default selectivity for equality conditions (1% of rows)
    public var defaultEqualitySelectivity: Double = 0.01

    /// Default selectivity for range conditions (30% of rows)
    public var defaultRangeSelectivity: Double = 0.3

    /// Default selectivity for LIKE/CONTAINS patterns (10% of rows)
    public var defaultPatternSelectivity: Double = 0.1

    public static let `default` = CostModel()

    public static let favorIndexes = CostModel(
        recordFetchWeight: 20.0,
        postFilterWeight: 5.0
    )

    /// Cost model optimized for high-latency distributed environments
    public static let distributed = CostModel(
        rangeInitiationWeight: 100.0,
        deduplicationWeight: 1.0
    )
}
```

### CostEstimator

```swift
/// Estimates the cost of executing a query plan
public struct CostEstimator {

    private let statistics: StatisticsProvider
    private let costModel: CostModel

    public init(statistics: StatisticsProvider, costModel: CostModel) {
        self.statistics = statistics
        self.costModel = costModel
    }

    /// Estimate the cost of a plan
    public func estimate<T: Persistable>(
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

    private func estimateTableScan<T: Persistable>(
        _ op: TableScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))
        let selectivity = estimatePredicateSelectivity(analysis.originalPredicate)

        return PlanCost(
            indexReads: 0,
            recordFetches: totalRows,
            postFilterCount: totalRows * (1 - selectivity),
            requiresSort: !analysis.sortRequirements.isEmpty
        )
    }

    private func estimateIndexScan<T: Persistable>(
        _ op: IndexScanOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let totalRows = Double(statistics.estimatedRowCount(for: T.self))
        let indexEntries = Double(op.estimatedEntries)

        // Calculate how much post-filtering is needed
        let satisfiedSelectivity = op.satisfiedConditions.reduce(1.0) { acc, cond in
            acc * estimateConditionSelectivity(cond)
        }
        let totalSelectivity = estimatePredicateSelectivity(analysis.originalPredicate)
        let postFilterRatio = max(0, totalSelectivity / satisfiedSelectivity)

        return PlanCost(
            indexReads: indexEntries,
            recordFetches: indexEntries,
            postFilterCount: indexEntries * (1 - postFilterRatio),
            requiresSort: !checkOrderingSatisfied(op, analysis: analysis)
        )
    }

    private func estimateIndexSeek<T: Persistable>(
        _ op: IndexSeekOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let seekCount = Double(op.seekValues.count)

        return PlanCost(
            indexReads: seekCount,
            recordFetches: seekCount, // Assume 1 record per seek
            postFilterCount: 0,
            requiresSort: !analysis.sortRequirements.isEmpty && op.seekValues.count > 1
        )
    }

    private func estimateUnion<T: Persistable>(
        _ op: UnionOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let childCosts = op.children.map { estimate(plan: $0, analysis: analysis) }

        // Sum of all child index reads + range initiation per child
        let totalIndexReads = childCosts.reduce(0) { $0 + $1.indexReads }
        let rangeInitCosts = Double(op.children.count) * costModel.rangeInitiationWeight

        // Total records fetched across all children
        let totalRecordFetches = childCosts.reduce(0) { $0 + $1.recordFetches }

        // Deduplication cost (if enabled)
        let dedupCost = op.deduplicate
            ? totalRecordFetches * costModel.deduplicationWeight
            : 0

        return PlanCost(
            indexReads: totalIndexReads + rangeInitCosts,
            recordFetches: totalRecordFetches,
            postFilterCount: dedupCost,
            requiresSort: true // Union output is unordered; sort needed if ordering required
        )
    }

    private func estimateIntersection<T: Persistable>(
        _ op: IntersectionOperator<T>,
        analysis: QueryAnalysis<T>
    ) -> PlanCost {
        let childCosts = op.children.map { estimate(plan: $0, analysis: analysis) }

        // All children need to be scanned for IDs
        let totalIndexReads = childCosts.reduce(0) { $0 + $1.indexReads }
        let rangeInitCosts = Double(op.children.count) * costModel.rangeInitiationWeight

        // Estimate intersection result size (product of selectivities, clamped)
        let minChildFetches = childCosts.map { $0.recordFetches }.min() ?? 0
        let intersectionRatio = 0.1 // Heuristic: 10% survive intersection
        let estimatedResults = minChildFetches * intersectionRatio

        // Cost for ID set operations
        let idSetCost = totalIndexReads * costModel.intersectionWeight

        // Cost for fetching final records
        let fetchCost = estimatedResults * costModel.intersectionFetchWeight

        return PlanCost(
            indexReads: totalIndexReads + rangeInitCosts + idSetCost,
            recordFetches: estimatedResults + fetchCost,
            postFilterCount: 0,
            requiresSort: !analysis.sortRequirements.isEmpty
        )
    }

    /// Estimate selectivity of a condition
    private func estimateConditionSelectivity<T>(_ condition: FieldCondition<T>) -> Double {
        switch condition.constraint {
        case .equals:
            return statistics.equalitySelectivity(
                field: condition.field.fieldName,
                type: T.self
            ) ?? costModel.defaultEqualitySelectivity

        case .range(let range):
            return statistics.rangeSelectivity(
                field: condition.field.fieldName,
                range: range,
                type: T.self
            ) ?? costModel.defaultRangeSelectivity

        case .in(let values):
            let eqSelectivity = statistics.equalitySelectivity(
                field: condition.field.fieldName,
                type: T.self
            ) ?? costModel.defaultEqualitySelectivity
            return min(1.0, eqSelectivity * Double(values.count))

        case .stringPattern:
            return costModel.defaultPatternSelectivity

        case .isNull(let isNull):
            return statistics.nullSelectivity(
                field: condition.field.fieldName,
                type: T.self
            ) ?? 0.05

        default:
            return 0.5
        }
    }
}
```

### StatisticsProvider

```swift
/// Provides statistics about tables and indexes for cost estimation
public protocol StatisticsProvider: Sendable {
    /// Estimated total row count for a type
    func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int

    /// Estimated distinct values for a field
    func estimatedDistinctValues<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Int?

    /// Selectivity for equality condition
    func equalitySelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double?

    /// Selectivity for range condition
    func rangeSelectivity<T: Persistable>(
        field: String,
        range: Range,
        type: T.Type
    ) -> Double?

    /// Selectivity for null check
    func nullSelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double?

    /// Index entry count estimate
    func estimatedIndexEntries(
        index: IndexDescriptor
    ) -> Int?
}

/// Default statistics provider with heuristic estimates
///
/// **⚠️ PLACEHOLDER IMPLEMENTATION**
///
/// This provider uses simple heuristics and should be replaced with
/// `CollectedStatisticsProvider` in production for accurate cost estimation.
///
/// **Limitations**:
/// - Returns same row count for all types
/// - Uses fixed 10% distinct value ratio regardless of field type
/// - No per-field statistics (cardinality, null ratio, value distribution)
/// - No histogram data for range selectivity estimation
///
/// **Production Requirements** (for accurate query planning):
/// - Per-table row counts (via periodic COUNT or sampling)
/// - Per-field distinct value counts (via HyperLogLog or exact count)
/// - Histogram data for range selectivity (value distribution buckets)
/// - Null ratio per field (percentage of NULL values)
/// - Index-specific entry counts
///
/// **Usage**:
/// ```swift
/// // Development/testing - use defaults
/// let planner = QueryPlanner<User>(indexes: indexes)
///
/// // Production - use collected statistics
/// let stats = CollectedStatisticsProvider()
/// await stats.collect(for: User.self, using: context)
/// let planner = QueryPlanner<User>(indexes: indexes, statistics: stats)
/// ```
public struct DefaultStatisticsProvider: StatisticsProvider {

    private let defaultRowCount: Int

    public init(defaultRowCount: Int = 10000) {
        self.defaultRowCount = defaultRowCount
    }

    public func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        defaultRowCount
    }

    public func estimatedDistinctValues<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Int? {
        // HEURISTIC: Assume 10% distinct values by default
        // This is a rough estimate; production should use actual statistics
        return defaultRowCount / 10
    }

    public func equalitySelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        // HEURISTIC: 1 / estimated distinct values
        guard let distinct = estimatedDistinctValues(field: field, type: type) else {
            return nil
        }
        return 1.0 / Double(distinct)
    }

    public func rangeSelectivity<T: Persistable>(
        field: String,
        range: Range,
        type: T.Type
    ) -> Double? {
        // HEURISTIC: Default to 30% for ranges
        // Production should use histogram-based estimation
        return 0.3
    }

    public func nullSelectivity<T: Persistable>(
        field: String,
        type: T.Type
    ) -> Double? {
        // HEURISTIC: Default 5% null
        return 0.05
    }

    public func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        defaultRowCount
    }
}

/// Statistics provider that collects actual statistics from the database
public actor CollectedStatisticsProvider: StatisticsProvider {

    private var tableStats: [String: TableStatistics] = [:]
    private var fieldStats: [String: FieldStatistics] = [:]
    private var indexStats: [String: IndexStatistics] = [:]

    /// Collect statistics from the database
    public func collect<T: Persistable>(
        for type: T.Type,
        using context: FDBContext
    ) async throws {
        // Count total rows
        let count = try await context.fetch(type).count()

        // Sample for distinct value estimation
        let sample = try await context.fetch(type).limit(1000).execute()

        let typeName = String(describing: type)
        tableStats[typeName] = TableStatistics(
            rowCount: count,
            sampleSize: sample.count
        )

        // Collect field statistics from sample
        // ... (field-level collection)
    }

    nonisolated public func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        // Would need to read from collected stats
        10000
    }

    // ... other methods
}
```

---

## Plan Optimization

### PlanOptimizer

```swift
/// Selects the best plan from candidates
public struct PlanOptimizer<T: Persistable> {

    private let costModel: CostModel
    private let rules: [OptimizationRule<T>]

    public init(
        costModel: CostModel = .default,
        rules: [OptimizationRule<T>] = OptimizationRule.defaultRules()
    ) {
        self.costModel = costModel
        self.rules = rules
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
}

/// Rule for plan optimization
public protocol OptimizationRule<T: Persistable> {
    associatedtype T: Persistable

    /// Apply this rule to transform a plan
    func apply(to plan: PlanOperator<T>) -> PlanOperator<T>
}

// =============================================================================
// MARK: - Optimization Rules (FUTURE WORK)
// =============================================================================
//
// The following optimization rules are documented for future implementation.
// Currently, the query planner performs basic optimization during plan
// enumeration. These rules would provide additional post-enumeration
// optimization passes.
//
// Implementation Priority:
// 1. EliminateSortRule - High impact, moderate complexity
// 2. PushDownLimitRule - High impact for top-N queries
// 3. PushDownFilterRule - Moderate impact, higher complexity
//
// =============================================================================

/// Push filters down into index scans
///
/// **Status**: 🚧 FUTURE WORK
///
/// **Purpose**: When a Filter operator wraps an IndexScan, attempt to
/// incorporate additional filter conditions into the scan bounds, reducing
/// the number of records that need post-filtering.
///
/// **Algorithm**:
/// 1. Extract remaining conditions from Filter predicate
/// 2. Check if IndexScan can incorporate conditions into bounds
///    - Conditions on indexed fields after the current prefix
///    - Range conditions that can extend scan bounds
/// 3. Merge compatible conditions into scan bounds
/// 4. Return modified plan or original if no optimization possible
///
/// **Example**:
/// ```
/// Before: Filter(age > 18, IndexScan[email="alice@example.com"])
/// After:  IndexScan[email="alice@example.com", age > 18] (if composite index)
/// ```
public struct PushDownFilterRule<T: Persistable>: OptimizationRule {
    public func apply(to plan: PlanOperator<T>) -> PlanOperator<T> {
        // TODO: Implement filter pushdown
        // Tracked in: Phase 7 - Advanced Features
        return plan
    }
}

/// Remove unnecessary sort when index provides ordering
///
/// **Status**: 🚧 FUTURE WORK
///
/// **Purpose**: Eliminate Sort operators when the input IndexScan already
/// provides data in the required order.
///
/// **Algorithm**:
/// 1. Check if Sort wraps an IndexScan
/// 2. Compare sort descriptors with index key order
/// 3. Account for equality prefixes that make subsequent keys ordered
/// 4. Remove Sort if index order satisfies requirements
///
/// **Example**:
/// ```
/// Before: Sort[createdAt DESC](IndexScan[User_createdAt])
/// After:  IndexScan[User_createdAt, reverse=true]
/// ```
public struct EliminateSortRule<T: Persistable>: OptimizationRule {
    public func apply(to plan: PlanOperator<T>) -> PlanOperator<T> {
        // TODO: Implement sort elimination
        // Tracked in: Phase 7 - Advanced Features
        return plan
    }
}

/// Apply limit early when possible
///
/// **Status**: 🚧 FUTURE WORK
///
/// **Purpose**: Push Limit operator into IndexScan when ordering is
/// compatible, avoiding fetching more records than needed.
///
/// **Algorithm**:
/// 1. Check if Limit wraps Sort(IndexScan) or IndexScan directly
/// 2. Verify index provides required ordering
/// 3. Apply limit to scan, stopping early once limit reached
/// 4. Handle offset by scanning past initial records
///
/// **Example**:
/// ```
/// Before: Limit[10](IndexScan[User_createdAt])
/// After:  IndexScan[User_createdAt, limit=10]
/// ```
///
/// **Note**: Cannot apply to Union/Intersection without sorting first.
public struct PushDownLimitRule<T: Persistable>: OptimizationRule {
    public func apply(to plan: PlanOperator<T>) -> PlanOperator<T> {
        // TODO: Implement limit pushdown
        // Tracked in: Phase 7 - Advanced Features
        return plan
    }
}

extension OptimizationRule {
    /// Default optimization rules
    ///
    /// **Note**: Currently returns placeholder implementations.
    /// Full optimization will be implemented in Phase 7.
    public static func defaultRules<T: Persistable>() -> [any OptimizationRule<T>] {
        [
            PushDownFilterRule<T>(),
            EliminateSortRule<T>(),
            PushDownLimitRule<T>()
        ]
    }
}
```

---

## Plan Execution

### PlanExecutor

```swift
/// Executes a query plan
public final class PlanExecutor<T: Persistable>: Sendable {

    private let context: FDBContext
    private let dataStore: DataStore

    public init(context: FDBContext, dataStore: DataStore) {
        self.context = context
        self.dataStore = dataStore
    }

    /// Execute a plan and return results
    public func execute(plan: QueryPlan<T>) async throws -> [T] {
        try await executeOperator(plan.rootOperator)
    }

    /// Execute a plan and stream results
    public func stream(plan: QueryPlan<T>) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    try await streamOperator(plan.rootOperator, to: continuation)
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    private func executeOperator(_ op: PlanOperator<T>) async throws -> [T] {
        switch op {
        case .tableScan(let scanOp):
            return try await executeTableScan(scanOp)

        case .indexScan(let scanOp):
            return try await executeIndexScan(scanOp)

        case .indexSeek(let seekOp):
            return try await executeIndexSeek(seekOp)

        case .union(let unionOp):
            return try await executeUnion(unionOp)

        case .intersection(let intersectionOp):
            return try await executeIntersection(intersectionOp)

        case .filter(let filterOp):
            let input = try await executeOperator(filterOp.input)
            return input.filter { evaluatePredicate(filterOp.predicate, on: $0) }

        case .sort(let sortOp):
            let input = try await executeOperator(sortOp.input)
            return input.sorted { compareModels($0, $1, by: sortOp.sortDescriptors) }

        case .limit(let limitOp):
            var input = try await executeOperator(limitOp.input)
            if let offset = limitOp.offset {
                input = Array(input.dropFirst(offset))
            }
            if let limit = limitOp.limit {
                input = Array(input.prefix(limit))
            }
            return input

        case .fullTextScan(let ftOp):
            return try await executeFullTextScan(ftOp)

        case .vectorSearch(let vectorOp):
            return try await executeVectorSearch(vectorOp)

        case .spatialScan(let spatialOp):
            return try await executeSpatialScan(spatialOp)

        case .aggregation(let aggOp):
            return try await executeAggregation(aggOp)

        case .project(let projectOp):
            return try await executeOperator(projectOp.input)
        }
    }

    private func executeIndexScan(_ op: IndexScanOperator<T>) async throws -> [T] {
        // Build scan range from bounds
        let range = buildScanRange(index: op.index, bounds: op.bounds)

        // Scan index entries
        let entries = try await dataStore.scanIndex(
            name: op.index.name,
            range: range,
            reverse: op.reverse
        )

        // Fetch records by ID
        var results: [T] = []
        for entry in entries {
            if let model: T = try await dataStore.fetch(id: entry.primaryKey) {
                results.append(model)
            }
        }

        return results
    }

    private func executeUnion(_ op: UnionOperator<T>) async throws -> [T] {
        // Execute children in parallel
        // NOTE: Results are returned in non-deterministic order due to parallel execution
        let childResults = try await withThrowingTaskGroup(of: [T].self) { group in
            for child in op.children {
                group.addTask {
                    try await self.executeOperator(child)
                }
            }

            var allResults: [[T]] = []
            for try await result in group {
                allResults.append(result)
            }
            return allResults
        }

        // Flatten and deduplicate using Persistable.ID
        // This works because Persistable.ID: Hashable (defined in protocol)
        var seen = Set<AnyHashable>()
        var results: [T] = []

        for childResult in childResults {
            for item in childResult {
                // Safe: Persistable.ID conforms to Hashable
                let key = AnyHashable(item.id)
                if !seen.contains(key) {
                    seen.insert(key)
                    results.append(item)
                }
            }
        }

        return results
    }

    private func executeIntersection(_ op: IntersectionOperator<T>) async throws -> [T] {
        guard !op.children.isEmpty else { return [] }

        // Execute all children
        let childResults = try await withThrowingTaskGroup(of: Set<AnyHashable>.self) { group in
            for child in op.children {
                group.addTask {
                    let results = try await self.executeOperator(child)
                    return Set(results.map { AnyHashable($0.id) })
                }
            }

            var sets: [Set<AnyHashable>] = []
            for try await resultSet in group {
                sets.append(resultSet)
            }
            return sets
        }

        // Intersect all ID sets
        guard var resultIds = childResults.first else { return [] }
        for otherSet in childResults.dropFirst() {
            resultIds = resultIds.intersection(otherSet)
        }

        // Fetch the intersecting records
        var results: [T] = []
        for id in resultIds {
            if let model: T = try await dataStore.fetch(id: id) {
                results.append(model)
            }
        }

        return results
    }
}
```

---

## Integration with Existing API

### Updated QueryExecutor

```swift
/// Extended QueryExecutor with query planning support
public struct QueryExecutor<T: Persistable>: Sendable {

    private let context: FDBContext
    private let dataStore: DataStore
    private var query: Query<T>

    // Existing fluent methods...

    /// Execute with automatic query planning
    public func execute() async throws -> [T] {
        // Build query planner
        let indexes = try await context.getIndexes(for: T.self)
        let planner = QueryPlanner<T>(
            indexes: indexes,
            statistics: context.statisticsProvider
        )

        // Plan the query
        let plan = try planner.plan(query: query)

        // Log plan if debugging enabled
        if context.configuration.logQueryPlans {
            context.logger.debug("Query plan: \(plan.explanation)")
        }

        // Execute the plan
        let executor = PlanExecutor<T>(context: context, dataStore: dataStore)
        return try await executor.execute(plan: plan)
    }

    /// Get the query plan without executing
    public func explain() async throws -> PlanExplanation {
        let indexes = try await context.getIndexes(for: T.self)
        let planner = QueryPlanner<T>(
            indexes: indexes,
            statistics: context.statisticsProvider
        )
        return try planner.explain(query: query)
    }

    /// Force a specific index (bypass planner)
    public func usingIndex(_ indexName: String) -> QueryExecutor<T> {
        var copy = self
        copy.query.hints.preferredIndex = indexName
        return copy
    }

    /// Force table scan (bypass planner)
    public func forcingScan() -> QueryExecutor<T> {
        var copy = self
        copy.query.hints.forceTableScan = true
        return copy
    }
}
```

### Query Hints

```swift
/// Hints to influence query planning
public struct QueryHints: Sendable {
    /// Prefer using this index
    public var preferredIndex: String?

    /// Force table scan instead of index
    public var forceTableScan: Bool = false

    /// Maximum cost before falling back to scan
    public var maxIndexCost: Double?

    /// Enable/disable specific optimizations
    public var disabledOptimizations: Set<String> = []
}
```

---

## Plan Explanation

```swift
/// Type-erased plan information for display purposes
///
/// This struct captures the essential information from a `QueryPlan<T>` in a
/// type-erased form suitable for logging, debugging, and display.
private struct ErasedPlanInfo: Sendable {
    let estimatedCost: PlanCost
    let usedIndexNames: [(name: String, kind: String)]
    let operatorTree: String
    let usedFields: Set<String>
    let orderingSatisfied: Bool
}

/// Human-readable explanation of a query plan
///
/// Uses type erasure internally to avoid exposing `QueryPlan<T>` generic parameter.
/// Create via `QueryPlanner.explain(query:)` or `QueryExecutor.explain()`.
public struct PlanExplanation: CustomStringConvertible, Sendable {

    private let info: ErasedPlanInfo

    /// Create explanation from a typed query plan
    public init<T: Persistable>(plan: QueryPlan<T>) {
        self.info = ErasedPlanInfo(
            estimatedCost: plan.estimatedCost,
            usedIndexNames: plan.usedIndexes.map { ($0.name, $0.kindIdentifier) },
            operatorTree: Self.buildOperatorTree(plan.rootOperator, indent: 2),
            usedFields: plan.usedFields,
            orderingSatisfied: plan.orderingSatisfied
        )
    }

    public var description: String {
        var lines: [String] = []
        lines.append("Query Plan:")
        lines.append("  Estimated Cost: \(String(format: "%.2f", info.estimatedCost.totalCost))")
        lines.append("  Index Reads: \(String(format: "%.1f", info.estimatedCost.indexReads))")
        lines.append("  Record Fetches: \(String(format: "%.1f", info.estimatedCost.recordFetches))")
        lines.append("  Requires Sort: \(info.estimatedCost.requiresSort)")
        lines.append("  Ordering Satisfied: \(info.orderingSatisfied)")
        lines.append("")
        lines.append("Execution Tree:")
        lines.append(info.operatorTree)

        if !info.usedIndexNames.isEmpty {
            lines.append("")
            lines.append("Used Indexes:")
            for (name, kind) in info.usedIndexNames {
                lines.append("  - \(name) (\(kind))")
            }
        }

        return lines.joined(separator: "\n")
    }

    /// Build operator tree string recursively
    private static func buildOperatorTree<T: Persistable>(
        _ op: PlanOperator<T>,
        indent: Int
    ) -> String {
        var lines: [String] = []
        let prefix = String(repeating: " ", count: indent)

        switch op {
        case .tableScan(let scanOp):
            lines.append("\(prefix)-> TableScan (est. \(scanOp.estimatedRows) rows)")

        case .indexScan(let scanOp):
            lines.append("\(prefix)-> IndexScan[\(scanOp.index.name)]")
            lines.append("\(prefix)   bounds: \(describeBounds(scanOp.bounds))")
            lines.append("\(prefix)   est. entries: \(scanOp.estimatedEntries)")

        case .indexSeek(let seekOp):
            lines.append("\(prefix)-> IndexSeek[\(seekOp.index.name)]")
            lines.append("\(prefix)   values: \(seekOp.seekValues.count) lookups")

        case .union(let unionOp):
            lines.append("\(prefix)-> Union (dedupe: \(unionOp.deduplicate), unordered)")
            for child in unionOp.children {
                lines.append(buildOperatorTree(child, indent: indent + 3))
            }

        case .intersection(let intersectionOp):
            lines.append("\(prefix)-> Intersection")
            for child in intersectionOp.children {
                lines.append(buildOperatorTree(child, indent: indent + 3))
            }

        case .filter(let filterOp):
            lines.append("\(prefix)-> Filter (selectivity: \(String(format: "%.2f", filterOp.selectivity)))")
            lines.append(buildOperatorTree(filterOp.input, indent: indent + 3))

        case .sort(let sortOp):
            let fields = sortOp.sortDescriptors.map { "\($0.fieldName) \($0.order)" }
            lines.append("\(prefix)-> Sort[\(fields.joined(separator: ", "))]")
            lines.append(buildOperatorTree(sortOp.input, indent: indent + 3))

        case .limit(let limitOp):
            lines.append("\(prefix)-> Limit[\(limitOp.limit ?? -1), offset: \(limitOp.offset ?? 0)]")
            lines.append(buildOperatorTree(limitOp.input, indent: indent + 3))

        case .fullTextScan(let ftOp):
            lines.append("\(prefix)-> FullTextScan[\(ftOp.index.name)]")
            lines.append("\(prefix)   terms: \(ftOp.searchTerms.joined(separator: ", "))")

        case .vectorSearch(let vectorOp):
            lines.append("\(prefix)-> VectorSearch[\(vectorOp.index.name)]")
            lines.append("\(prefix)   k: \(vectorOp.k), metric: \(vectorOp.distanceMetric)")

        case .spatialScan(let spatialOp):
            lines.append("\(prefix)-> SpatialScan[\(spatialOp.index.name)]")

        case .aggregation(let aggOp):
            lines.append("\(prefix)-> Aggregation[\(aggOp.index.name)]")

        case .project(let projectOp):
            lines.append("\(prefix)-> Project")
            lines.append(buildOperatorTree(projectOp.input, indent: indent + 3))
        }

        return lines.joined(separator: "\n")
    }

    private static func describeBounds(_ bounds: IndexScanBounds) -> String {
        if bounds.start.isEmpty && bounds.end.isEmpty {
            return "[unbounded]"
        }
        let startDesc = bounds.start.isEmpty ? "-∞" : bounds.start.map { "\($0.value ?? "nil")" }.joined(separator: ", ")
        let endDesc = bounds.end.isEmpty ? "+∞" : bounds.end.map { "\($0.value ?? "nil")" }.joined(separator: ", ")
        return "[\(startDesc) .. \(endDesc)]"
    }
}
```

---

## Usage Examples

### Basic Usage

```swift
// Automatic query planning
let users = try await context.fetch(User.self)
    .where(\.isActive == true)
    .where(\.age > 18)
    .orderBy(\.name)
    .limit(10)
    .execute()

// Explain the plan
let explanation = try await context.fetch(User.self)
    .where(\.email == "alice@example.com")
    .explain()
print(explanation)
// Output:
// Query Plan:
//   Estimated Cost: 11.0
//   Index Reads: 1.0
//   Record Fetches: 1.0
//   Requires Sort: false
//
// Execution Tree:
//   -> IndexSeek[User_email]
//      values: 1 lookups
//
// Used Indexes:
//   - User_email (scalar)
```

### Complex Queries

```swift
// OR condition with union
let items = try await context.fetch(Product.self)
    .where(\.category == "electronics" || \.category == "books")
    .execute()
// Plan: Union[IndexScan[category=electronics], IndexScan[category=books]]

// AND with multiple indexes (intersection)
let items = try await context.fetch(Order.self)
    .where(\.customerId == customerId)
    .where(\.status == .pending)
    .execute()
// Plan: Intersection[IndexSeek[customerId], IndexSeek[status]]

// Full-text search
let docs = try await context.fetch(Document.self)
    .where(\.content.textSearch("swift concurrency"))
    .execute()
// Plan: FullTextScan[Document_content_ft]

// Vector similarity
let similar = try await context.fetch(Article.self)
    .where(\.embedding.similarTo(queryVector, k: 10))
    .execute()
// Plan: VectorSearch[Article_embedding_hnsw]
```

### Query Hints

```swift
// Force specific index
let users = try await context.fetch(User.self)
    .where(\.department == "Engineering")
    .where(\.createdAt > cutoffDate)
    .usingIndex("User_department")
    .execute()

// Force table scan (for testing/comparison)
let all = try await context.fetch(User.self)
    .where(\.isActive == true)
    .forcingScan()
    .execute()
```

---

## Implementation Phases

### Phase 1: Core Infrastructure
1. Define `QueryCondition`, `FieldCondition`, `FieldConstraint` types
2. Implement `QueryAnalyzer` and `PredicateNormalizer`
3. Define `PlanOperator` hierarchy
4. Create basic `QueryPlan` structure

### Phase 2: Scalar Index Support
1. Implement `ScalarIndexStrategy`
2. Handle equality, range, IN conditions
3. Support composite indexes
4. Handle ordering optimization

### Phase 3: Cost Estimation
1. Implement `CostModel` and `CostEstimator`
2. Create `DefaultStatisticsProvider`
3. Add selectivity estimation

### Phase 4: Plan Enumeration
1. Implement `PlanEnumerator`
2. Support single-index plans
3. Add union/intersection plans
4. Implement optimization rules

### Phase 5: Plan Execution
1. Implement `PlanExecutor`
2. Integrate with existing `DataStore`
3. Add streaming execution

### Phase 6: Specialized Index Strategies
1. `FullTextIndexStrategy`
2. `VectorIndexStrategy`
3. `SpatialIndexStrategy`
4. `AggregationIndexStrategy`

### Phase 7: Advanced Features
1. `CollectedStatisticsProvider`
2. Query hints
3. Plan caching
4. Execution statistics

---

## Testing Strategy

```swift
@Suite struct QueryPlannerTests {

    @Test func testPointLookupUsesIndexSeek() async throws {
        let planner = QueryPlanner<User>(indexes: [emailIndex])
        let query = Query<User>().where(\.email == "test@example.com")

        let plan = try planner.plan(query: query)

        guard case .indexSeek(let seekOp) = plan.rootOperator else {
            Issue.record("Expected IndexSeek")
            return
        }
        #expect(seekOp.index.name == "User_email")
    }

    @Test func testRangeQueryUsesIndexScan() async throws {
        let planner = QueryPlanner<User>(indexes: [ageIndex])
        let query = Query<User>().where(\.age > 18)

        let plan = try planner.plan(query: query)

        guard case .indexScan(let scanOp) = plan.rootOperator else {
            Issue.record("Expected IndexScan")
            return
        }
        #expect(scanOp.bounds.start.first?.value != nil)
    }

    @Test func testOrConditionUsesUnion() async throws {
        let planner = QueryPlanner<Product>(indexes: [categoryIndex])
        let query = Query<Product>()
            .where(\.category == "A" || \.category == "B")

        let plan = try planner.plan(query: query)

        guard case .union = plan.rootOperator else {
            Issue.record("Expected Union")
            return
        }
    }

    @Test func testCostComparison() async throws {
        let planner = QueryPlanner<User>(indexes: [emailIndex, nameIndex])

        // Email is unique, name is not
        let emailQuery = Query<User>().where(\.email == "test@example.com")
        let nameQuery = Query<User>().where(\.name == "Alice")

        let emailPlan = try planner.plan(query: emailQuery)
        let namePlan = try planner.plan(query: nameQuery)

        #expect(emailPlan.estimatedCost < namePlan.estimatedCost)
    }
}
```

---

## Summary

This Query Planner design provides:

1. **Automatic Index Selection** - Analyzes predicates and chooses optimal indexes
2. **Cost-Based Optimization** - Estimates costs and selects cheapest plan
3. **Composite Index Support** - Leverages multi-field indexes effectively
4. **Specialized Strategies** - Handles all 11 index types appropriately
5. **Union/Intersection** - Supports complex OR/AND with multiple indexes
6. **Plan Explanation** - Debugging and optimization insights
7. **Backward Compatibility** - Integrates with existing fluent API
8. **Extensibility** - Easy to add new index strategies and optimization rules
