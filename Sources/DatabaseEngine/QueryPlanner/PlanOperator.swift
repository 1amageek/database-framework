// PlanOperator.swift
// QueryPlanner - Execution plan operators

import Core

/// Operators that make up a query plan
public indirect enum PlanOperator<T: Persistable>: @unchecked Sendable {

    // === Scan Operators ===

    /// Full table scan - reads all records
    case tableScan(TableScanOperator<T>)

    /// Index range scan - reads a range of index entries
    case indexScan(IndexScanOperator<T>)

    /// Index seek - point lookup(s) in index
    case indexSeek(IndexSeekOperator<T>)

    /// Index-only scan - reads from covering index without record fetch
    case indexOnlyScan(IndexOnlyScanOperator<T>)

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

// MARK: - Table Scan Operator

/// Table scan operator - reads all records
public struct TableScanOperator<T: Persistable>: @unchecked Sendable {
    /// Estimated row count
    public let estimatedRows: Int

    /// Optional predicate to apply during scan
    public let filterPredicate: Predicate<T>?

    public init(estimatedRows: Int, filterPredicate: Predicate<T>? = nil) {
        self.estimatedRows = estimatedRows
        self.filterPredicate = filterPredicate
    }
}

// MARK: - Index Scan Operator

/// Index scan operator - reads a range of index entries
public struct IndexScanOperator<T: Persistable>: @unchecked Sendable {
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

    public init(
        index: IndexDescriptor,
        bounds: IndexScanBounds,
        reverse: Bool = false,
        satisfiedConditions: [FieldCondition<T>] = [],
        estimatedEntries: Int
    ) {
        self.index = index
        self.bounds = bounds
        self.reverse = reverse
        self.satisfiedConditions = satisfiedConditions
        self.estimatedEntries = estimatedEntries
    }
}

/// Bounds for index scan
public struct IndexScanBounds: Sendable {
    /// Starting key components (inclusive/exclusive)
    public let start: [BoundComponent]

    /// Ending key components (inclusive/exclusive)
    public let end: [BoundComponent]

    public init(start: [BoundComponent] = [], end: [BoundComponent] = []) {
        self.start = start
        self.end = end
    }

    /// A single bound component
    public struct BoundComponent: @unchecked Sendable {
        public let value: AnySendable?
        public let inclusive: Bool

        public init(value: AnySendable?, inclusive: Bool) {
            self.value = value
            self.inclusive = inclusive
        }
    }

    /// Full index scan (no bounds)
    public static let unbounded = IndexScanBounds(start: [], end: [])

    /// Check if bounds are unbounded
    public var isUnbounded: Bool {
        start.isEmpty && end.isEmpty
    }
}

// MARK: - Index Seek Operator

/// Index seek operator - point lookups in index
public struct IndexSeekOperator<T: Persistable>: @unchecked Sendable {
    /// The index to seek in
    public let index: IndexDescriptor

    /// Values to seek (each inner array is one key)
    public let seekValues: [[AnySendable]]

    /// Conditions satisfied by this seek
    public let satisfiedConditions: [FieldCondition<T>]

    public init(
        index: IndexDescriptor,
        seekValues: [[AnySendable]],
        satisfiedConditions: [FieldCondition<T>] = []
    ) {
        self.index = index
        self.seekValues = seekValues
        self.satisfiedConditions = satisfiedConditions
    }
}

// MARK: - Union Operator

/// Union operator (OR)
///
/// **IMPORTANT**: Union output is UNORDERED. Results from parallel child
/// execution are merged without preserving any specific order. If ordering
/// is required, the PlanEnumerator will wrap this operator with a SortOperator.
///
/// Deduplication uses `Persistable.ID` (which is `Hashable`) to identify
/// duplicate records across children.
public struct UnionOperator<T: Persistable>: @unchecked Sendable {
    /// Child plans to union
    public let children: [PlanOperator<T>]

    /// Whether to deduplicate results
    /// When true, uses Set<AnyHashable> with item.id for O(1) dedup
    public let deduplicate: Bool

    public init(children: [PlanOperator<T>], deduplicate: Bool = true) {
        self.children = children
        self.deduplicate = deduplicate
    }
}

// MARK: - Intersection Operator

/// Intersection operator (AND with multiple indexes)
public struct IntersectionOperator<T: Persistable>: @unchecked Sendable {
    /// Child plans to intersect
    public let children: [PlanOperator<T>]

    public init(children: [PlanOperator<T>]) {
        self.children = children
    }
}

// MARK: - Filter Operator

/// Filter operator - applies predicate to input
public struct FilterOperator<T: Persistable>: @unchecked Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Predicate to apply
    public let predicate: Predicate<T>

    /// Estimated selectivity (0.0 - 1.0)
    public let selectivity: Double

    public init(input: PlanOperator<T>, predicate: Predicate<T>, selectivity: Double) {
        self.input = input
        self.predicate = predicate
        self.selectivity = selectivity
    }
}

// MARK: - Sort Operator

/// Sort operator - sorts input by descriptors
public struct SortOperator<T: Persistable>: @unchecked Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Sort descriptors
    public let sortDescriptors: [SortDescriptor<T>]

    /// Estimated input size
    public let estimatedInputSize: Int

    public init(
        input: PlanOperator<T>,
        sortDescriptors: [SortDescriptor<T>],
        estimatedInputSize: Int
    ) {
        self.input = input
        self.sortDescriptors = sortDescriptors
        self.estimatedInputSize = estimatedInputSize
    }
}

// MARK: - Limit Operator

/// Limit operator - limits and offsets results
public struct LimitOperator<T: Persistable>: @unchecked Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Maximum rows to return
    public let limit: Int?

    /// Rows to skip
    public let offset: Int?

    public init(input: PlanOperator<T>, limit: Int?, offset: Int?) {
        self.input = input
        self.limit = limit
        self.offset = offset
    }
}

// MARK: - Project Operator

/// Project operator - selects specific fields
public struct ProjectOperator<T: Persistable>: @unchecked Sendable {
    /// Input operator
    public let input: PlanOperator<T>

    /// Fields to project
    public let fields: Set<String>

    public init(input: PlanOperator<T>, fields: Set<String>) {
        self.input = input
        self.fields = fields
    }
}

// MARK: - Full Text Scan Operator

/// Full-text search scan operator
public struct FullTextScanOperator<T: Persistable>: @unchecked Sendable {
    /// The full-text index to use
    public let index: IndexDescriptor

    /// Search terms
    public let searchTerms: [String]

    /// Match mode
    public let matchMode: TextMatchMode

    /// Estimated results
    public let estimatedResults: Int

    public init(
        index: IndexDescriptor,
        searchTerms: [String],
        matchMode: TextMatchMode = .any,
        estimatedResults: Int
    ) {
        self.index = index
        self.searchTerms = searchTerms
        self.matchMode = matchMode
        self.estimatedResults = estimatedResults
    }
}

// MARK: - Vector Search Operator

/// Vector similarity search operator
public struct VectorSearchOperator<T: Persistable>: @unchecked Sendable {
    /// The vector index to use
    public let index: IndexDescriptor

    /// Query vector
    public let queryVector: [Float]

    /// Number of neighbors
    public let k: Int

    /// Distance metric
    public let distanceMetric: VectorDistanceMetric

    /// HNSW ef_search parameter
    public let efSearch: Int?

    public init(
        index: IndexDescriptor,
        queryVector: [Float],
        k: Int,
        distanceMetric: VectorDistanceMetric = .cosine,
        efSearch: Int? = nil
    ) {
        self.index = index
        self.queryVector = queryVector
        self.k = k
        self.distanceMetric = distanceMetric
        self.efSearch = efSearch
    }
}

// MARK: - Spatial Scan Operator

/// Spatial region scan operator
public struct SpatialScanOperator<T: Persistable>: @unchecked Sendable {
    /// The spatial index to use
    public let index: IndexDescriptor

    /// Spatial constraint
    public let constraint: SpatialConstraint

    /// Estimated results
    public let estimatedResults: Int

    public init(index: IndexDescriptor, constraint: SpatialConstraint, estimatedResults: Int) {
        self.index = index
        self.constraint = constraint
        self.estimatedResults = estimatedResults
    }
}

// MARK: - Aggregation Operator

/// Aggregation operator
public struct AggregationOperator<T: Persistable>: @unchecked Sendable {
    /// The aggregation index to use
    public let index: IndexDescriptor

    /// Type of aggregation
    public let aggregationType: AggregationType

    /// Group by fields (if any)
    public let groupByFields: [String]

    public init(
        index: IndexDescriptor,
        aggregationType: AggregationType,
        groupByFields: [String] = []
    ) {
        self.index = index
        self.aggregationType = aggregationType
        self.groupByFields = groupByFields
    }
}

/// Types of aggregations
public enum AggregationType: Sendable, Hashable {
    case count
    case sum(field: String)
    case min(field: String)
    case max(field: String)
    case avg(field: String)
}
