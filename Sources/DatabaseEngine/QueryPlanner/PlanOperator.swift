// PlanOperator.swift
// QueryPlanner - Execution plan operators

import Foundation
import Core
import FoundationDB

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

    // === IN Optimization Operators ===

    /// IN-Union: Execute multiple index seeks in parallel and union results
    /// Best for small IN lists (< 20 values) with index support
    /// Uses existential type to support any value type V
    case inUnion(any InOperatorExecutable<T>)

    /// IN-Join: Nested loop join with IN values as the driving table
    /// Best for larger IN lists (20-1000 values)
    /// Uses existential type to support any value type V
    case inJoin(any InOperatorExecutable<T>)
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

    /// Maximum number of entries to return (pushed down from LIMIT)
    /// When set, the scan will stop early after fetching this many entries.
    public let limit: Int?

    public init(
        index: IndexDescriptor,
        bounds: IndexScanBounds,
        reverse: Bool = false,
        satisfiedConditions: [FieldCondition<T>] = [],
        estimatedEntries: Int,
        limit: Int? = nil
    ) {
        self.index = index
        self.bounds = bounds
        self.reverse = reverse
        self.satisfiedConditions = satisfiedConditions
        self.estimatedEntries = estimatedEntries
        self.limit = limit
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

// MARK: - IN Operator Protocol

/// Protocol for IN operators that can be executed
///
/// This protocol enables type-safe IN operations while allowing
/// PlanOperator to hold them via existential type (`any InOperatorExecutable<T>`).
public protocol InOperatorExecutable<T>: Sendable {
    associatedtype T: Persistable

    /// The index to use
    var index: IndexDescriptor { get }

    /// Field path being queried
    var fieldPath: String { get }

    /// Number of values in the IN list
    var valueCount: Int { get }

    /// Estimated total results
    var estimatedTotalResults: Int { get }

    /// Additional filter predicate
    var additionalFilter: Predicate<T>? { get }

    /// Convert a value from index entry to check for membership
    func containsValue(_ value: Any) -> Bool

    /// Get values as TupleElements for index seeks
    func valuesAsTupleElements() -> [any TupleElement]

    /// Get min/max values for range scans (nil if not comparable or empty)
    func valueRange() -> (min: any TupleElement, max: any TupleElement)?
}

// MARK: - IN-Union Operator

/// IN-Union operator for small IN lists
///
/// Executes multiple index seeks (one per value) in parallel and unions the results.
/// This is efficient for small IN lists (< 20 values) where the overhead of parallel
/// seeks is offset by the reduced latency.
///
/// **Reference**: FDB Record Layer InExtractor union strategy
///
/// **Example SQL**:
/// ```sql
/// SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')
/// ```
///
/// **Execution**:
/// 1. Create one index seek per IN value
/// 2. Execute all seeks in parallel
/// 3. Union and deduplicate results
///
/// **Cost Model**:
/// - Seeks: O(n) where n = number of IN values
/// - Network round-trips: 1 (parallel execution)
/// - Memory: O(result_size)
public struct InUnionOperator<T: Persistable, V: Comparable & Hashable & Sendable & TupleElementConvertible>: InOperatorExecutable, Sendable {
    /// The index to use for seeks
    public let index: IndexDescriptor

    /// Field path being queried (e.g., "status")
    public let fieldPath: String

    /// Values in the IN list (type-safe)
    public let values: [V]

    /// Hash set for O(1) lookup
    public let valueSet: Set<V>

    /// Additional filter to apply after union
    public let additionalFilter: Predicate<T>?

    /// Estimated results per value
    public let estimatedResultsPerValue: Int

    /// Whether to deduplicate results (default: true)
    public let deduplicate: Bool

    public init(
        index: IndexDescriptor,
        fieldPath: String,
        values: [V],
        additionalFilter: Predicate<T>? = nil,
        estimatedResultsPerValue: Int = 10,
        deduplicate: Bool = true
    ) {
        self.index = index
        self.fieldPath = fieldPath
        self.values = values
        self.valueSet = Set(values)
        self.additionalFilter = additionalFilter
        self.estimatedResultsPerValue = estimatedResultsPerValue
        self.deduplicate = deduplicate
    }

    public var valueCount: Int { values.count }

    public var estimatedTotalResults: Int {
        values.count * estimatedResultsPerValue
    }

    public func containsValue(_ value: Any) -> Bool {
        guard let typedValue = value as? V else { return false }
        return valueSet.contains(typedValue)
    }

    public func valuesAsTupleElements() -> [any TupleElement] {
        values.map { $0.toTupleElement() }
    }

    public func valueRange() -> (min: any TupleElement, max: any TupleElement)? {
        guard let minVal = values.min(), let maxVal = values.max() else { return nil }
        return (minVal.toTupleElement(), maxVal.toTupleElement())
    }
}

// MARK: - IN-Join Operator

/// IN-Join operator for larger IN lists
///
/// Uses a nested loop join pattern where IN values form a "virtual table"
/// that drives the join. More efficient than union for larger IN lists
/// because it avoids the overhead of creating many parallel operations.
///
/// **Reference**: FDB Record Layer InExtractor join strategy
///
/// **Example SQL**:
/// ```sql
/// SELECT * FROM orders WHERE customer_id IN (/* 100+ values */)
/// ```
///
/// **Execution**:
/// 1. Create a hash set from IN values
/// 2. Scan the index in batches
/// 3. Filter entries where the key matches any IN value
///
/// **Cost Model**:
/// - Time: O(index_size * log(n)) where n = number of IN values
/// - Memory: O(n) for the hash set + O(batch_size) for results
///
/// **When to Use**:
/// - IN list has 20-1000 values
/// - Index exists on the IN field
/// - Alternative (table scan + filter) would be more expensive
public struct InJoinOperator<T: Persistable, V: Comparable & Hashable & Sendable & TupleElementConvertible>: InOperatorExecutable, Sendable {
    /// The index to use
    public let index: IndexDescriptor

    /// Field path being queried
    public let fieldPath: String

    /// Values in the IN list (type-safe)
    public let values: [V]

    /// Hash set for O(1) lookup
    public let valueSet: Set<V>

    /// Bloom filter for fast rejection
    public let bloomFilter: InJoinBloomFilter<V>?

    /// Batch size for scanning the index
    public let batchSize: Int

    /// Additional filter to apply after join
    public let additionalFilter: Predicate<T>?

    /// Estimated selectivity (fraction of index entries matching)
    public let estimatedSelectivity: Double

    public init(
        index: IndexDescriptor,
        fieldPath: String,
        values: [V],
        batchSize: Int = 100,
        additionalFilter: Predicate<T>? = nil,
        estimatedSelectivity: Double = 0.1,
        useBloomFilter: Bool = true
    ) {
        self.index = index
        self.fieldPath = fieldPath
        self.values = values
        self.valueSet = Set(values)
        self.batchSize = batchSize
        self.additionalFilter = additionalFilter
        self.estimatedSelectivity = estimatedSelectivity

        // Build Bloom filter for large value sets
        if useBloomFilter && values.count > 50 {
            var filter = InJoinBloomFilter<V>(expectedElements: values.count)
            for v in values {
                filter.insert(v)
            }
            self.bloomFilter = filter
        } else {
            self.bloomFilter = nil
        }
    }

    public var valueCount: Int { values.count }

    public var estimatedTotalResults: Int {
        Int(Double(valueCount) * estimatedSelectivity * 100)
    }

    /// Estimated results based on selectivity and index size
    public func estimatedResults(indexSize: Int) -> Int {
        Int(Double(indexSize) * estimatedSelectivity)
    }

    public func containsValue(_ value: Any) -> Bool {
        guard let typedValue = value as? V else { return false }

        // Fast rejection with Bloom filter
        if let bloom = bloomFilter, !bloom.mightContain(typedValue) {
            return false
        }

        return valueSet.contains(typedValue)
    }

    public func valuesAsTupleElements() -> [any TupleElement] {
        values.map { $0.toTupleElement() }
    }

    public func valueRange() -> (min: any TupleElement, max: any TupleElement)? {
        guard let minVal = values.min(), let maxVal = values.max() else { return nil }
        return (minVal.toTupleElement(), maxVal.toTupleElement())
    }
}

// MARK: - TupleElementConvertible

/// Protocol for types that can be converted to TupleElement
public protocol TupleElementConvertible {
    func toTupleElement() -> any TupleElement
}

extension Int: TupleElementConvertible {
    public func toTupleElement() -> any TupleElement { Int64(self) }
}

extension Int64: TupleElementConvertible {
    public func toTupleElement() -> any TupleElement { self }
}

extension String: TupleElementConvertible {
    public func toTupleElement() -> any TupleElement { self }
}

extension Double: TupleElementConvertible {
    public func toTupleElement() -> any TupleElement { self }
}

extension Bool: TupleElementConvertible {
    public func toTupleElement() -> any TupleElement { self }
}

// MARK: - InJoinBloomFilter

/// Bloom filter specialized for IN-Join operations
///
/// Reference: "Space/Time Trade-offs in Hash Coding with Allowable Errors"
/// by Burton H. Bloom (1970)
public struct InJoinBloomFilter<Element: Hashable>: Sendable {
    private var bits: [UInt64]
    private let bitCount: Int
    private let hashCount: Int

    public init(expectedElements: Int, falsePositiveRate: Double = 0.01) {
        let n = Double(max(1, expectedElements))
        let p = max(0.0001, min(0.5, falsePositiveRate))

        let m = Int(ceil(-n * log(p) / (log(2) * log(2))))
        self.bitCount = max(64, m)

        let k = max(1, Int(round(Double(bitCount) / n * log(2))))
        self.hashCount = min(k, 10)

        let wordCount = (bitCount + 63) / 64
        self.bits = [UInt64](repeating: 0, count: wordCount)
    }

    public mutating func insert(_ value: Element) {
        let hashes = computeHashes(value)
        for hash in hashes {
            let index = Int(hash % UInt64(bitCount))
            let wordIndex = index / 64
            let bitIndex = index % 64
            bits[wordIndex] |= (1 << bitIndex)
        }
    }

    public func mightContain(_ value: Element) -> Bool {
        let hashes = computeHashes(value)
        for hash in hashes {
            let index = Int(hash % UInt64(bitCount))
            let wordIndex = index / 64
            let bitIndex = index % 64
            if (bits[wordIndex] & (1 << bitIndex)) == 0 {
                return false
            }
        }
        return true
    }

    private func computeHashes(_ value: Element) -> [UInt64] {
        let h1 = UInt64(bitPattern: Int64(value.hashValue))
        var hasher = Hasher()
        hasher.combine(value)
        hasher.combine(0x9E3779B9)
        let h2 = UInt64(bitPattern: Int64(hasher.finalize()))

        return (0..<hashCount).map { i in
            h1 &+ UInt64(i) &* h2
        }
    }
}
