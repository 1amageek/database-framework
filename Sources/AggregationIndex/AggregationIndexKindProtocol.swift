// AggregationIndexKindProtocol.swift
// AggregationIndex - Common protocol for aggregation index kinds
//
// This protocol enables automatic index selection in AggregationQuery.
// When a query's groupBy fields and aggregation type match a defined index,
// the system can use the precomputed index instead of in-memory computation.

import Foundation
import Core

// MARK: - AggregationIndexKindProtocol

/// Common protocol for aggregation index kinds
///
/// This protocol enables the Execution Strategy Selector to:
/// 1. Identify aggregation indexes among all index descriptors
/// 2. Match query parameters (groupBy fields, aggregation type) against defined indexes
/// 3. Automatically use O(1) precomputed indexes instead of O(n) in-memory computation
///
/// **Conforming Types**:
/// - `CountIndexKind`: COUNT aggregation
/// - `SumIndexKind`: SUM aggregation
/// - `AverageIndexKind`: AVG aggregation
/// - `MinIndexKind`: MIN aggregation
/// - `MaxIndexKind`: MAX aggregation
/// - `DistinctIndexKind`: DISTINCT/cardinality (planned)
/// - `PercentileIndexKind`: PERCENTILE (planned)
///
/// **Usage in Query Optimization**:
/// ```
/// AggregationQueryBuilder.execute()
///     │
///     └── For each aggregation:
///         │
///         ├── Find IndexDescriptor where indexKind is AggregationIndexKindProtocol
///         ├── Check: aggregationType matches?
///         ├── Check: groupByFieldNames matches?
///         ├── Check: valueFieldName matches? (if applicable)
///         │
///         ├── Match found → Use IndexMaintainer [O(1)]
///         └── No match → Compute in-memory [O(n)]
/// ```
public protocol AggregationIndexKindProtocol: IndexKind {
    /// Aggregation type identifier
    ///
    /// Used to match query aggregation type with index aggregation type.
    ///
    /// **Standard Values**:
    /// - `"count"`: COUNT aggregation
    /// - `"sum"`: SUM aggregation
    /// - `"average"`: AVG aggregation
    /// - `"min"`: MIN aggregation
    /// - `"max"`: MAX aggregation
    /// - `"distinct"`: DISTINCT/cardinality aggregation (planned)
    /// - `"percentile"`: PERCENTILE aggregation (planned)
    var aggregationType: String { get }

    /// GROUP BY field names
    ///
    /// The fields used for grouping in this index.
    /// Must match exactly with the query's groupBy fields for index selection.
    ///
    /// **Example**:
    /// - Index: `CountIndexKind(groupBy: [\.region, \.status])`
    /// - Query: `.groupBy(\.region).groupBy(\.status)` → matches
    /// - Query: `.groupBy(\.region)` → doesn't match (different grouping)
    var groupByFieldNames: [String] { get }

    /// Value field name (for aggregations that operate on a specific field)
    ///
    /// - `nil` for COUNT (counts all records)
    /// - Field name for SUM, AVG, MIN, MAX, DISTINCT, PERCENTILE
    ///
    /// **Example**:
    /// - `SumIndexKind(groupBy: [\.region], value: \.amount)` → `"amount"`
    /// - `CountIndexKind(groupBy: [\.region])` → `nil`
    ///
    /// **Note**: Named `aggregationValueField` to avoid collision with existing
    /// `valueFieldName: String` properties on conforming types.
    var aggregationValueField: String? { get }
}

// MARK: - CountIndexKind Conformance

extension CountIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "count" }

    public var groupByFieldNames: [String] { fieldNames }

    public var aggregationValueField: String? { nil }
}

// MARK: - SumIndexKind Conformance

extension SumIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "sum" }

    // groupByFieldNames is already defined in SumIndexKind

    public var aggregationValueField: String? { valueFieldName }
}

// MARK: - AverageIndexKind Conformance

extension AverageIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "average" }

    // groupByFieldNames is already defined in AverageIndexKind

    public var aggregationValueField: String? { valueFieldName }
}

// MARK: - MinIndexKind Conformance

extension MinIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "min" }

    // groupByFieldNames is already defined in MinIndexKind

    public var aggregationValueField: String? { valueFieldName }
}

// MARK: - MaxIndexKind Conformance

extension MaxIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "max" }

    // groupByFieldNames is already defined in MaxIndexKind

    public var aggregationValueField: String? { valueFieldName }
}

// MARK: - CountNotNullIndexKind Conformance

extension CountNotNullIndexKind: AggregationIndexKindProtocol {
    public var aggregationType: String { "count_not_null" }

    // groupByFieldNames is already defined in CountNotNullIndexKind

    public var aggregationValueField: String? { valueFieldName }
}
