// AggregationQuery.swift
// AggregationIndex - Query extension for aggregation operations

import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

// MARK: - Aggregation Query Builder

/// Builder for aggregation queries
///
/// **Usage**:
/// ```swift
/// import AggregationIndex
///
/// let stats = try await context.aggregate(Order.self)
///     .groupBy(Order.fields.region)
///     .count(as: "orderCount")
///     .sum(Order.fields.amount, as: "totalSales")
///     .having { $0.aggregateInt64("orderCount") ?? 0 > 10 }
///     .execute()
/// // Returns: [AggregateResult<Order>]
/// ```
///
/// **Type Preservation**:
/// - Group keys retain their canonical `FieldValue` representation.
/// - Aggregates return typed results:
///   - count: `FieldValue.int64`
///   - sum: exact integer result for integer inputs, `FieldValue.float64` for floating-point inputs
///   - avg: exact integer when integral, otherwise an exactly convertible `FieldValue.float64`
///   - min/max: `FieldValue?` (original type, nil for empty groups)
///   - sum/avg/min/max: `nil` when every input is null
///
/// **Grouping Behavior**:
/// - Empty grouping fields: All items grouped into single group (global aggregation)
/// - Null field values: Treated as `FieldValue.null` and grouped together
///
/// **Numeric Type Support** (via FieldValue):
/// - Integers: Int, Int8, Int16, Int32, Int64, UInt, UInt8, UInt16, UInt32, UInt64
/// - Floating-point: Float, Double
public struct AggregationQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private var groupByFields: [FieldIdentity] = []
    private var aggregations: [AggregationSpec] = []
    private var havingPredicate: (@Sendable (AggregateResult<T>) -> Bool)?

    private var groupByFieldNames: [String] {
        groupByFields.map { $0.name }
    }

    /// Forced index name (set via AggregationEntryPoint.using(index:))
    internal var forcedIndexName: String?

    /// Specification for an aggregation
    internal struct AggregationSpec: Sendable {
        let name: String
        let type: AggregationType
    }

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Add a field to GROUP BY
    ///
    /// - Parameter field: Typed field to group by
    /// - Returns: Updated query builder
    public func groupBy<V>(_ field: Field<T, V>) -> Self {
        var copy = self
        copy.groupByFields.append(field.identity)
        return copy
    }

    /// Add a COUNT aggregation
    ///
    /// - Parameter name: Name for the aggregation result
    /// - Returns: Updated query builder
    public func count(as name: String = "count") -> Self {
        var copy = self
        copy.aggregations.append(AggregationSpec(name: name, type: .count))
        return copy
    }

    /// Add a SUM aggregation
    ///
    /// - Parameters:
    ///   - field: Typed numeric field to sum
    ///   - name: Name for the aggregation result (defaults to "sum_fieldName")
    /// - Returns: Updated query builder
    public func sum<V: IndexNumericValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let fieldName = field.name
        let aggName = name ?? "sum_\(fieldName)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .sum(field: field.identity))
        )
        return copy
    }

    public func sum<V: IndexNumericValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let aggName = name ?? "sum_\(field.name)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .sum(field: field.identity))
        )
        return copy
    }

    /// Add an AVG aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the numeric field to average
    ///   - name: Name for the aggregation result (defaults to "avg_fieldName")
    /// - Returns: Updated query builder
    public func avg<V: IndexNumericValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let fieldName = field.name
        let aggName = name ?? "avg_\(fieldName)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .avg(field: field.identity))
        )
        return copy
    }

    public func avg<V: IndexNumericValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let aggName = name ?? "avg_\(field.name)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .avg(field: field.identity))
        )
        return copy
    }

    /// Add a MIN aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the comparable field
    ///   - name: Name for the aggregation result (defaults to "min_fieldName")
    /// - Returns: Updated query builder
    public func min<V: IndexComparableValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let fieldName = field.name
        let aggName = name ?? "min_\(fieldName)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .min(field: field.identity))
        )
        return copy
    }

    public func min<V: IndexComparableValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let aggName = name ?? "min_\(field.name)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .min(field: field.identity))
        )
        return copy
    }

    /// Add a MAX aggregation
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the comparable field
    ///   - name: Name for the aggregation result (defaults to "max_fieldName")
    /// - Returns: Updated query builder
    public func max<V: IndexComparableValue>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let fieldName = field.name
        let aggName = name ?? "max_\(fieldName)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .max(field: field.identity))
        )
        return copy
    }

    public func max<V: IndexComparableValue>(
        _ field: Field<T, V?>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let aggName = name ?? "max_\(field.name)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .max(field: field.identity))
        )
        return copy
    }

    /// Add a DISTINCT aggregation (approximate cardinality)
    ///
    /// Uses Set-based counting for in-memory computation.
    /// When a matching DistinctIndexKind exists, uses HyperLogLog++ for O(1) lookup.
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the field to count distinct values
    ///   - name: Name for the aggregation result (defaults to "distinct_fieldName")
    /// - Returns: Updated query builder
    ///
    /// **Note**: In-memory computation is exact. Precomputed index (HyperLogLog++)
    /// provides approximate results with ~1% error but O(1) lookup.
    public func distinct<V>(
        _ field: Field<T, V>,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let fieldName = field.name
        let aggName = name ?? "distinct_\(fieldName)"
        copy.aggregations.append(
            AggregationSpec(name: aggName, type: .distinct(field: field.identity))
        )
        return copy
    }

    /// Add a PERCENTILE aggregation
    ///
    /// Uses sorted array interpolation for in-memory computation.
    /// When a matching PercentileIndexKind exists, uses t-digest for O(1) lookup.
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the numeric field
    ///   - p: Percentile to compute (0.0 to 1.0, e.g., 0.99 for p99)
    ///   - name: Name for the aggregation result (defaults to "p{percentile}_fieldName")
    /// - Returns: Updated query builder
    ///
    /// **Note**: In-memory computation is exact. Precomputed index (t-digest)
    /// provides approximate results with high accuracy at extremes.
    public func percentile<V: IndexNumericValue>(
        _ field: Field<T, V>,
        p: Double,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let fieldName = field.name
        let percentileLabel = DatabaseTextFormatting.fixedDecimal(
            p * 100,
            fractionDigits: 0
        )
        let aggName = name ?? "p\(percentileLabel)_\(fieldName)"
        copy.aggregations.append(
            AggregationSpec(
                name: aggName,
                type: .percentile(field: field.identity, percentile: p)
            )
        )
        return copy
    }

    public func percentile<V: IndexNumericValue>(
        _ field: Field<T, V?>,
        p: Double,
        as name: String? = nil
    ) -> Self {
        var copy = self
        let percentileLabel = DatabaseTextFormatting.fixedDecimal(
            p * 100,
            fractionDigits: 0
        )
        let aggName = name ?? "p\(percentileLabel)_\(field.name)"
        copy.aggregations.append(
            AggregationSpec(
                name: aggName,
                type: .percentile(field: field.identity, percentile: p)
            )
        )
        return copy
    }

    /// Add a HAVING clause to filter aggregated results
    ///
    /// - Parameter predicate: Predicate to filter results
    /// - Returns: Updated query builder
    public func having(_ predicate: @escaping @Sendable (AggregateResult<T>) -> Bool) -> Self {
        var copy = self
        copy.havingPredicate = predicate
        return copy
    }

    /// Execute the aggregation query
    ///
    /// - Returns: Array of aggregate results
    /// - Throws: Error if execution fails
    ///
    /// **Execution Strategy**:
    /// 1. Check if all aggregations have matching precomputed indexes
    /// 2. If yes: Use bounded index-backed scans (O(G), G = groups)
    /// 3. If no: Fall back to in-memory computation (O(n))
    ///
    /// **Index Matching Criteria**:
    /// - Canonical descriptor metadata identifies the requested operation
    /// - Operation matches (count, sum, avg, min, max, distinct, percentile)
    /// - `groupByFieldNames` match exactly
    /// - `aggregationValueField` matches (for non-COUNT aggregations)
    ///
    /// **Direct Index Access** (for single aggregations):
    /// For maximum performance, use maintainers directly:
    /// ```swift
    /// let count = try await countMaintainer.getCount(groupingValues: [region], transaction: tx)
    /// let sum = try await sumMaintainer.getSum(groupingValues: [region], transaction: tx)
    /// ```
    public func execute() async throws -> [AggregateResult<T>] {
        guard !aggregations.isEmpty else {
            throw AggregationQueryError.noAggregations
        }
        var aggregationNames = Set<String>()
        for aggregation in aggregations {
            guard aggregationNames.insert(aggregation.name).inserted else {
                throw AggregationQueryError.duplicateAggregationName(
                    aggregation.name
                )
            }
            if case .percentile(_, let percentile) = aggregation.type {
                try CanonicalAggregationReducer.validate(percentile: percentile)
            }
        }

        // Determine execution strategy for each aggregation
        let strategies = try determineExecutionStrategies()

        // Check if all aggregations can use indexes
        let allIndexBacked = strategies.values.allSatisfy { strategy in
            if case .useIndex = strategy { return true }
            return false
        }

        // If all aggregations have matching indexes, use index-backed execution
        if allIndexBacked {
            return try await executeWithIndexes(strategies: strategies)
        }

        // Otherwise, fall back to in-memory computation
        let items = try await queryContext.context.fetch(T.self).execute()

        // The typed values are the identity. Dictionary hashing only selects a
        // bucket; exact FieldValue equality resolves every collision.
        var groups: [[FieldValue]: [T]] = [:]
        for item in items {
            let groupFieldValues = try CanonicalAggregationReducer.groupIdentity(
                item: item,
                fields: groupByFields
            )

            groups[groupFieldValues, default: []].append(item)
        }
        if groups.isEmpty && groupByFieldNames.isEmpty {
            groups[[]] = []
        }

        // Compute aggregates for each group
        var results: [AggregateResult<T>] = []
        for (groupFieldValues, groupItems) in groups {

            // Build group key dictionary from stored FieldValue (type-preserving)
            var groupKeyDict: [String: FieldValue] = [:]
            for (index, fieldName) in groupByFieldNames.enumerated() {
                if index < groupFieldValues.count {
                    groupKeyDict[fieldName] = groupFieldValues[index]
                }
            }

            // Compute aggregates
            var aggregateDict: [String: FieldValue?] = [:]
            for agg in aggregations {
                let value = try CanonicalAggregationReducer.aggregate(
                    items: groupItems,
                    aggregation: agg.type
                )
                aggregateDict.updateValue(value, forKey: agg.name)
            }

            let result = AggregateResult<T>(
                groupKey: groupKeyDict,
                aggregates: aggregateDict
            )

            // Apply HAVING filter
            if let havingPredicate = havingPredicate {
                if havingPredicate(result) {
                    results.append(result)
                }
            } else {
                results.append(result)
            }
        }

        return results
    }

    // MARK: - Index Selection (Execution Strategy Selector)

    /// Find a matching index for an aggregation
    ///
    /// Searches canonical index descriptor metadata for an index that matches
    /// the aggregation operation, group fields, and value field.
    ///
    /// **Matching Criteria**:
    /// 1. Descriptor kind identifier matches the requested operation
    /// 2. Canonical metadata operation matches the descriptor identifier
    /// 3. `groupByFieldNames` match exactly (same fields in same order)
    /// 4. `aggregationValueField` matches (for non-COUNT aggregations)
    ///
    /// **Supported Aggregation Types**:
    /// - All types (COUNT, SUM, AVG, DISTINCT, PERCENTILE, MIN, MAX) support batch queries
    /// - MIN/MAX use their bounded aggregated-layer batch scans
    ///
    /// - Parameter aggregation: The aggregation to find an index for
    /// - Returns: Matching IndexDescriptor, or nil if no match found
    private func findMatchingIndex(
        for aggregation: AggregationSpec
    ) throws -> IndexDescriptor? {
        let descriptors = queryContext.indexDescriptors(for: T.self)
        let expectedIdentifier = aggregationTypeIdentifier(for: aggregation.type)

        for descriptor in descriptors where descriptor.kindIdentifier == expectedIdentifier {
            let metadata = try AggregationIndexMetadata(canonical: descriptor.kind)
            guard metadata.operation.rawValue == expectedIdentifier else {
                continue
            }
            guard metadata.groupByFieldNames == groupByFieldNames else {
                continue
            }
            if let valueField = aggregationValueField(for: aggregation.type) {
                guard metadata.valueFieldName == valueField else {
                    continue
                }
            }
            return descriptor
        }

        return nil
    }

    /// Get the aggregation type identifier for matching with index kinds
    private func aggregationTypeIdentifier(for type: AggregationType) -> String {
        switch type {
        case .count:
            return "count"
        case .sum:
            return "sum"
        case .avg:
            return "average"
        case .min:
            return "min"
        case .max:
            return "max"
        case .distinct:
            return "distinct"
        case .percentile:
            return "percentile"
        }
    }

    /// Get the value field name for an aggregation type
    private func aggregationValueField(for type: AggregationType) -> String? {
        switch type {
        case .count:
            return nil
        case .sum(let field):
            return field.name
        case .avg(let field):
            return field.name
        case .min(let field):
            return field.name
        case .max(let field):
            return field.name
        case .distinct(let field):
            return field.name
        case .percentile(let field, _):
            return field.name
        }
    }

    /// Execution strategy for an aggregation
    internal enum ExecutionStrategy {
        /// Use a precomputed index (O(1) direct reads or O(G) batch scans).
        case useIndex(IndexDescriptor)

        /// Compute in memory (O(n))
        case inMemory
    }

    /// Determine the execution strategy for each aggregation
    ///
    /// Returns a mapping from aggregation name to execution strategy.
    /// If `forcedIndexName` is set, attempts to use that specific index.
    ///
    /// - Returns: Dictionary mapping aggregation names to their execution strategy
    internal func determineExecutionStrategies() throws -> [String: ExecutionStrategy] {
        var strategies: [String: ExecutionStrategy] = [:]

        for aggregation in aggregations {
            // If forced index is specified, try to use it
            if let forcedName = forcedIndexName {
                guard let descriptor = queryContext.indexDescriptors(
                    for: T.self
                ).first(where: { $0.name == forcedName }) else {
                    throw AggregationQueryError.indexNotFound(forcedName)
                }
                let metadata = try AggregationIndexMetadata(canonical: descriptor.kind)
                let expectedIdentifier = aggregationTypeIdentifier(for: aggregation.type)
                guard metadata.operation.rawValue == expectedIdentifier,
                      metadata.groupByFieldNames == groupByFieldNames,
                      metadata.valueFieldName == aggregationValueField(for: aggregation.type) else {
                    throw AggregationQueryError.indexDoesNotMatchQuery(forcedName)
                }
                strategies[aggregation.name] = .useIndex(descriptor)
                continue
            }

            // Otherwise, find a matching index automatically
            if let descriptor = try findMatchingIndex(for: aggregation) {
                strategies[aggregation.name] = .useIndex(descriptor)
            } else {
                strategies[aggregation.name] = .inMemory
            }
        }

        return strategies
    }

    // MARK: - Index-Backed Execution

    /// Execute aggregation query using precomputed indexes
    ///
    /// **Requirements**:
    /// - All aggregations must have matching indexes (checked by caller)
    /// - Supported: COUNT, SUM, AVG, DISTINCT, PERCENTILE, MIN, MAX
    ///
    /// - Parameter strategies: Execution strategies with index descriptors
    /// - Returns: Array of aggregate results from indexes
    private func executeWithIndexes(
        strategies: [String: ExecutionStrategy]
    ) async throws -> [AggregateResult<T>] {
        // Standard idExpression for Persistable types
        let idExpression = FieldKeyExpression(fieldName: "id")

        // Type for results collected inside transaction
        typealias AggregationIndexResult = (
            aggregationName: String,
            aggregationType: AggregationType,
            results: [(grouping: [FieldValue], value: FieldValue?)]
        )

        // Collect all aggregation results inside the transaction
        let allAggregationResults: [AggregationIndexResult] = try await queryContext.withTransaction { transaction in
            var collected: [AggregationIndexResult] = []

            for aggregation in self.aggregations {
                guard case .useIndex(let descriptor) = strategies[aggregation.name] else {
                    continue
                }

                guard let readableIndex = try await self.queryContext
                    .readableIndex(
                        named: descriptor.name,
                        kindIdentifier: descriptor.kindIdentifier,
                        for: T.self,
                        transaction: transaction
                    ) else {
                    continue
                }
                let index = Self.buildIndex(from: descriptor, persistableType: T.persistableType)
                let indexResults = try await self.queryFromIndex(
                    index: index,
                    subspace: readableIndex.subspace,
                    idExpression: idExpression,
                    aggregation: aggregation,
                    transaction: transaction
                )

                collected.append((
                    aggregationName: aggregation.name,
                    aggregationType: aggregation.type,
                    results: indexResults
                ))
            }

            return collected
        }

        // Merge results outside the transaction (no Sendable restrictions)
        var groupedResults: [[FieldValue]: (groupKey: [String: FieldValue], aggregates: [String: FieldValue?])] = [:]

        for aggResult in allAggregationResults {
            for (groupingValues, value) in aggResult.results {
                let (identity, groupKeyDict) = try groupKeyAndIdentity(
                    groupingValues
                )

                if var existing = groupedResults[identity] {
                    existing.aggregates.updateValue(
                        value,
                        forKey: aggResult.aggregationName
                    )
                    groupedResults[identity] = existing
                } else {
                    var aggregates: [String: FieldValue?] = [:]
                    aggregates.updateValue(
                        value,
                        forKey: aggResult.aggregationName
                    )
                    groupedResults[identity] = (
                        groupKey: groupKeyDict,
                        aggregates: aggregates
                    )
                }
            }
        }

        // A global aggregate always has exactly one identity, including when
        // its input or every sparse aggregate index is empty.
        let synthesizedEmptyGlobal = groupedResults.isEmpty && groupByFieldNames.isEmpty
        if synthesizedEmptyGlobal {
            groupedResults[[]] = (groupKey: [:], aggregates: [:])
        }

        // Convert to AggregateResult array
        var results: [AggregateResult<T>] = []
        results.reserveCapacity(groupedResults.count)
        for (groupKey, storedAggregates) in groupedResults.values {
            var aggregates = storedAggregates
            for aggregation in aggregations where !aggregates.keys.contains(aggregation.name) {
                if case .count = aggregation.type, !synthesizedEmptyGlobal {
                    throw AggregationQueryError.invalidIndexMetadata(
                        "Count index is missing a group produced by another aggregate index"
                    )
                }
                aggregates.updateValue(
                    Self.emptyValue(for: aggregation.type),
                    forKey: aggregation.name
                )
            }
            results.append(AggregateResult<T>(
                groupKey: groupKey,
                aggregates: aggregates
            ))
        }

        // Apply HAVING filter
        if let havingPredicate = havingPredicate {
            results.removeAll { !havingPredicate($0) }
        }

        return results
    }

    /// Query all grouped results from a canonical aggregation index.
    private func queryFromIndex(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        aggregation: AggregationSpec,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        let metadata = try AggregationIndexMetadata(canonical: index.kind)
        switch aggregation.type {
        case .count:
            let maintainer = CountIndexMaintainer<T>(
                index: index,
                subspace: subspace,
                idExpression: idExpression
            )
            let counts = try await maintainer.getAllCounts(transaction: transaction)
            return counts.map { ($0.grouping, FieldValue.int64($0.count)) }

        case .sum:
            return try await querySums(
                valueType: try requireValueType(metadata),
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                transaction: transaction
            )

        case .avg:
            return try await queryAverages(
                valueType: try requireValueType(metadata),
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                transaction: transaction
            )

        case .distinct:
            guard let precision = metadata.precision else {
                throw AggregationQueryError.invalidIndexMetadata(index.name)
            }
            let maintainer = DistinctIndexMaintainer<T>(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                precision: precision
            )
            let distincts = try await maintainer.getAllDistinctCounts(transaction: transaction)
            return try distincts.map { result in
                guard result.estimated >= 0 else {
                    throw AggregationQueryError.invalidIndexMetadata(index.name)
                }
                return (result.grouping, FieldValue.int64(result.estimated))
            }

        case .percentile(_, let p):
            guard let compression = metadata.compression else {
                throw AggregationQueryError.invalidIndexMetadata(index.name)
            }
            let maintainer = PercentileIndexMaintainer<T>(
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                compression: compression
            )
            let percentiles = try await maintainer.getAllPercentiles(
                percentiles: [p],
                transaction: transaction
            )
            return try percentiles.map { result in
                guard let value = result.values[p] else {
                    return (result.grouping, nil)
                }
                try CanonicalAggregationReducer.validate(
                    value: .float64(value),
                    field: index.name
                )
                return (result.grouping, FieldValue.float64(value))
            }

        case .min:
            return try await queryMinimums(
                valueType: try requireValueType(metadata),
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                transaction: transaction
            )

        case .max:
            return try await queryMaximums(
                valueType: try requireValueType(metadata),
                index: index,
                subspace: subspace,
                idExpression: idExpression,
                transaction: transaction
            )
        }
    }

    private func requireValueType(
        _ metadata: AggregationIndexMetadata
    ) throws -> IndexScalarType {
        guard let valueType = metadata.valueType else {
            throw AggregationQueryError.invalidIndexMetadata(
                metadata.operation.rawValue
            )
        }
        return valueType
    }

    private func querySums(
        valueType: IndexScalarType,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        switch valueType {

        case .int8:
            return try await querySums(Int8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int16:
            return try await querySums(Int16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int32:
            return try await querySums(Int32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int64:
            return try await querySums(Int64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)

        case .uint8:
            return try await querySums(UInt8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint16:
            return try await querySums(UInt16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint32:
            return try await querySums(UInt32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint64:
            return try await querySums(UInt64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float32:
            return try await querySums(Float.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float64:
            return try await querySums(Double.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .string, .date, .timestamp:
            throw AggregationQueryError.invalidIndexMetadata(index.name)
        }
    }

    private func querySums<Value: IndexNumericValue>(
        _ valueType: Value.Type,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        let maintainer = SumIndexMaintainer<T, Value>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
        let sums = try await maintainer.getAllSums(transaction: transaction)
        return try sums.map { result in
            try CanonicalAggregationReducer.validate(
                value: result.sum,
                field: index.name
            )
            return (result.grouping, result.sum)
        }
    }

    private func queryAverages(
        valueType: IndexScalarType,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        switch valueType {

        case .int8:
            return try await queryAverages(Int8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int16:
            return try await queryAverages(Int16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int32:
            return try await queryAverages(Int32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int64:
            return try await queryAverages(Int64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)

        case .uint8:
            return try await queryAverages(UInt8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint16:
            return try await queryAverages(UInt16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint32:
            return try await queryAverages(UInt32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint64:
            return try await queryAverages(UInt64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float32:
            return try await queryAverages(Float.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float64:
            return try await queryAverages(Double.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .string, .date, .timestamp:
            throw AggregationQueryError.invalidIndexMetadata(index.name)
        }
    }

    private func queryAverages<Value: IndexNumericValue>(
        _ valueType: Value.Type,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        let maintainer = AverageIndexMaintainer<T, Value>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
        let averages = try await maintainer.getAllAverages(
            transaction: transaction
        )
        return try averages.map { result in
            try CanonicalAggregationReducer.validate(
                value: result.average,
                field: index.name
            )
            return (result.grouping, result.average)
        }
    }

    private func queryMinimums(
        valueType: IndexScalarType,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        switch valueType {

        case .int8:
            return try await queryMinimums(Int8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int16:
            return try await queryMinimums(Int16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int32:
            return try await queryMinimums(Int32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int64:
            return try await queryMinimums(Int64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)

        case .uint8:
            return try await queryMinimums(UInt8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint16:
            return try await queryMinimums(UInt16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint32:
            return try await queryMinimums(UInt32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint64:
            return try await queryMinimums(UInt64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float32:
            return try await queryMinimums(Float.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float64:
            return try await queryMinimums(Double.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .string:
            return try await queryMinimums(String.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .date:
            return try await queryMinimums(CivilDate.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .timestamp:
            return try await queryMinimums(Timestamp.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        }
    }

    private func queryMinimums<Value: IndexComparableValue>(
        _ valueType: Value.Type,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        let maintainer = MinIndexMaintainer<T, Value>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
        let values = try await maintainer.getAllMins(transaction: transaction)
        return try values.map { result in
            let value = try CanonicalAggregationReducer.fieldValue(
                result.min,
                field: index.name
            )
            return (result.grouping, value)
        }
    }

    private func queryMaximums(
        valueType: IndexScalarType,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        switch valueType {

        case .int8:
            return try await queryMaximums(Int8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int16:
            return try await queryMaximums(Int16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int32:
            return try await queryMaximums(Int32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .int64:
            return try await queryMaximums(Int64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)

        case .uint8:
            return try await queryMaximums(UInt8.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint16:
            return try await queryMaximums(UInt16.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint32:
            return try await queryMaximums(UInt32.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .uint64:
            return try await queryMaximums(UInt64.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float32:
            return try await queryMaximums(Float.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .float64:
            return try await queryMaximums(Double.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .string:
            return try await queryMaximums(String.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .date:
            return try await queryMaximums(CivilDate.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        case .timestamp:
            return try await queryMaximums(Timestamp.self, index: index, subspace: subspace, idExpression: idExpression, transaction: transaction)
        }
    }

    private func queryMaximums<Value: IndexComparableValue>(
        _ valueType: Value.Type,
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [FieldValue], value: FieldValue?)] {
        let maintainer = MaxIndexMaintainer<T, Value>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
        let values = try await maintainer.getAllMaxs(transaction: transaction)
        return try values.map { result in
            let value = try CanonicalAggregationReducer.fieldValue(
                result.max,
                field: index.name
            )
            return (result.grouping, value)
        }
    }

    /// Builds the typed identity and presentation dictionary for an index group.
    private func groupKeyAndIdentity(
        _ values: [FieldValue]
    ) throws -> ([FieldValue], [String: FieldValue]) {
        guard values.count == groupByFieldNames.count else {
            throw AggregationQueryError.invalidIndexMetadata(
                "Index grouping arity does not match the query"
            )
        }
        var groupKeyDict: [String: FieldValue] = [:]
        groupKeyDict.reserveCapacity(values.count)
        for (index, value) in values.enumerated() {
            let fieldName = groupByFieldNames[index]
            try CanonicalAggregationReducer.validate(
                value: value,
                field: fieldName
            )
            groupKeyDict[fieldName] = value
        }
        return (values, groupKeyDict)
    }

    private static func emptyValue(
        for aggregation: AggregationType
    ) -> FieldValue? {
        switch aggregation {
        case .count, .distinct:
            return .int64(0)
        case .sum, .avg, .min, .max, .percentile:
            return nil
        }
    }

    // MARK: - Helper Functions

    /// Build Index from IndexDescriptor
    ///
    /// Creates an Index runtime object from the IndexDescriptor metadata.
    private static func buildIndex(from descriptor: IndexDescriptor, persistableType: String) -> Index {
        let rootExpression = KeyExpressionFactory.from(
            keyPaths: descriptor.fieldNames
        )

        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: rootExpression,
            subspaceKey: descriptor.name,
            itemTypes: Set([persistableType]),
            isUnique: descriptor.isUnique
        )
    }

}

// MARK: - Aggregation Query Error

/// Errors for aggregation query operations
public enum AggregationQueryError: Error, Sendable, Equatable, CustomStringConvertible {
    /// No aggregations specified
    case noAggregations

    /// Invalid field for aggregation
    case invalidField(String)

    /// Result names are unique within one aggregation query.
    case duplicateAggregationName(String)

    /// Index not found
    case indexNotFound(String)

    /// Forced index does not implement the requested aggregate layout.
    case indexDoesNotMatchQuery(String)

    /// Canonical index metadata is incomplete or inconsistent.
    case invalidIndexMetadata(String)

    /// A persisted field cannot be represented by the canonical aggregation value model.
    case invalidFieldValue(field: String, reason: TypeConversionError)

    /// A compiled model failed to encode a schema-identified persisted field.
    case persistedFieldEncodingFailed(
        field: String,
        reason: PersistableEncodingError
    )

    /// A numeric aggregation received a non-numeric value.
    case nonNumericValue(field: String, value: FieldValue)

    /// NaN and infinity are not valid aggregation inputs or results.
    case nonFiniteNumericValue(field: String)

    /// Integer and floating-point values cannot be mixed without an explicit cast.
    case incompatibleNumericKinds(field: String)

    /// An aggregate exceeded the representable result domain.
    case numericOverflow(operation: String, field: String)

    /// The exact result cannot be represented by `FieldValue`.
    case resultNotRepresentable(operation: String, field: String)

    /// MIN, MAX, or percentile received values without a common ordering.
    case incomparableValues(field: String, lhs: FieldValue, rhs: FieldValue)

    /// Percentiles are finite values in the closed interval from zero through one.
    case invalidPercentile(Double)

    public var description: String {
        switch self {
        case .noAggregations:
            return "No aggregations specified for aggregation query"
        case .invalidField(let field):
            return "Invalid field for aggregation: \(field)"
        case .duplicateAggregationName(let name):
            return "Duplicate aggregation result name: \(name)"
        case .indexNotFound(let name):
            return "Aggregation index not found: \(name)"
        case .indexDoesNotMatchQuery(let name):
            return "Aggregation index does not match query: \(name)"
        case .invalidIndexMetadata(let name):
            return "Aggregation index metadata is invalid: \(name)"
        case .invalidFieldValue(let field, let reason):
            return "Field '\(field)' cannot be converted for aggregation: \(reason)"
        case .persistedFieldEncodingFailed(let field, let reason):
            return "Field '\(field)' could not be encoded for aggregation: \(reason)"
        case .nonNumericValue(let field, let value):
            return "Field '\(field)' contains a non-numeric aggregation value: \(value)"
        case .nonFiniteNumericValue(let field):
            return "Field '\(field)' contains NaN or infinity"
        case .incompatibleNumericKinds(let field):
            return "Field '\(field)' mixes integer and floating-point aggregation values"
        case .numericOverflow(let operation, let field):
            return "Aggregation '\(operation)' overflowed for field '\(field)'"
        case .resultNotRepresentable(let operation, let field):
            return "Aggregation '\(operation)' for field '\(field)' has no exact FieldValue representation"
        case .incomparableValues(let field, let lhs, let rhs):
            return "Field '\(field)' contains incomparable values: \(lhs) and \(rhs)"
        case .invalidPercentile(let percentile):
            return "Percentile must be finite and between zero and one: \(percentile)"
        }
    }
}
