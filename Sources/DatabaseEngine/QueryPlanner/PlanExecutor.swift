// PlanExecutor.swift
// QueryPlanner - Query plan execution

import Foundation
import Core

/// Errors that can occur during query plan execution
public enum PlanExecutionError: Error, Sendable {
    /// Aggregation operations are not yet implemented
    /// Aggregations return computed values (COUNT, SUM, etc.) rather than records,
    /// requiring a different return type than `[T]`
    case aggregationNotImplemented(type: String)

    /// The operation is not supported
    case unsupportedOperation(String)
}

extension PlanExecutionError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .aggregationNotImplemented(let type):
            return "Aggregation '\(type)' is not implemented. Aggregation operations require a different API that returns computed values rather than records."
        case .unsupportedOperation(let message):
            return "Unsupported operation: \(message)"
        }
    }
}

/// Executes a query plan and returns results
public final class PlanExecutor<T: Persistable & Codable>: @unchecked Sendable {

    private let context: FDBContext
    private let dataStore: any DataStore

    public init(context: FDBContext, dataStore: any DataStore) {
        self.context = context
        self.dataStore = dataStore
    }

    /// Execute a plan and return results
    public func execute(plan: QueryPlan<T>) async throws -> [T] {
        var results = try await executeOperator(plan.rootOperator)

        // Apply post-filter if needed
        if let postFilter = plan.postFilterPredicate {
            results = results.filter { evaluatePredicate(postFilter, on: $0) }
        }

        return results
    }

    /// Execute a plan and stream results
    public func stream(plan: QueryPlan<T>) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    try await self.streamOperator(
                        plan.rootOperator,
                        to: continuation,
                        postFilter: plan.postFilterPredicate
                    )
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    // MARK: - Operator Execution

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
            return sortResults(input, by: sortOp.sortDescriptors)

        case .limit(let limitOp):
            var input = try await executeOperator(limitOp.input)
            if let offset = limitOp.offset {
                input = Array(input.dropFirst(offset))
            }
            if let limit = limitOp.limit {
                input = Array(input.prefix(limit))
            }
            return input

        case .project(let projectOp):
            // Project doesn't change records, just limits fields
            // In practice, we'd return partial records
            return try await executeOperator(projectOp.input)

        case .fullTextScan(let ftOp):
            return try await executeFullTextScan(ftOp)

        case .vectorSearch(let vectorOp):
            return try await executeVectorSearch(vectorOp)

        case .spatialScan(let spatialOp):
            return try await executeSpatialScan(spatialOp)

        case .aggregation(let aggOp):
            return try await executeAggregation(aggOp)
        }
    }

    // MARK: - Table Scan

    private func executeTableScan(_ op: TableScanOperator<T>) async throws -> [T] {
        // Scan all records of type T
        let results = try await dataStore.scanRecords(type: T.self)

        // Apply filter if present
        if let filterPredicate = op.filterPredicate {
            return results.filter { evaluatePredicate(filterPredicate, on: $0) }
        }

        return results
    }

    // MARK: - Index Scan

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
            if let record: T = try await dataStore.fetchRecord(id: entry.recordId, type: T.self) {
                results.append(record)
            }
        }

        return results
    }

    /// Build FDB key range from scan bounds
    ///
    /// Returns an `IndexScanRange` that includes both values and inclusive/exclusive flags.
    /// The `inclusive` flags determine boundary behavior:
    /// - `startInclusive: true` → `>=` (include the start value)
    /// - `startInclusive: false` → `>` (exclude the start value)
    /// - `endInclusive: true` → `<=` (include the end value)
    /// - `endInclusive: false` → `<` (exclude the end value)
    private func buildScanRange(index: IndexDescriptor, bounds: IndexScanBounds) -> IndexScanRange {
        var startKey: [Any] = []
        var endKey: [Any] = []

        // Determine inclusiveness from the last component (or default to inclusive)
        var startInclusive = true
        var endInclusive = true

        for component in bounds.start {
            if let value = component.value?.value {
                startKey.append(value)
                startInclusive = component.inclusive
            }
        }

        for component in bounds.end {
            if let value = component.value?.value {
                endKey.append(value)
                endInclusive = component.inclusive
            }
        }

        return IndexScanRange(
            start: startKey,
            startInclusive: startInclusive,
            end: endKey,
            endInclusive: endInclusive
        )
    }

    // MARK: - Index Seek

    private func executeIndexSeek(_ op: IndexSeekOperator<T>) async throws -> [T] {
        var results: [T] = []

        for keyValues in op.seekValues {
            // Build the seek key
            let key = keyValues.map { $0.value }

            // Look up the index entry
            if let entry = try await dataStore.seekIndex(name: op.index.name, key: key) {
                // Fetch the record
                if let record: T = try await dataStore.fetchRecord(id: entry.recordId, type: T.self) {
                    results.append(record)
                }
            }
        }

        return results
    }

    // MARK: - Union

    private func executeUnion(_ op: UnionOperator<T>) async throws -> [T] {
        // Execute children sequentially to avoid Sendable issues with ID type
        var allResults: [[T]] = []
        for child in op.children {
            let result = try await executeOperator(child)
            allResults.append(result)
        }

        // Flatten and deduplicate using Persistable.ID
        // This works because Persistable.ID: Hashable (defined in protocol)
        if op.deduplicate {
            // Use string representation for deduplication to avoid Sendable issues
            var seenIds: Set<String> = []
            var results: [T] = []

            for childResult in allResults {
                for item in childResult {
                    let idString = "\(item.id)"
                    if !seenIds.contains(idString) {
                        seenIds.insert(idString)
                        results.append(item)
                    }
                }
            }

            return results
        } else {
            return allResults.flatMap { $0 }
        }
    }

    // MARK: - Intersection

    private func executeIntersection(_ op: IntersectionOperator<T>) async throws -> [T] {
        guard !op.children.isEmpty else { return [] }

        // Execute all children sequentially and collect ID sets
        var idSets: [Set<String>] = []
        var recordsByChild: [[T]] = []

        for child in op.children {
            let results = try await executeOperator(child)
            let idSet = Set(results.map { "\($0.id)" })
            idSets.append(idSet)
            recordsByChild.append(results)
        }

        // Intersect all ID sets
        guard var resultIds = idSets.first else { return [] }
        for otherSet in idSets.dropFirst() {
            resultIds = resultIds.intersection(otherSet)
        }

        // Use first child's records (already fetched)
        if let firstRecords = recordsByChild.first {
            return firstRecords.filter { resultIds.contains("\($0.id)") }
        }

        return []
    }

    // MARK: - Full Text Scan

    private func executeFullTextScan(_ op: FullTextScanOperator<T>) async throws -> [T] {
        // Execute full-text search
        let entries = try await dataStore.searchFullText(
            indexName: op.index.name,
            terms: op.searchTerms,
            matchMode: op.matchMode
        )

        // Fetch records
        var results: [T] = []
        for entry in entries {
            if let record: T = try await dataStore.fetchRecord(id: entry.recordId, type: T.self) {
                results.append(record)
            }
        }

        return results
    }

    // MARK: - Vector Search

    private func executeVectorSearch(_ op: VectorSearchOperator<T>) async throws -> [T] {
        // Execute vector similarity search
        let entries = try await dataStore.searchVector(
            indexName: op.index.name,
            queryVector: op.queryVector,
            k: op.k,
            efSearch: op.efSearch
        )

        // Fetch records
        var results: [T] = []
        for entry in entries {
            if let record: T = try await dataStore.fetchRecord(id: entry.recordId, type: T.self) {
                results.append(record)
            }
        }

        return results
    }

    // MARK: - Spatial Scan

    private func executeSpatialScan(_ op: SpatialScanOperator<T>) async throws -> [T] {
        // Execute spatial search
        let entries = try await dataStore.searchSpatial(
            indexName: op.index.name,
            constraint: op.constraint
        )

        // Fetch records
        var results: [T] = []
        for entry in entries {
            if let record: T = try await dataStore.fetchRecord(id: entry.recordId, type: T.self) {
                results.append(record)
            }
        }

        return results
    }

    // MARK: - Aggregation

    /// Execute an aggregation operation
    ///
    /// **⚠️ CURRENT LIMITATION**: This is a placeholder implementation that returns an empty array.
    ///
    /// Aggregation operations (COUNT, SUM, AVG, MIN, MAX) fundamentally differ from record queries:
    /// - They return computed scalar values, not `T` records
    /// - The return type should be something like `AggregationResult` rather than `[T]`
    ///
    /// **To properly implement aggregations**, consider:
    /// 1. Create a separate `executeAggregation() -> AggregationResult` method
    /// 2. Use a different operator type that doesn't fit in `PlanOperator<T>`
    /// 3. Or use a type-erased result wrapper
    ///
    /// **Example proper implementation**:
    /// ```swift
    /// struct AggregationResult {
    ///     let aggregationType: AggregationType
    ///     let value: Any  // Int for COUNT, Double for SUM/AVG, etc.
    ///     let groupKey: [String: Any]?  // For GROUP BY queries
    /// }
    ///
    /// func executeAggregation(_ op: AggregationOperator<T>) async throws -> [AggregationResult] {
    ///     switch op.aggregationType {
    ///     case .count:
    ///         let count = try await dataStore.countIndex(name: op.index.name)
    ///         return [AggregationResult(aggregationType: .count, value: count, groupKey: nil)]
    ///     case .sum(let field):
    ///         // Read from pre-computed aggregation index or scan and compute
    ///         ...
    ///     }
    /// }
    /// ```
    private func executeAggregation(_ op: AggregationOperator<T>) async throws -> [T] {
        // Aggregation operations are not yet implemented
        // They require a different return type (computed values vs records)
        let typeName: String
        switch op.aggregationType {
        case .count:
            typeName = "COUNT"
        case .sum(let field):
            typeName = "SUM(\(field))"
        case .min(let field):
            typeName = "MIN(\(field))"
        case .max(let field):
            typeName = "MAX(\(field))"
        case .avg(let field):
            typeName = "AVG(\(field))"
        }
        throw PlanExecutionError.aggregationNotImplemented(type: typeName)
    }

    // MARK: - Streaming

    private func streamOperator(
        _ op: PlanOperator<T>,
        to continuation: AsyncThrowingStream<T, Error>.Continuation,
        postFilter: Predicate<T>? = nil
    ) async throws {
        switch op {
        case .tableScan(let scanOp):
            try await streamTableScan(scanOp, to: continuation, postFilter: postFilter)

        case .indexScan(let scanOp):
            try await streamIndexScan(scanOp, to: continuation, postFilter: postFilter)

        case .filter(let filterOp):
            // Create a filtering stream
            let results = try await executeOperator(filterOp.input)
            for item in results {
                if evaluatePredicate(filterOp.predicate, on: item) {
                    // Apply post-filter if present
                    if let postFilter = postFilter {
                        if evaluatePredicate(postFilter, on: item) {
                            continuation.yield(item)
                        }
                    } else {
                        continuation.yield(item)
                    }
                }
            }

        case .limit(let limitOp):
            var count = 0
            let offset = limitOp.offset ?? 0
            let limit = limitOp.limit ?? Int.max

            let results = try await executeOperator(limitOp.input)
            for (index, item) in results.enumerated() {
                if index >= offset && count < limit {
                    // Apply post-filter if present
                    if let postFilter = postFilter {
                        if evaluatePredicate(postFilter, on: item) {
                            continuation.yield(item)
                            count += 1
                        }
                    } else {
                        continuation.yield(item)
                        count += 1
                    }
                }
            }

        default:
            // For other operators, fall back to batch execution
            let results = try await executeOperator(op)
            for item in results {
                // Apply post-filter if present
                if let postFilter = postFilter {
                    if evaluatePredicate(postFilter, on: item) {
                        continuation.yield(item)
                    }
                } else {
                    continuation.yield(item)
                }
            }
        }
    }

    private func streamTableScan(
        _ op: TableScanOperator<T>,
        to continuation: AsyncThrowingStream<T, Error>.Continuation,
        postFilter: Predicate<T>? = nil
    ) async throws {
        // Stream records
        for try await record in dataStore.streamRecords(type: T.self) {
            // Apply operator's filter first
            if let filter = op.filterPredicate {
                guard evaluatePredicate(filter, on: record) else { continue }
            }
            // Then apply post-filter if present
            if let postFilter = postFilter {
                guard evaluatePredicate(postFilter, on: record) else { continue }
            }
            continuation.yield(record)
        }
    }

    private func streamIndexScan(
        _ op: IndexScanOperator<T>,
        to continuation: AsyncThrowingStream<T, Error>.Continuation,
        postFilter: Predicate<T>? = nil
    ) async throws {
        let range = buildScanRange(index: op.index, bounds: op.bounds)

        // Stream index entries
        for try await entry in dataStore.streamIndex(name: op.index.name, range: range, reverse: op.reverse) {
            if let record: T = try await dataStore.fetchRecord(id: entry.recordId, type: T.self) {
                // Apply post-filter if present
                if let postFilter = postFilter {
                    guard evaluatePredicate(postFilter, on: record) else { continue }
                }
                continuation.yield(record)
            }
        }
    }

    // MARK: - Predicate Evaluation

    /// Evaluate a predicate against a model
    private func evaluatePredicate(_ predicate: Predicate<T>, on model: T) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return evaluateComparison(comparison, on: model)

        case .and(let predicates):
            return predicates.allSatisfy { evaluatePredicate($0, on: model) }

        case .or(let predicates):
            return predicates.contains { evaluatePredicate($0, on: model) }

        case .not(let inner):
            return !evaluatePredicate(inner, on: model)

        case .true:
            return true

        case .false:
            return false
        }
    }

    /// Evaluate a comparison against a model
    private func evaluateComparison(_ comparison: FieldComparison<T>, on model: T) -> Bool {
        // Get the field value from the model using the KeyPath
        let modelValue = getFieldValue(from: model, keyPath: comparison.keyPath, fieldName: comparison.fieldName)

        // Handle nil check operators first
        switch comparison.op {
        case .isNil:
            return modelValue == nil || isNilValue(modelValue!)

        case .isNotNil:
            guard let value = modelValue else { return false }
            return !isNilValue(value)

        default:
            break
        }

        // For other operators, we need a non-nil value
        guard let modelValue = modelValue else {
            return false
        }

        let expectedValue = comparison.value.value

        switch comparison.op {
        case .equal:
            return compareEqual(modelValue, expectedValue)

        case .notEqual:
            return !compareEqual(modelValue, expectedValue)

        case .lessThan:
            return compareLess(modelValue, expectedValue)

        case .lessThanOrEqual:
            return compareLess(modelValue, expectedValue) || compareEqual(modelValue, expectedValue)

        case .greaterThan:
            return compareLess(expectedValue, modelValue)

        case .greaterThanOrEqual:
            return compareLess(expectedValue, modelValue) || compareEqual(modelValue, expectedValue)

        case .contains:
            if let str = modelValue as? String, let substr = expectedValue as? String {
                return str.contains(substr)
            }
            return false

        case .hasPrefix:
            if let str = modelValue as? String, let prefix = expectedValue as? String {
                return str.hasPrefix(prefix)
            }
            return false

        case .hasSuffix:
            if let str = modelValue as? String, let suffix = expectedValue as? String {
                return str.hasSuffix(suffix)
            }
            return false

        case .in:
            // Handle AnySendable wrapping an array
            if let anySendableArray = expectedValue as? [AnySendable] {
                return anySendableArray.contains { compareEqual(modelValue, $0.value) }
            }
            // Handle case where AnySendable.value is an array
            if let innerArray = extractArrayFromValue(expectedValue) {
                return innerArray.contains { compareEqual(modelValue, $0) }
            }
            return false

        case .isNil, .isNotNil:
            // Already handled above
            return false
        }
    }

    /// Get field value from model using KeyPath or reflection
    private func getFieldValue(from model: T, keyPath: AnyKeyPath, fieldName: String) -> Any? {
        // Try to use the KeyPath directly if possible
        if let typedKeyPath = keyPath as? PartialKeyPath<T> {
            return model[keyPath: typedKeyPath]
        }

        // Fallback to Mirror-based reflection for nested fields
        return getFieldValueByReflection(from: model, fieldName: fieldName)
    }

    /// Get field value using Mirror reflection (supports dot notation for nested fields)
    private func getFieldValueByReflection(from object: Any, fieldName: String) -> Any? {
        let components = fieldName.split(separator: ".").map(String.init)
        var current: Any = object

        for component in components {
            let mirror = Mirror(reflecting: current)
            guard let child = mirror.children.first(where: { $0.label == component }) else {
                return nil
            }
            current = child.value
        }

        return current
    }

    /// Compare two values for equality
    private func compareEqual(_ lhs: Any, _ rhs: Any) -> Bool {
        // String types
        if let l = lhs as? String, let r = rhs as? String { return l == r }

        // Integer types
        if let l = lhs as? Int, let r = rhs as? Int { return l == r }
        if let l = lhs as? Int64, let r = rhs as? Int64 { return l == r }
        if let l = lhs as? Int32, let r = rhs as? Int32 { return l == r }
        if let l = lhs as? Int16, let r = rhs as? Int16 { return l == r }
        if let l = lhs as? Int8, let r = rhs as? Int8 { return l == r }
        if let l = lhs as? UInt, let r = rhs as? UInt { return l == r }
        if let l = lhs as? UInt64, let r = rhs as? UInt64 { return l == r }
        if let l = lhs as? UInt32, let r = rhs as? UInt32 { return l == r }
        if let l = lhs as? UInt16, let r = rhs as? UInt16 { return l == r }
        if let l = lhs as? UInt8, let r = rhs as? UInt8 { return l == r }

        // Floating point types
        if let l = lhs as? Double, let r = rhs as? Double { return l == r }
        if let l = lhs as? Float, let r = rhs as? Float { return l == r }

        // Cross-type numeric comparisons (Int vs Int64, etc.)
        if let l = toDouble(lhs), let r = toDouble(rhs) { return l == r }

        // Boolean
        if let l = lhs as? Bool, let r = rhs as? Bool { return l == r }

        // Date
        if let l = lhs as? Date, let r = rhs as? Date { return l == r }

        // UUID
        if let l = lhs as? UUID, let r = rhs as? UUID { return l == r }

        // Data
        if let l = lhs as? Data, let r = rhs as? Data { return l == r }

        // Fall back to string comparison for other types
        return "\(lhs)" == "\(rhs)"
    }

    /// Compare if lhs < rhs
    private func compareLess(_ lhs: Any, _ rhs: Any) -> Bool {
        // String types
        if let l = lhs as? String, let r = rhs as? String { return l < r }

        // Integer types
        if let l = lhs as? Int, let r = rhs as? Int { return l < r }
        if let l = lhs as? Int64, let r = rhs as? Int64 { return l < r }
        if let l = lhs as? Int32, let r = rhs as? Int32 { return l < r }
        if let l = lhs as? Int16, let r = rhs as? Int16 { return l < r }
        if let l = lhs as? Int8, let r = rhs as? Int8 { return l < r }
        if let l = lhs as? UInt, let r = rhs as? UInt { return l < r }
        if let l = lhs as? UInt64, let r = rhs as? UInt64 { return l < r }
        if let l = lhs as? UInt32, let r = rhs as? UInt32 { return l < r }
        if let l = lhs as? UInt16, let r = rhs as? UInt16 { return l < r }
        if let l = lhs as? UInt8, let r = rhs as? UInt8 { return l < r }

        // Floating point types
        if let l = lhs as? Double, let r = rhs as? Double { return l < r }
        if let l = lhs as? Float, let r = rhs as? Float { return l < r }

        // Cross-type numeric comparisons
        if let l = toDouble(lhs), let r = toDouble(rhs) { return l < r }

        // Date
        if let l = lhs as? Date, let r = rhs as? Date { return l < r }

        // UUID (lexicographic comparison via string)
        if let l = lhs as? UUID, let r = rhs as? UUID { return l.uuidString < r.uuidString }

        // Data (lexicographic comparison)
        if let l = lhs as? Data, let r = rhs as? Data {
            return l.lexicographicallyPrecedes(r)
        }

        return false
    }

    /// Convert numeric types to Double for cross-type comparison
    private func toDouble(_ value: Any) -> Double? {
        switch value {
        case let v as Int: return Double(v)
        case let v as Int64: return Double(v)
        case let v as Int32: return Double(v)
        case let v as Int16: return Double(v)
        case let v as Int8: return Double(v)
        case let v as UInt: return Double(v)
        case let v as UInt64: return Double(v)
        case let v as UInt32: return Double(v)
        case let v as UInt16: return Double(v)
        case let v as UInt8: return Double(v)
        case let v as Double: return v
        case let v as Float: return Double(v)
        default: return nil
        }
    }

    /// Check if a value is nil
    private func isNilValue(_ value: Any) -> Bool {
        if case Optional<Any>.none = value { return true }
        if "\(value)" == "nil" { return true }
        return false
    }

    /// Extract array elements from a value that might be an array
    ///
    /// Handles various array representations:
    /// - Direct `[Any]` arrays
    /// - Arrays wrapped in `AnySendable`
    /// - Arrays accessed via Mirror reflection
    private func extractArrayFromValue(_ value: Any) -> [Any]? {
        // Direct array cast
        if let array = value as? [Any] {
            return array
        }

        // Use Mirror to check if value is a collection
        let mirror = Mirror(reflecting: value)
        if mirror.displayStyle == .collection {
            return mirror.children.map { $0.value }
        }

        return nil
    }

    // MARK: - Sorting

    /// Sort results by sort descriptors
    private func sortResults(_ results: [T], by sortDescriptors: [SortDescriptor<T>]) -> [T] {
        guard !sortDescriptors.isEmpty else { return results }

        return results.sorted { lhs, rhs in
            for descriptor in sortDescriptors {
                let lhsValue = getFieldValue(from: lhs, keyPath: descriptor.keyPath, fieldName: descriptor.fieldName)
                let rhsValue = getFieldValue(from: rhs, keyPath: descriptor.keyPath, fieldName: descriptor.fieldName)

                // Handle nil values
                let lhsIsNil = lhsValue == nil || isNilValue(lhsValue!)
                let rhsIsNil = rhsValue == nil || isNilValue(rhsValue!)

                if lhsIsNil && rhsIsNil { continue }
                if lhsIsNil { return descriptor.order == .ascending }
                if rhsIsNil { return descriptor.order == .descending }

                let comparison = compareValues(lhsValue!, rhsValue!)
                if comparison != 0 {
                    return descriptor.order == .ascending ? (comparison < 0) : (comparison > 0)
                }
            }
            return false
        }
    }

    /// Compare two values, returning -1, 0, or 1
    private func compareValues(_ lhs: Any, _ rhs: Any) -> Int {
        if let l = lhs as? String, let r = rhs as? String {
            return l < r ? -1 : (l > r ? 1 : 0)
        }
        if let l = lhs as? Int, let r = rhs as? Int {
            return l < r ? -1 : (l > r ? 1 : 0)
        }
        if let l = lhs as? Int64, let r = rhs as? Int64 {
            return l < r ? -1 : (l > r ? 1 : 0)
        }
        if let l = lhs as? Double, let r = rhs as? Double {
            return l < r ? -1 : (l > r ? 1 : 0)
        }
        if let l = lhs as? Date, let r = rhs as? Date {
            return l < r ? -1 : (l > r ? 1 : 0)
        }
        return 0
    }
}

// MARK: - DataStore Extensions for Query Planning

// MARK: - Index Scan Range

/// Represents a range for index scanning with inclusive/exclusive bounds
public struct IndexScanRange: @unchecked Sendable {
    /// Start key values (type-erased, expected to be Sendable in practice)
    public let start: [Any]

    /// Whether the start bound is inclusive (>=) or exclusive (>)
    public let startInclusive: Bool

    /// End key values (type-erased, expected to be Sendable in practice)
    public let end: [Any]

    /// Whether the end bound is inclusive (<=) or exclusive (<)
    public let endInclusive: Bool

    public init(
        start: [Any] = [],
        startInclusive: Bool = true,
        end: [Any] = [],
        endInclusive: Bool = true
    ) {
        self.start = start
        self.startInclusive = startInclusive
        self.end = end
        self.endInclusive = endInclusive
    }

    /// Create an unbounded range (full scan)
    public static var unbounded: IndexScanRange {
        IndexScanRange()
    }
}

// MARK: - DataStore Extensions for Query Planning

extension DataStore {
    /// Scan all records of a type
    func scanRecords<T: Persistable & Codable>(type: T.Type) async throws -> [T] {
        // Implementation would use FDB range scan
        []
    }

    /// Stream records of a type
    func streamRecords<T: Persistable & Codable>(type: T.Type) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { _ in }
    }

    /// Scan index entries with proper inclusive/exclusive bounds
    ///
    /// - Parameters:
    ///   - name: Index name
    ///   - range: Scan range with inclusive/exclusive flags
    ///   - reverse: Whether to scan in reverse order
    /// - Returns: Matching index entries
    ///
    /// **Implementation Note**: The actual FDB implementation should use:
    /// - `startInclusive: true` → start key included in range
    /// - `startInclusive: false` → start key excluded (use `strinc(startKey)`)
    /// - `endInclusive: true` → use `strinc(endKey)` as end
    /// - `endInclusive: false` → use `endKey` as end (excluded by default in FDB)
    func scanIndex(name: String, range: IndexScanRange, reverse: Bool) async throws -> [IndexEntry] {
        []
    }

    /// Stream index entries with proper inclusive/exclusive bounds
    func streamIndex(name: String, range: IndexScanRange, reverse: Bool) -> AsyncThrowingStream<IndexEntry, Error> {
        AsyncThrowingStream { _ in }
    }

    /// Seek a specific key in an index
    func seekIndex(name: String, key: [Any]) async throws -> IndexEntry? {
        nil
    }

    /// Fetch a record by ID
    func fetchRecord<T: Persistable & Codable>(id: AnySendable, type: T.Type) async throws -> T? {
        nil
    }

    /// Search full-text index
    func searchFullText(indexName: String, terms: [String], matchMode: TextMatchMode) async throws -> [IndexEntry] {
        []
    }

    /// Search vector index
    func searchVector(indexName: String, queryVector: [Float], k: Int, efSearch: Int?) async throws -> [IndexEntry] {
        []
    }

    /// Search spatial index
    func searchSpatial(indexName: String, constraint: SpatialConstraint) async throws -> [IndexEntry] {
        []
    }
}

/// Represents an index entry
public struct IndexEntry: @unchecked Sendable {
    /// The record ID (type-erased but expected to be Sendable)
    public let recordId: AnySendable

    /// The indexed values
    public let indexedValues: [AnySendable]

    public init(recordId: AnySendable, indexedValues: [AnySendable]) {
        self.recordId = recordId
        self.indexedValues = indexedValues
    }

    /// Convenience initializer with Any types
    public init(recordIdAny: Any, indexedValuesAny: [Any]) {
        self.recordId = AnySendable(recordIdAny)
        self.indexedValues = indexedValuesAny.map { AnySendable($0) }
    }
}
