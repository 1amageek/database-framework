// PlanExecutor.swift
// QueryPlanner - Query plan execution

import Foundation
import Core
import FoundationDB

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
///
/// PlanExecutor takes a `QueryPlan` and executes it against a data store,
/// returning the matching records.
///
/// **Usage**:
/// ```swift
/// let executor = PlanExecutor<User>(context: context, executionContext: context)
/// let results = try await executor.execute(plan: queryPlan)
/// ```
///
/// **Architecture**:
/// - Record access via `QueryExecutionContext.scanRecords/fetchItem`
/// - Index access via `IndexSearcher` + `context.storageReader`
public final class PlanExecutor<T: Persistable & Codable>: @unchecked Sendable {

    private let context: FDBContext
    private let executionContext: any QueryExecutionContext

    public init(context: FDBContext, executionContext: any QueryExecutionContext) {
        self.context = context
        self.executionContext = executionContext
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

        case .indexOnlyScan(let scanOp):
            return try await executeIndexOnlyScan(scanOp)

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

        case .inUnion(let inUnionOp):
            return try await executeInUnion(inUnionOp)

        case .inJoin(let inJoinOp):
            return try await executeInJoin(inJoinOp)
        }
    }

    // MARK: - ID-Only Operator Execution

    /// Execute an operator returning only IDs (no record fetch)
    ///
    /// This is used for optimized set operations (UNION/INTERSECTION) where
    /// we first collect IDs, perform set operations, then batch fetch only
    /// the final needed records. This avoids fetching records that will be
    /// eliminated by the set operation.
    ///
    /// Reference: FDB Record Layer "remote fetch" optimization pattern
    private func executeOperatorIdsOnly(_ op: PlanOperator<T>) async throws -> Set<Tuple> {
        switch op {
        case .tableScan(let scanOp):
            return try await executeTableScanIdsOnly(scanOp)

        case .indexScan(let scanOp):
            return try await executeIndexScanIdsOnly(scanOp)

        case .indexSeek(let seekOp):
            return try await executeIndexSeekIdsOnly(seekOp)

        case .indexOnlyScan(let scanOp):
            return try await executeIndexOnlyScanIdsOnly(scanOp)

        case .union(let unionOp):
            // Union: combine IDs from all children
            var allIds: Set<Tuple> = []
            for child in unionOp.children {
                let childIds = try await executeOperatorIdsOnly(child)
                allIds.formUnion(childIds)
            }
            return allIds

        case .intersection(let intersectionOp):
            // Intersection: find common IDs across all children
            guard let firstChild = intersectionOp.children.first else { return [] }
            var resultIds = try await executeOperatorIdsOnly(firstChild)
            for child in intersectionOp.children.dropFirst() {
                let childIds = try await executeOperatorIdsOnly(child)
                resultIds = resultIds.intersection(childIds)
                // Early exit if no intersection possible
                if resultIds.isEmpty { return [] }
            }
            return resultIds

        case .filter(let filterOp):
            // For filter, we need to fetch records to apply the predicate
            // Then extract IDs from filtered results
            let input = try await executeOperator(filterOp.input)
            let filtered = input.filter { evaluatePredicate(filterOp.predicate, on: $0) }
            return Set(filtered.map { extractItemId($0) })

        case .sort(let sortOp):
            // Sort doesn't change which IDs are present
            return try await executeOperatorIdsOnly(sortOp.input)

        case .limit(let limitOp):
            // For limit, we need to materialize to apply limit properly
            let input = try await executeOperator(limitOp.input)
            var result = input
            if let offset = limitOp.offset {
                result = Array(result.dropFirst(offset))
            }
            if let limit = limitOp.limit {
                result = Array(result.prefix(limit))
            }
            return Set(result.map { extractItemId($0) })

        case .project(let projectOp):
            return try await executeOperatorIdsOnly(projectOp.input)

        case .fullTextScan(let ftOp):
            return try await executeFullTextScanIdsOnly(ftOp)

        case .vectorSearch(let vectorOp):
            return try await executeVectorSearchIdsOnly(vectorOp)

        case .spatialScan(let spatialOp):
            return try await executeSpatialScanIdsOnly(spatialOp)

        case .aggregation:
            // Aggregations don't return IDs
            return []

        case .inUnion(let inUnionOp):
            return try await executeInUnionIdsOnly(inUnionOp)

        case .inJoin(let inJoinOp):
            return try await executeInJoinIdsOnly(inJoinOp)
        }
    }

    /// Extract item ID as Tuple from a Persistable item
    private func extractItemId(_ item: T) -> Tuple {
        // Create a Tuple from the item's ID
        // Persistable.ID must be convertible to TupleElement
        if let element = item.id as? any TupleElement {
            return Tuple(element)
        }
        // Fallback: use string representation (less efficient but always works)
        return Tuple("\(item.id)")
    }

    // MARK: - Table Scan

    /// Execute a full table scan
    ///
    /// **Current Limitation**: Table scan fetches all records into memory before
    /// applying the filter predicate. This is because the underlying StorageReader
    /// does not support predicate push-down for arbitrary conditions.
    ///
    /// **Future Enhancement**: If the storage layer gains support for conditional
    /// scanning (e.g., server-side filtering in FoundationDB 7.x or via computed
    /// indexes), the filter could be pushed down to reduce I/O and memory usage.
    ///
    /// **Mitigation**: For filtered queries, the query planner should prefer
    /// index scans over table scans whenever a suitable index exists. Table scan
    /// with filter is only used as a fallback when no index can satisfy the query.
    ///
    /// **Memory Impact**: O(N) where N = total records of type T
    private func executeTableScan(_ op: TableScanOperator<T>) async throws -> [T] {
        // Scan all records of type T
        // NOTE: This fetches all records before filtering - see doc comment above
        let results = try await executionContext.scanRecords(type: T.self)

        // Apply filter in memory if present
        if let filterPredicate = op.filterPredicate {
            return results.filter { evaluatePredicate(filterPredicate, on: $0) }
        }

        return results
    }

    // MARK: - Index Scan

    private func executeIndexScan(_ op: IndexScanOperator<T>) async throws -> [T] {
        // Build query from bounds, with limit pushed down for early termination
        let query = buildScalarQuery(bounds: op.bounds, reverse: op.reverse, limit: op.limit)

        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Use IndexSearcher for index access
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Batch fetch items by ID for improved throughput
        // Collect all IDs first, then fetch in optimized batches
        let ids = entries.map { $0.itemID }
        return try await context.indexQueryContext.batchFetchItems(
            ids: ids,
            type: T.self,
            configuration: .default
        )
    }

    /// Build ScalarIndexQuery from IndexScanBounds
    ///
    /// Converts query planner bounds to IndexSearcher query format.
    private func buildScalarQuery(bounds: IndexScanBounds, reverse: Bool = false, limit: Int? = nil) -> ScalarIndexQuery {
        var startValues: [any TupleElement] = []
        var endValues: [any TupleElement] = []
        var startInclusive = true
        var endInclusive = true

        for component in bounds.start {
            if let element = component.value {
                startValues.append(element)
                startInclusive = component.inclusive
            }
        }

        for component in bounds.end {
            if let element = component.value {
                endValues.append(element)
                endInclusive = component.inclusive
            }
        }

        return ScalarIndexQuery(
            start: startValues.isEmpty ? nil : startValues,
            startInclusive: startInclusive,
            end: endValues.isEmpty ? nil : endValues,
            endInclusive: endInclusive,
            reverse: reverse,
            limit: limit
        )
    }

    /// Convert Any to TupleElement
    private func anyToTupleElement(_ value: Any) -> (any TupleElement)? {
        switch value {
        case let s as String: return s
        case let i as Int: return i
        case let i64 as Int64: return i64
        case let d as Double: return d
        case let f as Float: return Double(f)
        case let b as Bool: return b
        case let data as Data: return [UInt8](data)
        case let bytes as [UInt8]: return bytes
        default: return nil
        }
    }

    // MARK: - Index Seek

    private func executeIndexSeek(_ op: IndexSeekOperator<T>) async throws -> [T] {
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)

        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Collect all IDs from all seek operations first (ID-first approach)
        var allIds: [Tuple] = []

        for keyValues in op.seekValues {
            // keyValues is already [any TupleElement]
            let seekElements: [any TupleElement] = keyValues

            // Use equality query for point lookup
            let query = ScalarIndexQuery.equals(seekElements)
            let entries = try await searcher.search(
                query: query,
                in: indexSubspace,
                using: executionContext.storageReader
            )

            // Collect IDs only - no record fetch yet
            for entry in entries {
                allIds.append(entry.itemID)
            }
        }

        // Batch fetch all items at once for improved throughput
        return try await context.indexQueryContext.batchFetchItems(
            ids: allIds,
            type: T.self,
            configuration: .default
        )
    }

    // MARK: - Index-Only Scan

    private func executeIndexOnlyScan(_ op: IndexOnlyScanOperator<T>) async throws -> [T] {
        // Build query from bounds, with limit pushed down for early termination
        let query = buildScalarQuery(bounds: op.bounds, reverse: op.reverse, limit: op.limit)

        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Use IndexSearcher for index access
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Check if we can use true index-only scan (no item fetch)
        if op.metadata.isFullyCovering {
            // True Index-Only Scan: Reconstruct items from index data
            let decoder = IndexEntryDecoder<T>(metadata: op.metadata)
            var results: [T] = []
            var failedIds: [Tuple] = []

            for entry in entries {
                do {
                    let item = try decoder.decode(from: entry)
                    results.append(item)
                } catch {
                    // Decoding failed - collect for batch fallback fetch
                    failedIds.append(entry.itemID)
                }
            }

            // Batch fetch any entries that failed to decode
            if !failedIds.isEmpty {
                let fallbackItems = try await context.indexQueryContext.batchFetchItems(
                    ids: failedIds,
                    type: T.self,
                    configuration: .default
                )
                results.append(contentsOf: fallbackItems)
            }

            return results
        } else {
            // Partial coverage - must fetch items from storage using batch fetch
            // This path should not normally be reached if PlanEnumerator is correct,
            // as it should only generate index-only plans for fully covering indexes
            let ids = entries.map { $0.itemID }
            return try await context.indexQueryContext.batchFetchItems(
                ids: ids,
                type: T.self,
                configuration: .default
            )
        }
    }

    // MARK: - Union

    /// Execute union using ID-first approach for improved efficiency
    ///
    /// **Optimization**: Instead of fetching full records from each child and then
    /// deduplicating, we first collect only IDs from all children, deduplicate the
    /// IDs, then batch fetch only the unique records once. This avoids fetching
    /// records that will be eliminated by deduplication.
    ///
    /// Reference: FDB Record Layer "remote fetch" optimization pattern
    private func executeUnion(_ op: UnionOperator<T>) async throws -> [T] {
        // ID-first approach: collect IDs first, then batch fetch
        var allIds: Set<Tuple> = []

        for child in op.children {
            let childIds = try await executeOperatorIdsOnly(child)
            if op.deduplicate {
                allIds.formUnion(childIds)
            } else {
                // For non-deduplicated union, we can't use ID-first optimization
                // because we need to preserve duplicates - fall back to legacy approach
                return try await executeLegacyUnion(op)
            }
        }

        // Batch fetch only the unique records
        return try await context.indexQueryContext.batchFetchItems(
            ids: Array(allIds),
            type: T.self,
            configuration: .default
        )
    }

    /// Optimized union implementation for non-deduplicated case (UNION ALL)
    ///
    /// For non-deduplicated unions, we need to preserve exact order and duplicates
    /// from each child. The optimization here is to use ID-first execution for
    /// simple scan children (which don't require full record fetch for ordering),
    /// and fall back to full execution only for complex children.
    ///
    /// **Note**: For scan-only children, this fetches each unique record once.
    /// For complex children (with filters, sorts, limits), we must execute fully
    /// to preserve correct ordering.
    private func executeLegacyUnion(_ op: UnionOperator<T>) async throws -> [T] {
        // Check if all children are simple scans (can use ID-first approach)
        let allSimpleScans = op.children.allSatisfy { isSimpleScanOperator($0) }

        if allSimpleScans {
            // Optimized path: collect all IDs preserving order, batch fetch unique
            var allIds: [Tuple] = []

            for child in op.children {
                let childIds = try await executeOperatorIdsOnly(child)
                // Convert Set to Array (order within child doesn't matter for scans)
                allIds.append(contentsOf: childIds)
            }

            // Batch fetch all (some IDs may be duplicated across children)
            return try await context.indexQueryContext.batchFetchItems(
                ids: allIds,
                type: T.self,
                configuration: .default
            )
        } else {
            // Fall back: execute children fully and concatenate
            var allResults: [T] = []
            for child in op.children {
                let result = try await executeOperator(child)
                allResults.append(contentsOf: result)
            }
            return allResults
        }
    }

    /// Check if an operator is a simple scan (no ordering dependencies)
    private func isSimpleScanOperator(_ op: PlanOperator<T>) -> Bool {
        switch op {
        case .tableScan, .indexScan, .indexSeek, .indexOnlyScan,
             .fullTextScan, .vectorSearch, .spatialScan,
             .inUnion, .inJoin:
            return true
        case .union(let unionOp):
            return unionOp.children.allSatisfy { isSimpleScanOperator($0) }
        case .intersection(let intersectionOp):
            return intersectionOp.children.allSatisfy { isSimpleScanOperator($0) }
        default:
            return false
        }
    }

    // MARK: - Intersection

    /// Execute intersection using ID-first approach for improved efficiency
    ///
    /// **Optimization**: Instead of fetching full records from all children and then
    /// intersecting, we first collect only IDs from each child, compute the
    /// intersection of IDs, then batch fetch only the final intersected records.
    /// This avoids fetching records that will be eliminated by the intersection.
    ///
    /// **Early Exit**: If any child returns empty or the intersection becomes empty,
    /// we can short-circuit without fetching remaining children.
    ///
    /// Reference: FDB Record Layer "remote fetch" optimization pattern
    private func executeIntersection(_ op: IntersectionOperator<T>) async throws -> [T] {
        guard !op.children.isEmpty else { return [] }

        // ID-first approach: collect IDs, intersect, then batch fetch
        guard let firstChild = op.children.first else { return [] }

        // Get IDs from first child
        var resultIds = try await executeOperatorIdsOnly(firstChild)
        if resultIds.isEmpty { return [] }

        // Intersect with remaining children (with early exit optimization)
        for child in op.children.dropFirst() {
            let childIds = try await executeOperatorIdsOnly(child)
            resultIds = resultIds.intersection(childIds)

            // Early exit if no intersection possible
            if resultIds.isEmpty { return [] }
        }

        // Batch fetch only the intersected records
        return try await context.indexQueryContext.batchFetchItems(
            ids: Array(resultIds),
            type: T.self,
            configuration: .default
        )
    }

    // MARK: - ID-Only Scan Implementations

    /// Execute table scan returning only IDs
    private func executeTableScanIdsOnly(_ op: TableScanOperator<T>) async throws -> Set<Tuple> {
        // For table scan, we need to fetch records to get IDs
        // (unless we have a separate ID-only scan API)
        let results = try await executeTableScan(op)
        return Set(results.map { extractItemId($0) })
    }

    /// Execute index scan returning only IDs (no record fetch)
    private func executeIndexScanIdsOnly(_ op: IndexScanOperator<T>) async throws -> Set<Tuple> {
        let query = buildScalarQuery(bounds: op.bounds, reverse: op.reverse, limit: op.limit)

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Return only IDs - no record fetch!
        return Set(entries.map { $0.itemID })
    }

    /// Execute index seek returning only IDs (no record fetch)
    private func executeIndexSeekIdsOnly(_ op: IndexSeekOperator<T>) async throws -> Set<Tuple> {
        var ids: Set<Tuple> = []
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        for keyValues in op.seekValues {
            // keyValues is already [any TupleElement]
            let seekElements: [any TupleElement] = keyValues

            let query = ScalarIndexQuery.equals(seekElements)
            let entries = try await searcher.search(
                query: query,
                in: indexSubspace,
                using: executionContext.storageReader
            )

            // Collect IDs only
            for entry in entries {
                ids.insert(entry.itemID)
            }
        }

        return ids
    }

    /// Execute index-only scan returning only IDs (no record fetch or decode)
    private func executeIndexOnlyScanIdsOnly(_ op: IndexOnlyScanOperator<T>) async throws -> Set<Tuple> {
        let query = buildScalarQuery(bounds: op.bounds, reverse: op.reverse, limit: op.limit)

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Return only IDs - no record decode or fetch!
        return Set(entries.map { $0.itemID })
    }

    /// Execute full-text scan returning only IDs
    private func executeFullTextScanIdsOnly(_ op: FullTextScanOperator<T>) async throws -> Set<Tuple> {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = FullTextIndexSearcher()
        let query = FullTextIndexQuery(
            terms: op.searchTerms,
            matchMode: op.matchMode
        )
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        return Set(entries.map { $0.itemID })
    }

    /// Execute vector search returning only IDs
    private func executeVectorSearchIdsOnly(_ op: VectorSearchOperator<T>) async throws -> Set<Tuple> {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let dimensions = op.queryVector.count
        let searcher = VectorIndexSearcher(dimensions: dimensions, metric: op.distanceMetric)
        let query = VectorIndexQuery(queryVector: op.queryVector, k: op.k)

        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        return Set(entries.map { $0.itemID })
    }

    /// Execute spatial scan returning only IDs
    private func executeSpatialScanIdsOnly(_ op: SpatialScanOperator<T>) async throws -> Set<Tuple> {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = SpatialIndexSearcher()
        let query = SpatialIndexQuery(constraint: op.constraint)

        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        return Set(entries.map { $0.itemID })
    }

    // MARK: - Full Text Scan

    private func executeFullTextScan(_ op: FullTextScanOperator<T>) async throws -> [T] {
        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Use FullTextIndexSearcher for full-text search
        let searcher = FullTextIndexSearcher()
        let query = FullTextIndexQuery(
            terms: op.searchTerms,
            matchMode: op.matchMode
        )
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Fetch items
        var results: [T] = []
        for entry in entries {
            if let item: T = try await executionContext.fetchItem(id: entry.itemID, type: T.self) {
                results.append(item)
            }
        }

        return results
    }

    // MARK: - Vector Search

    private func executeVectorSearch(_ op: VectorSearchOperator<T>) async throws -> [T] {
        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Use VectorIndexSearcher for vector similarity search
        let searcher = VectorIndexSearcher(dimensions: op.queryVector.count)
        let query = VectorIndexQuery(
            queryVector: op.queryVector,
            k: op.k,
            efSearch: op.efSearch
        )
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Fetch items
        var results: [T] = []
        for entry in entries {
            if let item: T = try await executionContext.fetchItem(id: entry.itemID, type: T.self) {
                results.append(item)
            }
        }

        return results
    }

    // MARK: - Spatial Scan

    private func executeSpatialScan(_ op: SpatialScanOperator<T>) async throws -> [T] {
        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Use SpatialIndexSearcher for spatial search
        let searcher = SpatialIndexSearcher()
        let query = SpatialIndexQuery(constraint: op.constraint)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Fetch items
        var results: [T] = []
        for entry in entries {
            if let item: T = try await executionContext.fetchItem(id: entry.itemID, type: T.self) {
                results.append(item)
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
    ///         let count = try await executionReader.countIndex(name: op.index.name)
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

    // MARK: - IN-Union Execution

    /// Execute IN-Union: parallel index seeks for each value in the IN list
    ///
    /// **Algorithm**:
    /// 1. For each value in the IN list, create an index seek
    /// 2. Execute all seeks in parallel using TaskGroup
    /// 3. Union the results (deduplicate by ID)
    /// 4. Apply any additional filter
    ///
    /// **Reference**: FDB Record Layer InExtractor union strategy
    private func executeInUnion(_ op: any InOperatorExecutable<T>) async throws -> [T] {
        // Get index subspace
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)

        // Get values as TupleElements for index seeks
        let tupleElements = op.valuesAsTupleElements()

        // Execute all seeks in parallel using TaskGroup
        var allIdsList: [Tuple] = []
        var uniqueIds: Set<Tuple> = []

        try await withThrowingTaskGroup(of: [Tuple].self) { group in
            for element in tupleElements {
                group.addTask {
                    // Point lookup query
                    let query = ScalarIndexQuery.equals([element])
                    let entries = try await searcher.search(
                        query: query,
                        in: indexSubspace,
                        using: self.executionContext.storageReader
                    )

                    return entries.map { $0.itemID }
                }
            }

            // Collect results with deduplication
            for try await ids in group {
                for id in ids {
                    if uniqueIds.insert(id).inserted {
                        allIdsList.append(id)
                    }
                }
            }
        }

        // Batch fetch items
        var results = try await context.indexQueryContext.batchFetchItems(
            ids: allIdsList,
            type: T.self,
            configuration: .default
        )

        // Apply additional filter if present
        if let filter = op.additionalFilter {
            results = results.filter { evaluatePredicate(filter, on: $0) }
        }

        return results
    }

    /// Execute IN-Union returning only IDs (no record fetch)
    private func executeInUnionIdsOnly(_ op: any InOperatorExecutable<T>) async throws -> Set<Tuple> {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)

        // Get values as TupleElements for index seeks
        let tupleElements = op.valuesAsTupleElements()

        var allIds: Set<Tuple> = []

        try await withThrowingTaskGroup(of: [Tuple].self) { group in
            for element in tupleElements {
                group.addTask {
                    let query = ScalarIndexQuery.equals([element])
                    let entries = try await searcher.search(
                        query: query,
                        in: indexSubspace,
                        using: self.executionContext.storageReader
                    )

                    return entries.map { $0.itemID }
                }
            }

            for try await ids in group {
                allIds.formUnion(ids)
            }
        }

        return allIds
    }

    // MARK: - IN-Join Execution

    /// Execute IN-Join with optimized strategy selection
    ///
    /// **Algorithm**:
    /// 1. Analyze IN values to determine optimal execution strategy
    /// 2. Execute using the selected strategy:
    ///    - convertToUnion: Delegate to IN-Union for small value sets
    ///    - boundedRangeScan: Scan only the range between min/max values
    ///    - fullScan: Scan entire index with hash set filtering
    ///
    /// **Reference**: FDB Record Layer InExtractor join strategy
    private func executeInJoin(_ op: any InOperatorExecutable<T>) async throws -> [T] {
        // Select execution strategy
        let strategySelector = InJoinStrategySelector()
        let estimatedIndexSize = op.estimatedTotalResults * 10
        let strategy = strategySelector.selectStrategy(for: op, estimatedIndexSize: estimatedIndexSize)

        // Execute based on selected strategy
        let matchingIds: [Tuple]
        switch strategy {
        case .convertToUnion:
            // Delegate to IN-Union execution
            return try await executeInUnion(op)

        case .boundedRangeScan:
            matchingIds = try await executeInJoinBoundedScan(op: op)

        case .fullScan:
            matchingIds = try await executeInJoinFullScan(op: op)
        }

        // Batch fetch matching items
        var results = try await context.indexQueryContext.batchFetchItems(
            ids: matchingIds,
            type: T.self,
            configuration: .default
        )

        // Apply additional filter if present
        if let filter = op.additionalFilter {
            results = results.filter { evaluatePredicate(filter, on: $0) }
        }

        return results
    }

    /// Execute IN-Join with bounded range scan
    private func executeInJoinBoundedScan(op: any InOperatorExecutable<T>) async throws -> [Tuple] {
        guard let range = op.valueRange() else {
            return try await executeInJoinFullScan(op: op)
        }

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)

        let query = ScalarIndexQuery(
            start: [range.min],
            startInclusive: true,
            end: [range.max],
            endInclusive: true
        )

        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        return filterMatchingEntries(entries, op: op)
    }

    /// Execute IN-Join with full index scan
    private func executeInJoinFullScan(op: any InOperatorExecutable<T>) async throws -> [Tuple] {
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)

        let query = ScalarIndexQuery.all
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        return filterMatchingEntries(entries, op: op)
    }

    /// Filter index entries using operator's containsValue
    private func filterMatchingEntries(
        _ entries: [IndexEntry],
        op: any InOperatorExecutable<T>
    ) -> [Tuple] {
        var matchingIds: [Tuple] = []

        for entry in entries {
            if let firstKey = entry.keyValues[0] {
                if op.containsValue(firstKey) {
                    matchingIds.append(entry.itemID)
                }
            }
        }

        return matchingIds
    }

    /// Execute IN-Join returning only IDs (no record fetch)
    private func executeInJoinIdsOnly(_ op: any InOperatorExecutable<T>) async throws -> Set<Tuple> {
        let strategySelector = InJoinStrategySelector()
        let estimatedIndexSize = op.estimatedTotalResults * 10
        let strategy = strategySelector.selectStrategy(for: op, estimatedIndexSize: estimatedIndexSize)

        let matchingIds: [Tuple]
        switch strategy {
        case .convertToUnion:
            return try await executeInUnionIdsOnly(op)

        case .boundedRangeScan:
            matchingIds = try await executeInJoinBoundedScan(op: op)

        case .fullScan:
            matchingIds = try await executeInJoinFullScan(op: op)
        }

        return Set(matchingIds)
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
        for try await record in executionContext.streamRecords(type: T.self) {
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
        // Build query from bounds
        let query = buildScalarQuery(bounds: op.bounds, reverse: op.reverse)

        // Get index subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        // Use IndexSearcher for index access
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.keyPaths.count)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        // Fetch items for each entry
        for entry in entries {
            if let item: T = try await executionContext.fetchItem(id: entry.itemID, type: T.self) {
                // Apply post-filter if present
                if let postFilter = postFilter {
                    guard evaluatePredicate(postFilter, on: item) else { continue }
                }
                continuation.yield(item)
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
        let modelRaw = getFieldValue(from: model, keyPath: comparison.keyPath, fieldName: comparison.fieldName)
        let modelFieldValue = modelRaw.flatMap { FieldValue($0) } ?? .null

        // Handle nil check operators first
        switch comparison.op {
        case .isNil:
            return modelFieldValue.isNull

        case .isNotNil:
            return !modelFieldValue.isNull

        default:
            break
        }

        // For other operators with null model value, comparison fails
        if modelFieldValue.isNull {
            return false
        }

        let expectedValue = comparison.value

        switch comparison.op {
        case .equal:
            return modelFieldValue.isEqual(to: expectedValue)

        case .notEqual:
            return !modelFieldValue.isEqual(to: expectedValue)

        case .lessThan:
            return modelFieldValue.isLessThan(expectedValue)

        case .lessThanOrEqual:
            return modelFieldValue.isLessThan(expectedValue) || modelFieldValue.isEqual(to: expectedValue)

        case .greaterThan:
            return expectedValue.isLessThan(modelFieldValue)

        case .greaterThanOrEqual:
            return expectedValue.isLessThan(modelFieldValue) || modelFieldValue.isEqual(to: expectedValue)

        case .contains:
            if let str = modelRaw as? String, let substr = expectedValue.stringValue {
                return str.contains(substr)
            }
            return false

        case .hasPrefix:
            if let str = modelRaw as? String, let prefix = expectedValue.stringValue {
                return str.hasPrefix(prefix)
            }
            return false

        case .hasSuffix:
            if let str = modelRaw as? String, let suffix = expectedValue.stringValue {
                return str.hasSuffix(suffix)
            }
            return false

        case .in:
            // Check if model value is in the expected array
            if let arrayValues = expectedValue.arrayValue {
                return arrayValues.contains { modelFieldValue.isEqual(to: $0) }
            }
            return false

        case .isNil, .isNotNil:
            // Already handled above
            return false
        }
    }

    /// Get field value from model using KeyPath or dynamicMember subscript
    private func getFieldValue(from model: T, keyPath: AnyKeyPath, fieldName: String) -> Any? {
        // Try to use the KeyPath directly if possible
        if let typedKeyPath = keyPath as? PartialKeyPath<T> {
            return model[keyPath: typedKeyPath]
        }

        // Fallback to dynamicMember-based access for nested fields
        return getFieldValueByDynamicMember(from: model, fieldName: fieldName)
    }

    /// Get field value using Persistable's dynamicMember subscript
    ///
    /// Uses dynamicMember for first-level access (Persistable requirement),
    /// then falls back to Mirror for nested non-Persistable types.
    private func getFieldValueByDynamicMember(from model: T, fieldName: String) -> Any? {
        let components = fieldName.split(separator: ".").map(String.init)
        guard let firstComponent = components.first else { return nil }

        // First level: use Persistable's dynamicMember subscript
        guard let firstValue = model[dynamicMember: firstComponent] else { return nil }

        if components.count == 1 {
            return firstValue
        }

        // Nested levels: use Mirror for non-Persistable types
        var current: Any = firstValue
        for component in components.dropFirst() {
            let mirror = Mirror(reflecting: current)
            guard let child = mirror.children.first(where: { $0.label == component }) else {
                return nil
            }
            current = child.value
        }

        return current
    }

    // MARK: - Sorting

    /// Sort results by sort descriptors
    private func sortResults(_ results: [T], by sortDescriptors: [SortDescriptor<T>]) -> [T] {
        guard !sortDescriptors.isEmpty else { return results }

        return results.sorted { lhs, rhs in
            for descriptor in sortDescriptors {
                let lhsRaw = getFieldValue(from: lhs, keyPath: descriptor.keyPath, fieldName: descriptor.fieldName)
                let rhsRaw = getFieldValue(from: rhs, keyPath: descriptor.keyPath, fieldName: descriptor.fieldName)

                let lhsField = lhsRaw.flatMap { FieldValue($0) } ?? .null
                let rhsField = rhsRaw.flatMap { FieldValue($0) } ?? .null

                // null sorts first in ascending, last in descending
                if case .null = lhsField, case .null = rhsField { continue }
                if case .null = lhsField { return descriptor.order == .ascending }
                if case .null = rhsField { return descriptor.order == .descending }

                if let comparison = lhsField.compare(to: rhsField) {
                    switch comparison {
                    case .orderedAscending:
                        return descriptor.order == .ascending
                    case .orderedDescending:
                        return descriptor.order == .descending
                    case .orderedSame:
                        continue
                    }
                }
            }
            return false
        }
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

// MARK: - Query Execution Context Protocol

/// Protocol for query execution context
///
/// Provides access to records and raw storage for `PlanExecutor` and `IndexSearcher`.
///
/// **Architecture**:
/// - Record access: `scanRecords`, `fetchItem` (high-level)
/// - Index access: via `storageReader` + `IndexSearcher` (low-level)
///
/// **Usage**:
/// ```swift
/// // Record access
/// let records = try await context.scanRecords(type: User.self)
///
/// // Index search via IndexSearcher
/// let searcher = ScalarIndexSearcher(keyFieldCount: 1)
/// let entries = try await searcher.search(
///     indexName: "idx_email",
///     query: .equals(["test@example.com"]),
///     using: context.storageReader
/// )
/// ```
public protocol QueryExecutionContext: Sendable {

    /// Scan all records of a type
    func scanRecords<T: Persistable & Codable>(type: T.Type) async throws -> [T]

    /// Stream records of a type
    func streamRecords<T: Persistable & Codable>(type: T.Type) -> AsyncThrowingStream<T, Error>

    /// Fetch an item by ID
    func fetchItem<T: Persistable & Codable>(id: Tuple, type: T.Type) async throws -> T?

    /// Low-level storage reader for IndexSearcher implementations
    var storageReader: StorageReader { get }
}

/// Errors for DataStore operations
public enum DataStoreError: Error, CustomStringConvertible {
    case notImplemented(String)

    public var description: String {
        switch self {
        case .notImplemented(let method):
            return "DataStore method not implemented: \(method)"
        }
    }
}

// ExecutionIndexEntry has been unified with IndexEntry (see StorageReader.swift)
// Use IndexEntry for all index entry operations
