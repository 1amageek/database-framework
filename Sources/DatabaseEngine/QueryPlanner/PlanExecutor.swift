// PlanExecutor.swift
// QueryPlanner - Query plan execution

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import StorageKit

/// Errors that can occur during query plan execution
public enum PlanExecutionError: Error, Sendable {
    /// The operation is not supported
    case unsupportedOperation(String)
}

extension PlanExecutionError: CustomStringConvertible {
    public var description: String {
        switch self {
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
public final class PlanExecutor<T: Persistable & Codable>: Sendable {

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
            return Set(try filtered.map { try extractItemID($0) })

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
            return Set(try result.map { try extractItemID($0) })

        case .project(let projectOp):
            return try await executeOperatorIdsOnly(projectOp.input)

        case .fullTextScan(let ftOp):
            return try await executeFullTextScanIdsOnly(ftOp)

        case .vectorSearch(let vectorOp):
            return try await executeVectorSearchIdsOnly(vectorOp)

        case .spatialScan(let spatialOp):
            return try await executeSpatialScanIdsOnly(spatialOp)

        case .inUnion(let inUnionOp):
            return try await executeInUnionIdsOnly(inUnionOp)

        case .inJoin(let inJoinOp):
            return try await executeInJoinIdsOnly(inJoinOp)
        }
    }

    /// Extract item ID as Tuple from a Persistable item
    private func extractItemID(_ item: T) throws -> Tuple {
        try item.recordIdentifierTuple()
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
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)
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

    // MARK: - Index Seek

    private func executeIndexSeek(_ op: IndexSeekOperator<T>) async throws -> [T] {
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)

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
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)
        let entries = try await searcher.search(
            query: query,
            in: indexSubspace,
            using: executionContext.storageReader
        )

        guard op.metadata.isFullyCovering else {
            throw CanonicalIndexProjectionError.incompleteProjection(
                entity: T.persistableType,
                missingFields: Set(T.fieldSchemas.map(\.name))
                    .subtracting(op.metadata.allFields)
                    .sorted()
            )
        }
        let decoder = try IndexEntryDecoder<T>(metadata: op.metadata)
        return try entries.map { try decoder.decode(from: $0) }
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
        var allIDs: Set<Tuple> = []

        for child in op.children {
            let childIDs = try await executeOperatorIdsOnly(child)
            if op.deduplicate {
                allIDs.formUnion(childIDs)
            } else {
                return try await executeDuplicatePreservingUnion(op)
            }
        }

        // Batch fetch only the unique records
        return try await context.indexQueryContext.batchFetchItems(
            ids: Array(allIDs),
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
    private func executeDuplicatePreservingUnion(
        _ op: UnionOperator<T>
    ) async throws -> [T] {
        // Check if all children are simple scans (can use ID-first approach)
        let allSimpleScans = op.children.allSatisfy { isSimpleScanOperator($0) }

        if allSimpleScans {
            // Optimized path: collect all IDs preserving order, batch fetch unique
            var allIDs: [Tuple] = []

            for child in op.children {
                let childIDs = try await executeOperatorIdsOnly(child)
                // Convert Set to Array (order within child doesn't matter for scans)
                allIDs.append(contentsOf: childIDs)
            }

            // Batch fetch all (some IDs may be duplicated across children)
            return try await context.indexQueryContext.batchFetchItems(
                ids: allIDs,
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
        return Set(try results.map { try extractItemID($0) })
    }

    /// Execute index scan returning only IDs (no record fetch)
    private func executeIndexScanIdsOnly(_ op: IndexScanOperator<T>) async throws -> Set<Tuple> {
        let query = buildScalarQuery(bounds: op.bounds, reverse: op.reverse, limit: op.limit)

        let typeSubspace = try await context.indexQueryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(op.index.name)

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)
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
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)

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

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)
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

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)

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

        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)

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
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)

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
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)

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
        let searcher = ScalarIndexSearcher(keyFieldCount: op.index.fieldNames.count)
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
            return comparison.evaluate(on: model)

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

    // MARK: - Sorting

    /// Sort results by sort descriptors
    private func sortResults(_ results: [T], by sortDescriptors: [SortDescriptor<T>]) -> [T] {
        guard !sortDescriptors.isEmpty else { return results }

        return results.sorted { lhs, rhs in
            for descriptor in sortDescriptors {
                let result = descriptor.orderedComparison(lhs, rhs)
                if result != .orderedSame {
                    return result == .orderedAscending
                }
            }
            return false
        }
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

// ExecutionIndexEntry has been unified with IndexEntry (see StorageReader.swift)
// Use IndexEntry for all index entry operations
