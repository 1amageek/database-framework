import StorageKit
import Core
import DatabaseValue

/// Canonical model persistence service over a `StorageEngine`.
///
/// This class owns schema-aware item and index behavior. Backend-specific key-value
/// behavior remains in the injected engine owned by `DBContainer`.
///
/// Key structure:
/// - Items: `[subspace]/items/[persistableType]/[id]` = serialized data
/// - Indexes: `[subspace]/indexes/[indexName]/[values]/[id]` = ''
///
/// **Metrics**: Operations are tracked via DataStoreDelegate (default: MetricsDataStoreDelegate).
/// Metrics include operation counts, durations, and item counts per type.
package final class DatabaseDataStore: DataStore, Sendable {
    /// Security delegate for access control evaluation
    package let securityDelegate: (any DataStoreSecurityDelegate)?

    // MARK: - Properties

    /// Container reference for transaction execution
    let container: DBContainer

    let subspace: Subspace
    let schema: Schema
    private let logger: DatabaseLogger

    /// Delegate for operation callbacks (metrics, etc.)
    private let metricsDelegate: DataStoreDelegate

    /// Items subspace: [subspace]/items/
    let itemSubspace: Subspace

    /// Indexes subspace: [subspace]/indexes/
    let indexSubspace: Subspace

    /// Blobs subspace: [subspace]/blobs/ - for large value chunks
    let blobsSubspace: Subspace

    /// Metadata subspace: [subspace]/_metadata/
    let metadataSubspace: Subspace

    /// Precomputed point-read prefix for the store's default type, when known.
    ///
    /// `DBContainer` creates and caches stores by `(type, path)`, so the default type
    /// is stable for hot point-read paths. Keeping the fully encoded prefix avoids
    /// repeated tuple encoding of the type name on every read.
    private let defaultPersistableType: String?
    private let defaultPointReadPrefix: Bytes?

    /// Index state manager for checking index readability
    let indexLifecycleStore: IndexLifecycleStore

    /// Violation tracker for uniqueness constraint violations
    ///
    /// Tracks violations during online indexing (writeOnly mode) instead of
    /// immediately throwing errors.
    let violationTracker: UniquenessViolationTracker

    /// Index maintenance service for all index operations
    let indexMaintenanceService: IndexMaintenanceService

    // MARK: - Initialization

    init(
        container: DBContainer,
        subspace: Subspace,
        persistableType: String? = nil,
        metricsDelegate: DataStoreDelegate? = nil,
        securityDelegate: (any DataStoreSecurityDelegate)? = nil,
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        self.container = container
        self.subspace = subspace
        self.schema = container.schema
        self.logger = container.configuration.logging.logger(
            label: "com.database.framework.data-store"
        )
        self.metricsDelegate = metricsDelegate ?? container.dataStoreDelegate
        self.securityDelegate = securityDelegate
        self.itemSubspace = subspace.subspace(SubspaceKey.items)
        self.indexSubspace = subspace.subspace(SubspaceKey.indexes)
        self.blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        self.metadataSubspace = subspace.subspace(SubspaceKey.metadata)
        self.defaultPersistableType = persistableType
        if let persistableType {
            var prefix = self.itemSubspace.prefix
            let encodedType = persistableType.encodeTuple()
            prefix.reserveCapacity(prefix.count + encodedType.count)
            prefix.append(contentsOf: encodedType)
            self.defaultPointReadPrefix = prefix
        } else {
            self.defaultPointReadPrefix = nil
        }
        self.indexLifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: subspace
        )
        self.violationTracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: subspace.subspace(SubspaceKey.metadata)
        )
        self.indexMaintenanceService = IndexMaintenanceService(
            indexLifecycleStore: indexLifecycleStore,
            violationTracker: violationTracker,
            indexSubspace: indexSubspace,
            maintainerProviders: container.runtimeConfiguration.indexMaintainerProviders,
            configurations: indexConfigurations
        )
    }

    // MARK: - Fetch Operations
    //
    // **Design Intent - No ReadVersionCache**:
    // Fetch operations use `container.engine.withTransaction()` directly without
    // ReadVersionCache. This is a deliberate simplification:
    //
    // 1. DatabaseDataStore is a low-level storage component that doesn't own a cache
    // 2. Cache ownership is at the DatabaseContext level (per unit-of-work)
    // 3. For weak read semantics optimization, users should use DatabaseContext.withTransaction()
    //
    // Example for optimized reads:
    // ```swift
    // let users = try await context.withTransaction(configuration: .readOnly) { tx in
    //     try await tx.fetch(User.self, ids: userIds)
    // }
    // ```

    /// Fetch all models of a type
    package func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T] {
        // Evaluate LIST security via delegate
        try securityDelegate?.evaluateList(
            type: type,
            limit: nil,
            offset: nil,
            orderBy: nil
        )

        let results = try await fetchAllInternal(type)
        guard let delegate = securityDelegate else { return results }
        return delegate.filterByGetAccess(results)
    }

    /// Internal fetchAll without security evaluation (for internal use after security is already evaluated)
    private func fetchAllInternal<T: Persistable>(_ type: T.Type) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        let startTime = MonotonicClock.now()

        do {
            let results: [T] = try await container.engine.withTransaction(configuration: .default) { transaction in
                // Use ItemStorage for proper handling of large values
                let storage = self.container.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: self.blobsSubspace
                )
                var results: [T] = []

                // ItemStorage.scan handles both inline and external (split) values transparently
                for try await (_, data) in storage.scan(begin: begin, end: end, snapshot: true) {
                    let model: T = try DataAccess.deserialize(data)
                    results.append(model)
                }
                return results
            }

            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailFetch(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Fetch a single model by ID
    package func fetch<T: Persistable>(_ type: T.Type, id: T.ID) async throws -> T? {
        let result: T? = try await container.engine.withTransaction { [self] transaction in
            try await self.fetchByIDInTransaction(type, id: id, transaction: transaction)
        }

        return result
    }

    /// Fetch models matching a query
    ///
    /// This method attempts to use indexes for efficient queries:
    /// 1. If predicate matches an index, use index scan instead of full table scan
    /// 2. If sorting matches an index, use index ordering
    /// 3. Fall back to full table scan + in-memory filtering if no suitable index
    package func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        // Evaluate LIST security via delegate
        let orderByFields = query.sortDescriptors.map { $0.fieldName }
        try securityDelegate?.evaluateList(
            type: T.self,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )

        let results = try await fetchInternal(query)
        guard let delegate = securityDelegate else { return results }
        return delegate.filterByGetAccess(results)
    }

    // MARK: - Index-Optimized Fetch

    /// Result from index-based fetch
    private struct IndexFetchResult<T: Persistable> {
        let models: [T]
        let needsPostFiltering: Bool
    }

    /// Attempt to fetch using an index
    ///
    /// Returns nil if no suitable index is available for the predicate,
    /// or if the index is not in readable state.
    private func fetchUsingIndex<T: Persistable>(
        _ predicate: Predicate<T>,
        type: T.Type,
        limit: Int?
    ) async throws -> IndexFetchResult<T>? {
        // Extract indexable condition from predicate
        guard let condition = try extractIndexableCondition(from: predicate),
              let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) else {
            return nil
        }

        // Check index state - only use readable indexes for queries
        let indexState = try await indexLifecycleStore.state(of: matchingIndex.name)
        guard indexState.isReadable else {
            logger.debug("Index '\(matchingIndex.name)' is not readable (state: \(indexState)), falling back to scan")
            return nil
        }

        // Build index scan range based on condition OUTSIDE transaction
        let indexSubspaceForIndex = indexSubspace.subspace(matchingIndex.name)
        let valueTuple = condition.valueTuple
        let indexedFieldCount = matchingIndex.fieldNames.count

        // Build value subspace using flat encoding (prefix + tuple.pack())
        // NOTE: Do NOT use indexSubspaceForIndex.subspace(valueTuple) because
        // Tuple conforms to TupleElement, which would create a NESTED tuple
        // encoding (type code 0x05) that doesn't match the flat key structure
        // written by ScalarIndexMaintainer.
        let valueSubspace = Subspace(prefix: indexSubspaceForIndex.prefix + valueTuple.pack())

        // Compute key range outside transaction to avoid capturing non-Sendable condition
        let scanRange: IndexScanRange
        switch condition.op {
        case .equal:
            let (begin, end) = valueSubspace.range()
            scanRange = .exactMatch(begin: begin, end: end, valueSubspace: valueSubspace)

        case .greaterThan:
            let beginKey = valueSubspace.range().1  // End of value range = start after
            let (_, endKey) = indexSubspaceForIndex.range()
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        case .greaterThanOrEqual:
            let beginKey = indexSubspaceForIndex.pack(valueTuple)
            let (_, endKey) = indexSubspaceForIndex.range()
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        case .lessThan:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = indexSubspaceForIndex.pack(valueTuple)
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        case .lessThanOrEqual:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = valueSubspace.range().1
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        default:
            // Other comparisons (contains, hasPrefix, etc.) are not index-optimizable
            return nil
        }

        // Execute scan in transaction - all captured values are now Sendable
        // Select optimal StreamingMode based on limit
        let streamingMode: StreamingMode = StreamingMode.forQuery(limit: limit)

        let ids: [Tuple] = try await container.engine.withTransaction(configuration: .default) { transaction in
            var ids: [Tuple] = []
            if let limit = limit {
                ids.reserveCapacity(limit)
            }

            switch scanRange {
            case .exactMatch(let begin, let end, let valueSubspace):
                // Apply limit pushdown to reduce server-side work
                let sequence = try await transaction.collectRange(
                    from: KeySelector.firstGreaterOrEqual(begin),
                    to: KeySelector.firstGreaterOrEqual(end),
                    limit: limit ?? 0,  // 0 = unlimited in FDB
                    reverse: false,
                    snapshot: true,
                    streamingMode: streamingMode
                )
                for (key, _) in sequence {
                    if let idTuple = try self.extractIDFromIndexKey(
                        key,
                        subspace: valueSubspace
                    ) {
                        ids.append(idTuple)
                    }
                }

            case .range(let begin, let end, let baseSubspace, let keyPathsCount):
                // Apply limit pushdown to reduce server-side work
                let sequence = try await transaction.collectRange(
                    from: KeySelector.firstGreaterOrEqual(begin),
                    to: KeySelector.firstGreaterOrEqual(end),
                    limit: limit ?? 0,  // 0 = unlimited in FDB
                    reverse: false,
                    snapshot: true,
                    streamingMode: streamingMode
                )
                for (key, _) in sequence {
                    if let idTuple = try self.extractIDFromIndexKey(
                        key,
                        baseSubspace: baseSubspace,
                        keyPathsCount: keyPathsCount
                    ) {
                        ids.append(idTuple)
                    }
                }
            }

            return ids
        }

        // If no IDs found, return empty result
        if ids.isEmpty {
            return IndexFetchResult(models: [], needsPostFiltering: false)
        }

        // Fetch models by IDs
        var models = try await fetchByIds(T.self, ids: ids)

        // Apply GET security filter
        if let delegate = securityDelegate {
            models = delegate.filterByGetAccess(models)
        }

        // Determine if post-filtering is needed
        // (needed if predicate has additional conditions beyond the indexed field)
        let needsPostFiltering = !isSimpleFieldPredicate(predicate, fieldName: condition.fieldName)

        return IndexFetchResult(models: models, needsPostFiltering: needsPostFiltering)
    }

    /// Represents a pre-computed index scan range (Sendable)
    private enum IndexScanRange: Sendable {
        case exactMatch(begin: Bytes, end: Bytes, valueSubspace: Subspace)
        /// Range scan with keyPathsCount to know how many elements are index values vs ID
        case range(begin: Bytes, end: Bytes, baseSubspace: Subspace, keyPathsCount: Int)
    }

    /// Extract a simple indexable condition from a predicate
    private struct IndexableCondition: Sendable {
        let fieldName: String
        let op: ComparisonOperator
        let valueTuple: Tuple
    }

    /// Extract all indexable conditions from a predicate
    ///
    /// For AND predicates, extracts all conditions that can potentially use an index.
    /// This enables compound index optimization.
    ///
    /// Throws `CanonicalReadError.unencodablePredicateValue` when an indexable
    /// operator's value cannot be encoded into the FDB tuple form. Silently
    /// falling back to a full scan here would mask data-shape bugs and is
    /// explicitly prohibited by project policy.
    private func extractAllIndexableConditions<T: Persistable>(from predicate: Predicate<T>) throws -> [IndexableCondition] {
        switch predicate {
        case .comparison(let comparison):
            switch comparison.op {
            case .equal, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual:
                let tuple: Tuple
                do {
                    tuple = try valueToTuple(comparison.value)
                } catch {
                    throw CanonicalReadError.unencodablePredicateValue(
                        field: comparison.fieldName,
                        valueDescription: String(describing: comparison.value)
                    )
                }
                return [IndexableCondition(fieldName: comparison.fieldName, op: comparison.op, valueTuple: tuple)]
            default:
                return []
            }

        case .and(let predicates):
            // Extract all indexable conditions from AND predicates
            return try predicates.flatMap { try extractAllIndexableConditions(from: $0) }

        default:
            return []
        }
    }

    /// Extract best indexable condition considering available indexes
    ///
    /// Priority:
    /// 1. Compound index matching multiple conditions (equals only)
    /// 2. Single field index with equals comparison
    /// 3. Single field index with range comparison
    ///
    /// - Parameter descriptors: The indexes the condition must be matchable against.
    ///   Defaults to `T.indexDescriptors`; callers pass a narrower list when a
    ///   forced-index hint restricts selection to one descriptor.
    private func extractIndexableCondition<T: Persistable>(
        from predicate: Predicate<T>,
        in descriptors: [IndexDescriptor]? = nil
    ) throws -> IndexableCondition? {
        let allConditions = try extractAllIndexableConditions(from: predicate)
        guard !allConditions.isEmpty else { return nil }

        // Build field-to-condition map for quick lookup
        var conditionsByField: [String: IndexableCondition] = [:]
        for condition in allConditions {
            // Prefer equals over range for the same field
            if let existing = conditionsByField[condition.fieldName] {
                if condition.op == .equal && existing.op != .equal {
                    conditionsByField[condition.fieldName] = condition
                }
            } else {
                conditionsByField[condition.fieldName] = condition
            }
        }

        // Find best matching index
        let descriptors = descriptors ?? T.indexDescriptors

        // Priority 1: Find compound index matching multiple equals conditions
        for descriptor in descriptors {
            guard descriptor.fieldNames.count > 1 else { continue }

            // Check whether the leading indexed fields have equality conditions.
            var matchCount = 0
            for fieldName in descriptor.fieldNames {
                if let condition = conditionsByField[fieldName], condition.op == .equal {
                    matchCount += 1
                } else {
                    break  // Must match from the beginning
                }
            }

            if matchCount >= 2 {
                var tupleElements: [any TupleElement] = []
                var firstFieldName: String?

                for fieldName in descriptor.fieldNames.prefix(matchCount) {
                    guard let condition = conditionsByField[fieldName],
                          condition.op == .equal else {
                        break
                    }
                    if firstFieldName == nil {
                        firstFieldName = fieldName
                    }
                    for index in 0..<condition.valueTuple.count {
                        if let element = condition.valueTuple[index] {
                            tupleElements.append(element)
                        }
                    }
                }

                if tupleElements.count == matchCount, let firstFieldName {
                    return IndexableCondition(
                        fieldName: firstFieldName,
                        op: .equal,
                        valueTuple: Tuple(tupleElements)
                    )
                }
            }
        }

        // Priority 2: Single field with equals
        for condition in allConditions where condition.op == .equal {
            if findMatchingIndex(for: condition, in: descriptors, type: T.self) != nil {
                return condition
            }
        }

        // Priority 3: Any indexable condition
        for condition in allConditions {
            if findMatchingIndex(for: condition, in: descriptors, type: T.self) != nil {
                return condition
            }
        }

        return allConditions.first
    }

    /// Find an index that matches the condition's field
    private func findMatchingIndex<T: Persistable>(
        for condition: IndexableCondition,
        in descriptors: [IndexDescriptor],
        type: T.Type
    ) -> IndexDescriptor? {
        // Find an index where the leading field matches the condition.
        for descriptor in descriptors {
            if descriptor.fieldNames.first == condition.fieldName {
                return descriptor
            }
        }
        return nil
    }

    /// Convert a value to a Tuple for index key construction
    ///
    /// Uses TupleEncoder for consistent type conversion across all index modules.
    private func valueToTuple(_ value: Any) throws -> Tuple {
        // Handle FieldValue first (most common case after refactoring)
        if let fieldValue = value as? FieldValue {
            return Tuple([try fieldValue.toTupleElement()])
        }

        // Handle values that are already TupleElement
        if let tupleElement = value as? any TupleElement {
            return Tuple([tupleElement])
        }

        // Use TupleEncoder for consistent type conversion
        let element = try TupleEncoder.encode(value)
        return Tuple([element])
    }

    /// Extract ID from an index key given a value subspace
    private func extractIDFromIndexKey(
        _ key: Bytes,
        subspace: Subspace
    ) throws -> Tuple? {
        let tuple = try subspace.unpack(key)
        if tuple.count > 0 {
            return tuple
        }
        return nil
    }

    /// Extract ID from an index key given a base subspace and keyPaths count
    ///
    /// Index key structure: [baseSubspace]/[value1]/[value2]/.../[id1]/[id2]/...
    /// The first `keyPathsCount` elements are index values, the rest are the ID.
    ///
    /// - Parameters:
    ///   - key: The raw key bytes
    ///   - baseSubspace: The index subspace
    ///   - keyPathsCount: Number of index key paths (determines how many elements are values vs ID)
    private func extractIDFromIndexKey(
        _ key: Bytes,
        baseSubspace: Subspace,
        keyPathsCount: Int
    ) throws -> Tuple? {
        let tuple = try baseSubspace.unpack(key)
        guard tuple.count > keyPathsCount else {
            return nil
        }

        var idElements: [any TupleElement] = []
        idElements.reserveCapacity(tuple.count - keyPathsCount)
        for index in keyPathsCount..<tuple.count {
            if let element = tuple[index] {
                idElements.append(element)
            }
        }
        guard !idElements.isEmpty else {
            return nil
        }
        return Tuple(idElements)
    }

    /// Fetch models by IDs (parallel reads for 10-30× speedup)
    private func fetchByIds<T: Persistable>(_ type: T.Type, ids: [Tuple]) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)

        // Pre-compute keys outside transaction
        let keys = ids.map { typeSubspace.pack($0) }

        return try await container.engine.withTransaction(configuration: .default) { transaction in
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )

            // Parallel reads within the same transaction
            return try await withThrowingTaskGroup(of: (Int, T?).self) { group in
                for (index, key) in keys.enumerated() {
                    group.addTask {
                        if let bytes = try await storage.read(for: key) {
                            let model: T = try DataAccess.deserialize(bytes)
                            return (index, model)
                        }
                        return (index, nil)
                    }
                }

                // Collect results preserving order
                var indexed: [(Int, T?)] = []
                indexed.reserveCapacity(keys.count)
                for try await result in group {
                    indexed.append(result)
                }
                indexed.sort { $0.0 < $1.0 }

                return indexed.compactMap { $0.1 }
            }
        }
    }

    /// Check if predicate is a simple field comparison (no AND/OR/NOT)
    private func isSimpleFieldPredicate<T: Persistable>(_ predicate: Predicate<T>, fieldName: String) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return comparison.fieldName == fieldName
        default:
            return false
        }
    }

    /// Fetch count of models matching a query
    ///
    /// This method attempts to use indexes for efficient counting:
    /// 1. If no predicate, count all entities without deserialization
    /// 2. If predicate matches an index, count using index scan
    /// 3. Fall back to fetch and count if no optimization possible
    package func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int {
        // Evaluate LIST security via delegate
        let orderByFields = query.sortDescriptors.map { $0.fieldName }
        try securityDelegate?.evaluateList(
            type: T.self,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )
        if query.fetchLimit == 0 {
            return 0
        }
        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // For count, we can optimize by not deserializing if no predicate
        if combinedPredicate == nil {
            let totalCount = try await countAll(T.self)
            return QueryResultWindow.resultCount(
                totalCount: totalCount,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            )
        }

        // Try to use index for counting
        if let predicate = combinedPredicate,
           let condition = try extractIndexableCondition(from: predicate),
           let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) {
            let totalCount = try await countUsingIndex(
                condition: condition,
                index: matchingIndex
            )
            return QueryResultWindow.resultCount(
                totalCount: totalCount,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            )
        }

        // Otherwise, fetch and count (security already evaluated above)
        let results = try await fetchInternal(query)
        return results.count
    }

    /// Internal fetch without security evaluation (for internal use after security is already evaluated)
    private func fetchInternal<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )
        if query.fetchLimit == 0 {
            return []
        }

        var results: [T]

        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // Try index-optimized fetch
        if let predicate = combinedPredicate,
           let indexResult = try await fetchUsingIndex(predicate, type: T.self, limit: nil) {
            results = indexResult.models

            // If index didn't cover all predicate conditions, apply remaining filters
            if indexResult.needsPostFiltering {
                results = results.filter { model in
                    evaluatePredicate(predicate, on: model)
                }
            }
        } else {
            // Fall back to full table scan
            results = try await fetchAllInternal(T.self)

            // Apply predicate filter
            if let predicate = combinedPredicate {
                results = results.filter { model in
                    evaluatePredicate(predicate, on: model)
                }
            }
        }

        // Apply sorting
        if !query.sortDescriptors.isEmpty {
            results.sort { lhs, rhs in
                for sortDescriptor in query.sortDescriptors {
                    let result = sortDescriptor.orderedComparison(lhs, rhs)
                    if result != .orderedSame {
                        return result == .orderedAscending
                    }
                }
                return false
            }
        }

        QueryResultWindow.apply(
            to: &results,
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )

        return results
    }

    // MARK: - Transaction-Injected Fetch (for DatabaseContext)

    /// Fetch items within an existing transaction
    ///
    /// This method is called by DatabaseContext.fetch() which manages the transaction
    /// and ReadVersionCache. DatabaseDataStore does not create transactions for this path.
    ///
    /// - Parameters:
    ///   - query: Query to execute
    ///   - transaction: Transaction to use for fetch
    /// - Returns: Array of matching items
    func fetchInTransaction<T: Persistable>(
        _ query: Query<T>,
        transaction: any TransactionAccess
    ) async throws -> [T] {
        // Security evaluation
        let orderByFields = query.sortDescriptors.map { $0.fieldName }
        try securityDelegate?.evaluateList(
            type: T.self,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )

        let results = try await fetchInternalWithTransaction(query, transaction: transaction)
        guard let delegate = securityDelegate else { return results }
        return delegate.filterByGetAccess(results)
    }

    /// Internal fetch within an existing transaction
    ///
    /// This method contains the core fetch logic without creating transactions.
    /// Called by DatabaseContext.fetch() which manages transaction and cache.
    private func fetchInternalWithTransaction<T: Persistable>(
        _ query: Query<T>,
        transaction: any TransactionAccess
    ) async throws -> [T] {
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )
        if query.fetchLimit == 0 {
            return []
        }

        var results: [T]

        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // Try index-optimized fetch
        if let predicate = combinedPredicate,
           let indexResult = try await fetchUsingIndexWithTransaction(
               predicate,
               type: T.self,
               requestedLimit: query.fetchLimit,
               offset: query.fetchOffset,
               hasSort: !query.sortDescriptors.isEmpty,
               forcedIndexName: query.forcedIndex?.indexName,
               transaction: transaction,
               workMeter: query.executionWorkMeter
           ) {
            results = indexResult.models

            // If index didn't cover all predicate conditions, apply remaining filters
            if indexResult.needsPostFiltering {
                results = try results.filter { model in
                    try query.executionWorkMeter?.consume(
                        at: .filterEvaluation
                    )
                    return evaluatePredicate(predicate, on: model)
                }
            }
        } else {
            // Fall back to full table scan. A forced index without a predicate
            // is a misuse — the fetch path has nothing to match against, so we
            // fail loudly rather than silently scanning the whole table.
            if query.forcedIndex != nil {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(query.forcedIndex!.indexName)' requires a filter predicate"
                )
            }
            results = try await fetchAllWithTransaction(
                T.self,
                transaction: transaction,
                workMeter: query.executionWorkMeter
            )

            // Apply predicate filter
            if let predicate = combinedPredicate {
                results = try results.filter { model in
                    try query.executionWorkMeter?.consume(
                        at: .filterEvaluation
                    )
                    return evaluatePredicate(predicate, on: model)
                }
            }
        }

        // Apply sorting
        if !query.sortDescriptors.isEmpty {
            try query.executionWorkMeter?.consume(
                UInt64(results.count),
                at: .sortInput
            )
            try results.sort { lhs, rhs in
                for sortDescriptor in query.sortDescriptors {
                    try query.executionWorkMeter?.consume(
                        2,
                        at: .sortComparison
                    )
                    let result = sortDescriptor.orderedComparison(lhs, rhs)
                    if result != .orderedSame {
                        return result == .orderedAscending
                    }
                }
                return false
            }
        }

        QueryResultWindow.apply(
            to: &results,
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )

        try query.executionWorkMeter?.consume(
            UInt64(results.count),
            at: .projection
        )

        return results
    }

    /// Fetch all models with an existing transaction
    private func fetchAllWithTransaction<T: Persistable>(
        _ type: T.Type,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter?
    ) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        let startTime = MonotonicClock.now()

        do {
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )
            var results: [T] = []

            // ItemStorage.scan handles both inline and external (split) values transparently
            for try await (_, data) in storage.scan(begin: begin, end: end, snapshot: true) {
                try workMeter?.consume(at: .storageRow)
                let model: T = try DataAccess.deserialize(data)
                results.append(model)
            }

            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailFetch(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Attempt to fetch using an index with an existing transaction.
    ///
    /// - Parameters:
    ///   - forcedIndexName: When set, restricts index selection to the named
    ///     index. Mismatches (index not found, or predicate has no condition on
    ///     the index's leading field) raise `CanonicalReadError` instead of
    ///     silently falling back to a full scan — the caller has asked for this
    ///     exact index and deserves an explicit failure.
    private func fetchUsingIndexWithTransaction<T: Persistable>(
        _ predicate: Predicate<T>,
        type: T.Type,
        requestedLimit: Int?,
        offset: Int?,
        hasSort: Bool,
        forcedIndexName: String? = nil,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter?
    ) async throws -> IndexFetchResult<T>? {
        // Restrict descriptors when a forced index hint is present.
        let candidateDescriptors: [IndexDescriptor]
        if let forcedIndexName {
            guard let forced = T.indexDescriptors.first(where: { $0.name == forcedIndexName }) else {
                throw CanonicalReadError.indexHintNotFound(
                    "Forced index '\(forcedIndexName)' not found on type '\(T.persistableType)'"
                )
            }
            candidateDescriptors = [forced]
        } else {
            candidateDescriptors = T.indexDescriptors
        }

        // Extract indexable condition from predicate
        guard let condition = try extractIndexableCondition(from: predicate, in: candidateDescriptors),
              let matchingIndex = findMatchingIndex(for: condition, in: candidateDescriptors, type: T.self) else {
            if forcedIndexName != nil {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(forcedIndexName!)' cannot serve the given predicate on type '\(T.persistableType)'"
                )
            }
            return nil
        }
        let needsPostFiltering = !isSimpleFieldPredicate(
            predicate,
            fieldName: condition.fieldName
        )
        let indexReadLimit = QueryResultWindow.indexReadLimit(
            requestedLimit: requestedLimit,
            offset: offset,
            hasSort: hasSort,
            requiresPostFilter: needsPostFiltering,
            hasSecurityFilter: securityDelegate != nil
        )

        // Check index state - only use readable indexes for queries
        // Use transaction-aware overload to avoid nested transaction deadlock
        let indexState = try await indexLifecycleStore.state(of: matchingIndex.name, transaction: transaction)
        guard indexState.isReadable else {
            logger.debug("Index '\(matchingIndex.name)' is not readable (state: \(indexState)), falling back to scan")
            return nil
        }

        // Build index scan range based on condition
        let indexSubspaceForIndex = indexSubspace.subspace(matchingIndex.name)
        let valueTuple = condition.valueTuple
        let indexedFieldCount = matchingIndex.fieldNames.count

        // Build value subspace using flat encoding (prefix + tuple.pack())
        // NOTE: Do NOT use indexSubspaceForIndex.subspace(valueTuple) because
        // Tuple conforms to TupleElement, which would create a NESTED tuple
        // encoding (type code 0x05) that doesn't match the flat key structure
        // written by ScalarIndexMaintainer.
        let valueSubspace = Subspace(prefix: indexSubspaceForIndex.prefix + valueTuple.pack())

        // Compute key range
        let scanRange: IndexScanRange
        switch condition.op {
        case .equal:
            let (begin, end) = valueSubspace.range()
            scanRange = .exactMatch(begin: begin, end: end, valueSubspace: valueSubspace)

        case .greaterThan:
            let beginKey = valueSubspace.range().1  // End of value range = start after
            let (_, endKey) = indexSubspaceForIndex.range()
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        case .greaterThanOrEqual:
            let beginKey = indexSubspaceForIndex.pack(valueTuple)
            let (_, endKey) = indexSubspaceForIndex.range()
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        case .lessThan:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = indexSubspaceForIndex.pack(valueTuple)
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        case .lessThanOrEqual:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = valueSubspace.range().1
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: indexedFieldCount)

        default:
            // Other comparisons (contains, hasPrefix, etc.) are not index-optimizable
            return nil
        }

        // Execute scan with provided transaction
        let storageLimit = try boundedStorageLimit(
            requested: indexReadLimit,
            workMeter: workMeter
        )
        let streamingMode: StreamingMode = StreamingMode.forQuery(
            limit: storageLimit
        )

        var ids: [Tuple] = []
        if let storageLimit {
            ids.reserveCapacity(min(storageLimit, 4_096))
        }

        try workMeter?.consume(at: .indexScan)

        switch scanRange {
        case .exactMatch(let begin, let end, let valueSubspace):
            let sequence = try await transaction.collectRange(
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: storageLimit ?? 0,
                reverse: false,
                snapshot: true,
                streamingMode: streamingMode
            )
            for (key, _) in sequence {
                try workMeter?.consume(at: .storageRow)
                if let idTuple = try self.extractIDFromIndexKey(
                    key,
                    subspace: valueSubspace
                ) {
                    ids.append(idTuple)
                }
            }

        case .range(let begin, let end, let baseSubspace, let keyPathsCount):
            let sequence = try await transaction.collectRange(
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: storageLimit ?? 0,
                reverse: false,
                snapshot: true,
                streamingMode: streamingMode
            )
            for (key, _) in sequence {
                try workMeter?.consume(at: .storageRow)
                if let idTuple = try self.extractIDFromIndexKey(
                    key,
                    baseSubspace: baseSubspace,
                    keyPathsCount: keyPathsCount
                ) {
                    ids.append(idTuple)
                }
            }
        }

        // If no IDs found, return empty result
        if ids.isEmpty {
            return IndexFetchResult(models: [], needsPostFiltering: false)
        }

        // Fetch models by IDs with provided transaction
        var models = try await fetchByIdsWithTransaction(
            T.self,
            ids: ids,
            transaction: transaction,
            workMeter: workMeter
        )

        // Apply GET security filter
        if let delegate = securityDelegate {
            models = delegate.filterByGetAccess(models)
        }

        return IndexFetchResult(models: models, needsPostFiltering: needsPostFiltering)
    }

    /// Fetch models by IDs with an existing transaction (parallel reads)
    private func fetchByIdsWithTransaction<T: Persistable>(
        _ type: T.Type,
        ids: [Tuple],
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter?
    ) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let keys = ids.map { typeSubspace.pack($0) }

        let storage = self.container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )

        try workMeter?.consume(UInt64(keys.count), at: .storageRow)

        // Parallel reads within the same transaction
        return try await withThrowingTaskGroup(of: (Int, T?).self) { group in
            for (index, key) in keys.enumerated() {
                group.addTask {
                    if let bytes = try await storage.read(for: key) {
                        let model: T = try DataAccess.deserialize(bytes)
                        return (index, model)
                    }
                    return (index, nil)
                }
            }

            // Collect results preserving order
            var indexed: [(Int, T?)] = []
            indexed.reserveCapacity(keys.count)
            for try await result in group {
                indexed.append(result)
            }
            indexed.sort { $0.0 < $1.0 }

            return indexed.compactMap { $0.1 }
        }
    }

    private func boundedStorageLimit(
        requested: Int?,
        workMeter: DatabaseWorkMeter?
    ) throws -> Int? {
        guard let workMeter else { return requested }
        let budgetLimit = try workMeter.storageReadLimitWithSentinel()
        guard let requested else { return budgetLimit }
        return max(1, min(requested, budgetLimit))
    }

    /// Fetch count within an existing transaction
    ///
    /// Called by DatabaseContext.fetchCount() which manages transaction and ReadVersionCache.
    func fetchCountInTransaction<T: Persistable>(
        _ query: Query<T>,
        transaction: any TransactionAccess
    ) async throws -> Int {
        // Security evaluation
        let orderByFields = query.sortDescriptors.map { $0.fieldName }
        try securityDelegate?.evaluateList(
            type: T.self,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )
        if query.fetchLimit == 0 {
            return 0
        }

        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // For count, we can optimize by not deserializing if no predicate
        if combinedPredicate == nil {
            let totalCount = try await countAllWithTransaction(
                T.self,
                transaction: transaction
            )
            return QueryResultWindow.resultCount(
                totalCount: totalCount,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            )
        }

        // Try to use index for counting
        if let predicate = combinedPredicate,
           let condition = try extractIndexableCondition(from: predicate),
           let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) {
            let totalCount = try await countUsingIndexWithTransaction(
                condition: condition,
                index: matchingIndex,
                transaction: transaction
            )
            return QueryResultWindow.resultCount(
                totalCount: totalCount,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            )
        }

        // Otherwise, fetch and count
        let results = try await fetchInternalWithTransaction(query, transaction: transaction)
        return results.count
    }

    /// Fetch a single model by ID within an existing transaction
    ///
    /// This method performs a direct key lookup (O(1)) rather than a query scan.
    /// Called by DatabaseContext.model(for:as:) which manages transaction and ReadVersionCache.
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    ///   - transaction: The transaction to use
    /// - Returns: The model if found, nil if not found
    /// - Throws: SecurityError if GET not allowed, or other errors on failure
    func fetchByIDInTransaction<T: Persistable>(
        _ type: T.Type,
        id: T.ID,
        transaction: any TransactionAccess
    ) async throws -> T? {
        let identifier = try PersistableIdentifierKeyCodec.tuple(for: id)
        return try await fetchByIdentifierTupleInTransaction(
            type,
            identifier: identifier,
            transaction: transaction
        )
    }

    /// Fetches an entity from an identifier tuple already produced by an index.
    ///
    /// Callers must validate the tuple against `T.persistableIdentifierType` before
    /// entering this storage-only path. Keeping the original tuple avoids an
    /// otherwise redundant logical-value-to-tuple conversion.
    func fetchByIdentifierTupleInTransaction<T: Persistable>(
        _ type: T.Type,
        identifier: Tuple,
        transaction: any TransactionAccess,
        snapshot: Bool = false
    ) async throws -> T? {
        let key = itemKey(for: T.persistableType, id: identifier)

        let storage = self.container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )
        guard let bytes = try await storage.read(for: key, snapshot: snapshot) else {
            return nil
        }

        // Deserialize using DataAccess
        let result: T = try DataAccess.deserialize(bytes)

        // Evaluate GET security via delegate after fetch
        try securityDelegate?.evaluateGet(result)

        return result
    }

    private func itemKey(
        for persistableType: String,
        id: Tuple
    ) -> Bytes {
        let keyPrefix: Bytes
        if persistableType == defaultPersistableType, let defaultPointReadPrefix {
            keyPrefix = defaultPointReadPrefix
        } else {
            let encodedType = persistableType.encodeTuple()
            var prefix = itemSubspace.prefix
            prefix.reserveCapacity(prefix.count + encodedType.count)
            prefix.append(contentsOf: encodedType)
            keyPrefix = prefix
        }
        return Subspace(prefix: keyPrefix).pack(id)
    }

    /// Count all models with an existing transaction
    private func countAllWithTransaction<T: Persistable>(
        _ type: T.Type,
        transaction: any TransactionAccess
    ) async throws -> Int {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()

        var count = 0
        let sequence = try await transaction.collectRange(
            from: KeySelector.firstGreaterOrEqual(begin),
            to: KeySelector.firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        for _ in sequence {
            count += 1
        }
        return count
    }

    /// Count using index scan with an existing transaction
    private func countUsingIndexWithTransaction(
        condition: IndexableCondition,
        index: IndexDescriptor,
        transaction: any TransactionAccess
    ) async throws -> Int {
        let indexSubspaceForIndex = indexSubspace.subspace(index.name)
        let valueTuple = condition.valueTuple

        // Build value subspace using flat encoding (see fetchUsingIndexWithTransaction comment)
        let valueSubspace = Subspace(prefix: indexSubspaceForIndex.prefix + valueTuple.pack())

        let beginKey: Bytes
        let endKey: Bytes

        switch condition.op {
        case .equal:
            (beginKey, endKey) = valueSubspace.range()

        case .greaterThan:
            beginKey = valueSubspace.range().1
            endKey = indexSubspaceForIndex.range().1

        case .greaterThanOrEqual:
            beginKey = indexSubspaceForIndex.pack(valueTuple)
            endKey = indexSubspaceForIndex.range().1

        case .lessThan:
            beginKey = indexSubspaceForIndex.range().0
            endKey = indexSubspaceForIndex.pack(valueTuple)

        case .lessThanOrEqual:
            beginKey = indexSubspaceForIndex.range().0
            endKey = valueSubspace.range().1

        default:
            return 0  // Not index-optimizable
        }

        var count = 0
        let sequence = try await transaction.collectRange(
            from: KeySelector.firstGreaterOrEqual(beginKey),
            to: KeySelector.firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        for _ in sequence {
            count += 1
        }
        return count
    }

    /// Count using index scan (without deserializing entities)
    private func countUsingIndex(condition: IndexableCondition, index: IndexDescriptor) async throws -> Int {
        let indexSubspaceForIndex = indexSubspace.subspace(index.name)
        let valueTuple = condition.valueTuple

        // Compute key range outside transaction to avoid capturing non-Sendable condition
        let beginKey: Bytes
        let endKey: Bytes

        switch condition.op {
        case .equal:
            let valueSubspace = indexSubspaceForIndex.subspace(valueTuple)
            (beginKey, endKey) = valueSubspace.range()

        case .greaterThan:
            let valueSubspace = indexSubspaceForIndex.subspace(valueTuple)
            beginKey = valueSubspace.range().1  // Start after value range
            endKey = indexSubspaceForIndex.range().1

        case .greaterThanOrEqual:
            beginKey = indexSubspaceForIndex.pack(valueTuple)
            endKey = indexSubspaceForIndex.range().1

        case .lessThan:
            beginKey = indexSubspaceForIndex.range().0
            endKey = indexSubspaceForIndex.pack(valueTuple)

        case .lessThanOrEqual:
            let valueSubspace = indexSubspaceForIndex.subspace(valueTuple)
            beginKey = indexSubspaceForIndex.range().0
            endKey = valueSubspace.range().1

        default:
            return 0  // Not index-optimizable
        }

        return try await container.engine.withTransaction(configuration: .default) { transaction in
            var count = 0
            // Use .wantAll for count operations - aggressive prefetch
            let sequence = try await transaction.collectRange(
                from: KeySelector.firstGreaterOrEqual(beginKey),
                to: KeySelector.firstGreaterOrEqual(endKey),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            for _ in sequence {
                count += 1
            }
            return count
        }
    }

    /// Count all models of a type
    private func countAll<T: Persistable>(_ type: T.Type) async throws -> Int {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()

        return try await container.engine.withTransaction(configuration: .default) { transaction in
            var count = 0
            // Use .wantAll for count operations - aggressive prefetch
            let sequence = try await transaction.collectRange(
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )

            for _ in sequence {
                count += 1
            }
            return count
        }
    }

    // MARK: - Approximate Count (O(1))

    /// Approximate count using FDB's getEstimatedRangeSizeBytes
    ///
    /// This is O(1) and much faster than full scan for large datasets.
    /// Accuracy depends on cluster statistics freshness.
    ///
    /// - Parameters:
    ///   - type: The persistable type to count
    ///   - avgRowSizeBytes: Estimated average row size (default: 500 bytes)
    /// - Returns: Estimated row count
    func approximateCount<T: Persistable>(
        _ type: T.Type,
        avgRowSizeBytes: Int = 500
    ) async throws -> Int {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()

        let sizeBytes = try await container.engine.withTransaction(configuration: .default) { transaction in
            try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )
        }

        let rowSize = max(1, avgRowSizeBytes)
        return max(0, sizeBytes / rowSize)
    }

    /// Approximate count for an index range
    ///
    /// - Parameters:
    ///   - index: The index descriptor
    ///   - avgEntrySizeBytes: Estimated average entry size (default: 50 bytes)
    /// - Returns: Estimated entry count
    func approximateIndexCount(
        index: IndexDescriptor,
        avgEntrySizeBytes: Int = 50
    ) async throws -> Int {
        let indexSubspaceForIndex = indexSubspace.subspace(index.name)
        let (begin, end) = indexSubspaceForIndex.range()

        let sizeBytes = try await container.engine.withTransaction(configuration: .default) { transaction in
            try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )
        }

        let entrySize = max(1, avgEntrySizeBytes)
        return max(0, sizeBytes / entrySize)
    }

    // MARK: - Save Operations

    /// Save models (insert or update)
    func save<T: Persistable>(_ models: [T]) async throws {
        guard !models.isEmpty else { return }

        let startTime = MonotonicClock.now()

        do {
            let mutations = models.map {
                PersistableMutation.save(
                    model: $0,
                    precondition: .none
                )
            }
            try await withTransaction(configuration: .default) { transaction in
                try await transaction.apply(mutations)
            }

            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didSave(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Saved \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailSave(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    // MARK: - Delete Operations

    /// Delete models
    func delete<T: Persistable>(_ models: [T]) async throws {
        guard !models.isEmpty else { return }

        let startTime = MonotonicClock.now()

        do {
            let mutations = models.map {
                PersistableMutation.delete(
                    model: $0,
                    precondition: .exists
                )
            }
            try await withTransaction(configuration: .default) { transaction in
                try await transaction.apply(mutations)
            }

            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didDelete(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Deleted \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailDelete(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Delete model by ID
    func delete<T: Persistable>(_ type: T.Type, id: T.ID) async throws {
        try await withTransaction(configuration: .default) { transaction in
            try await transaction.delete(type, identifiedBy: id)
        }
    }

    // MARK: - Batch Operations

    /// Execute a batch of saves and deletes in a single transaction
    package func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable]
    ) async throws {
        let startTime = MonotonicClock.now()

        do {
            var mutations: [PersistableMutation] = []
            mutations.reserveCapacity(inserts.count + deletes.count)
            for model in inserts {
                mutations.append(
                    .save(model: model, precondition: .none)
                )
            }
            for model in deletes {
                mutations.append(
                    .delete(model: model, precondition: .exists)
                )
            }
            let capturedMutations = mutations
            try await withTransaction(configuration: .default) { transaction in
                try await transaction.apply(capturedMutations)
            }

            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didExecuteBatch(insertCount: inserts.count, deleteCount: deletes.count, duration: duration)

            logger.trace("Executed batch", metadata: [
                "inserts": "\(inserts.count)",
                "deletes": "\(deletes.count)"
            ])
        } catch {
            let duration = MonotonicClock.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailBatch(error: error, duration: duration)
            throw error
        }
    }

    /// Saves one persisted model through the complete security and maintenance
    /// pipeline of the caller's logical transaction.
    func save(
        _ model: any Persistable,
        precondition: WritePrecondition,
        transaction: any TransactionAccess
    ) async throws -> PersistableWriteResult {
        let modelType = type(of: model)
        let persistableType = modelType.persistableType
        let idTuple = try model.persistableIdentifierTuple()

        let key = itemKey(for: persistableType, id: idTuple)

        // Use ItemStorage for large value handling (stores chunks in blobs subspace)
        let storage = self.container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )

        // Load the persisted value for security evaluation and index maintenance.
        var oldModel: (any Persistable)?
        let existingRowPresent: Bool

        if let oldData = try await storage.read(for: key) {
            let persistedModel = try DataAccess.deserializeAny(
                oldData,
                as: modelType
            )
            oldModel = persistedModel
            try securityDelegate?.evaluateUpdate(
                persistedModel,
                newResource: model
            )
            existingRowPresent = true
        } else {
            try securityDelegate?.evaluateCreate(model)
            existingRowPresent = false
        }

        // Validate intent before the first write so a mismatch cannot leave
        // partial primary or derived state.
        try Self.evaluateWritePrecondition(
            precondition,
            existingRowPresent: existingRowPresent,
            currentVersion: existingRowPresent ? oldModel.map(Self.entityVersionDigest) : nil,
            typeName: persistableType,
            idDescription: String(describing: model.id)
        )

        let data = try PersistableStorageCodec.encode(model)

        try await storage.write(data, for: key)

        // Update indexes via IndexMaintenanceService (efficient diff-based update)
        try await indexMaintenanceService.updateIndexesUntyped(
            oldModel: oldModel,
            newModel: model,
            id: idTuple,
            transaction: transaction
        )
        return PersistableWriteResult(
            model: model,
            previousModel: oldModel,
            encodedValue: data,
            identifier: idTuple
        )
    }

    /// Evaluate a `WritePrecondition` against an observed existence bit.
    ///
    /// Called after the existence probe (where required) but before any
    /// mutation. Violations throw `DatabaseContextError.preconditionFailed` so
    /// the enclosing transaction is aborted cleanly — no silent fallback.
    ///
    /// `.matchesStored` / `.matchesStoredOrAbsent` compare the caller's
    /// server-issued row digest against the current stored row before writing.
    private static func evaluateWritePrecondition(
        _ precondition: WritePrecondition,
        existingRowPresent: Bool,
        currentVersion: DatabaseBytes?,
        typeName: String,
        idDescription: String
    ) throws {
        switch precondition {
        case .none:
            return
        case .notExists:
            if existingRowPresent {
                throw DatabaseContextError.preconditionFailed(
                    typeName: typeName,
                    idDescription: idDescription,
                    precondition: precondition,
                    reason: "row already exists"
                )
            }
        case .exists:
            if !existingRowPresent {
                throw DatabaseContextError.preconditionFailed(
                    typeName: typeName,
                    idDescription: idDescription,
                    precondition: precondition,
                    reason: "row not found"
                )
            }
        case .matchesStored(let version):
            guard let currentVersion else {
                throw DatabaseContextError.preconditionFailed(
                    typeName: typeName,
                    idDescription: idDescription,
                    precondition: precondition,
                    reason: "row not found"
                )
            }
            if currentVersion != version {
                throw DatabaseContextError.preconditionFailed(
                    typeName: typeName,
                    idDescription: idDescription,
                    precondition: precondition,
                    reason: "entity version changed"
                )
            }
        case .matchesStoredOrAbsent(let version):
            guard let currentVersion else { return }
            if currentVersion != version {
                throw DatabaseContextError.preconditionFailed(
                    typeName: typeName,
                    idDescription: idDescription,
                    precondition: precondition,
                    reason: "entity version changed"
                )
            }
        }
    }

    private static func entityVersionDigest(
        for model: any Persistable
    ) throws -> DatabaseBytes {
        let fields = try PersistableFieldEncoder.encode(model)
        return try PersistableVersionTokenCodec.digest(fields: fields)
    }

    /// Deletes the currently persisted value and its physical indexes.
    /// A missing value produces no mutation.
    func delete(
        _ model: any Persistable,
        precondition: WritePrecondition,
        transaction: any TransactionAccess
    ) async throws -> (any Persistable)? {
        let persistableType = type(of: model).persistableType
        let idTuple = try model.persistableIdentifierTuple()
        let key = itemKey(for: persistableType, id: idTuple)
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )
        guard let existingData = try await storage.read(for: key) else {
            try Self.evaluateWritePrecondition(
                precondition,
                existingRowPresent: false,
                currentVersion: nil,
                typeName: persistableType,
                idDescription: String(describing: model.id)
            )
            return nil
        }
        let persistedModel = try DataAccess.deserializeAny(
            existingData,
            as: type(of: model)
        )
        try Self.evaluateWritePrecondition(
            precondition,
            existingRowPresent: true,
            currentVersion: try Self.entityVersionDigest(for: persistedModel),
            typeName: persistableType,
            idDescription: String(describing: model.id)
        )
        try securityDelegate?.evaluateDelete(persistedModel)
        try await indexMaintenanceService.updateIndexesUntyped(
            oldModel: persistedModel,
            newModel: nil,
            id: idTuple,
            transaction: transaction
        )
        try await storage.delete(for: key)
        return persistedModel
    }

    // MARK: - Predicate Evaluation

    /// Evaluate a predicate on a model
    private func evaluatePredicate<T: Persistable>(_ predicate: Predicate<T>, on model: T) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return comparison.evaluate(on: model)

        case .and(let predicates):
            return predicates.allSatisfy { evaluatePredicate($0, on: model) }

        case .or(let predicates):
            return predicates.contains { evaluatePredicate($0, on: model) }

        case .not(let predicate):
            return !evaluatePredicate(predicate, on: model)

        case .true:
            return true

        case .false:
            return false
        }
    }

    // evaluateFieldComparison and compareModels removed — unified into
    // FieldComparison.evaluate(on:) and SortDescriptor.orderedComparison()

    // MARK: - Transaction Operations

    /// Execute operations within a transaction
    ///
    /// **Note**: This uses the database directly without ReadVersionCache.
    /// For application operations that benefit from caching, use DatabaseContext.withTransaction().
    package func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> T
    ) async throws -> T {
        return try await container.engine.withTransaction(configuration: configuration) { transaction in
            let databaseTransaction = DatabaseTransaction(
                storageAccess: transaction,
                container: self.container
            )
            do {
                let result = try await operation(databaseTransaction)
                try await databaseTransaction.prepareForCommit()
                return result
            } catch {
                await databaseTransaction.invalidate()
                throw error
            }
        }
    }
}

// MARK: - DatabaseIndexError

/// Errors that can occur during index operations
public enum DatabaseIndexError: Error, CustomStringConvertible {
    /// Unique constraint violation: duplicate value exists for another entity
    case uniqueConstraintViolation(indexName: String, values: [String])

    /// Index not found in schema
    case indexNotFound(indexName: String)

    /// Unsupported index kind for operation
    case unsupportedIndexKind(indexName: String, kindIdentifier: String)

    public var description: String {
        switch self {
        case .uniqueConstraintViolation(let indexName, let values):
            return "Unique constraint violation on index '\(indexName)': values [\(values.joined(separator: ", "))] already exist for another entity"
        case .indexNotFound(let indexName):
            return "Index '\(indexName)' not found in schema"
        case .unsupportedIndexKind(let indexName, let kindIdentifier):
            return "Unsupported index kind '\(kindIdentifier)' for index '\(indexName)'"
        }
    }
}
