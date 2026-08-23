import DatabaseKit
import DatabaseTypes
import StorageKit

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
    let entity: Schema.Entity
    private let indexDescriptors: [IndexDescriptor]
    private let logger: DatabaseLogger

    /// Delegate for operation callbacks (metrics, etc.)
    private let metricsDelegate: DataStoreDelegate

    /// Items subspace: [subspace]/items/
    let itemSubspace: Subspace

    /// Blobs subspace: [subspace]/blobs/ - for large value chunks
    let blobsSubspace: Subspace

    /// Metadata subspace: [subspace]/_metadata/
    let metadataSubspace: Subspace

    /// Precomputed point-read prefix for the store's entity.
    ///
    /// `DBContainer` creates and caches stores by `(type, path)`, so the default type
    /// is stable for hot point-read paths. Keeping the fully encoded prefix avoids
    /// repeated tuple encoding of the type name on every read.
    private let defaultPointReadPrefix: ByteString

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
        entity: Schema.Entity,
        metricsDelegate: DataStoreDelegate? = nil,
        securityDelegate: (any DataStoreSecurityDelegate)? = nil,
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) {
        self.container = container
        self.subspace = subspace
        self.entity = entity
        self.indexDescriptors = entity.indexDescriptors
        self.logger = container.configuration.logging.logger(
            label: "com.database.framework.data-store"
        )
        self.metricsDelegate = metricsDelegate ?? container.dataStoreDelegate
        self.securityDelegate = securityDelegate
        self.itemSubspace = subspace.subspace(SubspaceKey.items)
        self.blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        self.metadataSubspace = subspace.subspace(SubspaceKey.metadata)
        let encodedType = Tuple([entity.name]).pack()
        self.defaultPointReadPrefix = self.itemSubspace.prefix.appending(
            contentsOf: encodedType
        )
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
            configurations: indexConfigurations
        )
    }

    // MARK: - Fetch Operations
    //
    // **Design Intent - No ReadVersionCache**:
    // Fetch operations use `container.transactionExecutor.withTransaction()` directly without
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
            entity: T.persistableType,
            limit: nil,
            offset: nil,
            orderBy: nil
        )

        let results = try await fetchAllInternal(type)
        try evaluateReadResults(results)
        return results
    }

    /// Internal fetchAll without security evaluation (for internal use after security is already evaluated)
    private func fetchAllInternal<T: Persistable>(_ type: T.Type) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        let startTime = container.monotonicClock.now

        do {
            let results: [T] = try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
                // Use ItemStorage for proper handling of large values
                let storage = self.container.itemStorageFactory.make(
                    transaction: transaction,
                    blobsSubspace: self.blobsSubspace
                )
                var results: [T] = []

                // ItemStorage.scan handles both inline and external (split) values transparently
                var iterator = storage.scan(
                    begin: begin,
                    end: end,
                    snapshot: true
                ).makeAsyncIterator()
                while let (_, data) = try await iterator.next() {
                    let model: T = try DataAccess.deserialize(data)
                    results.append(model)
                }
                return results
            }

            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didFailFetch(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Fetch a single model by ID
    package func fetch<T: Persistable>(_ type: T.Type, id: T.ID) async throws -> T? {
        let result: T? = try await container.transactionExecutor.withTransaction { [self] transaction in
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
            entity: T.persistableType,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )

        let results = try await fetchInternal(query)
        try evaluateReadResults(results)
        return results
    }

    // MARK: - Index-Optimized Fetch

    /// Result from index-based fetch
    private struct IndexFetchResult<T: Persistable> {
        let models: [T]
        let needsPostFiltering: Bool
    }

    /// Resolve the physical access path using the same selection, encoding, and
    /// lifecycle checks as query execution.
    func executionPlan<T: Persistable>(
        for query: Query<T>,
        transaction: any TransactionAccess
    ) async throws -> QueryAccessPlan {
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )

        let predicate: Predicate<T>?
        switch query.predicates.count {
        case 0:
            predicate = nil
        case 1:
            predicate = query.predicates[0]
        default:
            predicate = .and(query.predicates)
        }

        guard let predicate else {
            if query.forcedIndex != nil {
                throw CanonicalReadError.indexHintNotApplicable(
                    "A forced index requires an indexable filter predicate"
                )
            }
            return QueryAccessPlan(
                accessPath: .fullScan,
                indexedConditions: [],
                residualFilterRequired: false,
                sortRequired: !query.sortDescriptors.isEmpty,
                fetchLimit: query.fetchLimit,
                fetchOffset: query.fetchOffset
            )
        }

        guard let selection = try ScalarIndexAccessPlanner.select(
            for: predicate,
            descriptors: indexDescriptors,
            forcedIndexName: query.forcedIndex?.indexName
        ) else {
            if let forcedIndex = query.forcedIndex {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(forcedIndex.indexName)' cannot serve the given predicate on type '\(T.persistableType)'"
                )
            }
            return QueryAccessPlan(
                accessPath: .fullScan,
                indexedConditions: [],
                residualFilterRequired: true,
                sortRequired: !query.sortDescriptors.isEmpty,
                fetchLimit: query.fetchLimit,
                fetchOffset: query.fetchOffset
            )
        }

        // Execution must be able to encode the selected values before this can
        // be reported as a viable physical access path.
        _ = try encodeScalarIndexAccess(selection)

        let state = try await indexLifecycleStore.state(
            of: selection.descriptor.name,
            transaction: transaction
        )
        guard state.isReadable else {
            if query.forcedIndex != nil {
                throw CanonicalReadError.indexHintNotReadable(
                    indexName: selection.descriptor.name,
                    state: state.description
                )
            }
            return QueryAccessPlan(
                accessPath: .fullScan,
                indexedConditions: [],
                residualFilterRequired: true,
                sortRequired: !query.sortDescriptors.isEmpty,
                fetchLimit: query.fetchLimit,
                fetchOffset: query.fetchOffset
            )
        }

        return QueryAccessPlan(
            accessPath: .orderedIndex(
                name: selection.descriptor.name,
                indexType: selection.descriptor.type,
                indexedFields: selection.descriptor.fieldNames
            ),
            indexedConditions: selection.clauses.map { clause in
                QueryAccessCondition(
                    fieldName: clause.fieldName,
                    comparison: clause.comparison,
                    value: clause.value
                )
            },
            residualFilterRequired: selection.requiresPostFilter,
            sortRequired: !query.sortDescriptors.isEmpty,
            fetchLimit: query.fetchLimit,
            fetchOffset: query.fetchOffset
        )
    }

    /// Attempt to fetch using an index
    ///
    /// Returns nil if no suitable index is available for the predicate,
    /// or if the index is not in readable state.
    private func fetchUsingIndex<T: Persistable>(
        _ predicate: Predicate<T>,
        type: T.Type,
        limit: Int?,
        forcedIndexName: String? = nil
    ) async throws -> IndexFetchResult<T>? {
        guard let selection = try ScalarIndexAccessPlanner.select(
            for: predicate,
            descriptors: indexDescriptors,
            forcedIndexName: forcedIndexName
        ) else {
            if let forcedIndexName {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(forcedIndexName)' cannot serve the given predicate on type '\(T.persistableType)'"
                )
            }
            return nil
        }
        let accessPath = try encodeScalarIndexAccess(selection)
        let condition = accessPath.condition
        let matchingIndex = accessPath.descriptor

        // Check index state - only use readable indexes for queries
        let indexState = try await indexLifecycleStore.state(of: matchingIndex.name)
        guard indexState.isReadable else {
            if forcedIndexName != nil {
                throw CanonicalReadError.indexHintNotReadable(
                    indexName: matchingIndex.name,
                    state: indexState.description
                )
            }
            logger.debug("Index '\(matchingIndex.name)' is not readable (state: \(indexState.description)), falling back to scan")
            return nil
        }

        // Build index scan range based on condition OUTSIDE transaction
        let indexSubspaceForIndex = try indexLifecycleStore.indexSubspace(
            for: matchingIndex.name)
        let valueTuple = condition.valueTuple
        let indexedFieldCount = matchingIndex.fieldNames.count

        // Build value subspace using flat encoding (prefix + tuple.pack())
        // NOTE: Do NOT use indexSubspaceForIndex.subspace(valueTuple) because
        // Tuple conforms to TupleElement, which would create a NESTED tuple
        // encoding (type code 0x05) that doesn't match the flat key structure
        // written by ScalarIndexMaintainer.
        let valueSubspace = Subspace(
            prefix: indexSubspaceForIndex.prefix.appending(
                contentsOf: valueTuple.pack()
            )
        )

        // Compute key range outside transaction to avoid capturing non-Sendable condition
        let scanRange: IndexScanRange
        switch condition.op {
        case .equal:
            let (begin, end) = valueSubspace.range()
            if condition.matchedFieldCount == indexedFieldCount {
                scanRange = .exactMatch(
                    begin: begin,
                    end: end,
                    valueSubspace: valueSubspace
                )
            } else {
                scanRange = .range(
                    begin: begin,
                    end: end,
                    baseSubspace: indexSubspaceForIndex,
                    keyPathsCount: indexedFieldCount
                )
            }

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
            throw CanonicalReadError.unsupportedAccessPath(
                "Scalar index '\(matchingIndex.name)' cannot execute comparison operator '\(condition.op)'"
            )
        }

        // Execute scan in transaction - all captured values are now Sendable
        // Select optimal StreamingMode based on limit
        let streamingMode: StreamingMode = StreamingMode.forQuery(limit: limit)
        let scannedIndexName = matchingIndex.name

        let ids: [Tuple] = try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
            var ids: [Tuple] = []
            if let limit = limit {
                ids.reserveCapacity(limit)
            }

            switch scanRange {
            case .exactMatch(let begin, let end, let valueSubspace):
                // Apply limit pushdown to reduce server-side work
                let sequence = try await TransactionRangeCollection.collect(using: transaction,
                    from: KeySelector.firstGreaterOrEqual(begin),
                    to: KeySelector.firstGreaterOrEqual(end),
                    limit: limit ?? 0,  // 0 = unlimited in FDB
                    reverse: false,
                    snapshot: true,
                    streamingMode: streamingMode
                )
                for (key, _) in sequence {
                    ids.append(try self.extractIDFromIndexKey(
                        key,
                        subspace: valueSubspace,
                        indexName: scannedIndexName
                    ))
                }

            case .range(let begin, let end, let baseSubspace, let keyPathsCount):
                // Apply limit pushdown to reduce server-side work
                let sequence = try await TransactionRangeCollection.collect(using: transaction,
                    from: KeySelector.firstGreaterOrEqual(begin),
                    to: KeySelector.firstGreaterOrEqual(end),
                    limit: limit ?? 0,  // 0 = unlimited in FDB
                    reverse: false,
                    snapshot: true,
                    streamingMode: streamingMode
                )
                for (key, _) in sequence {
                    ids.append(try self.extractIDFromIndexKey(
                        key,
                        baseSubspace: baseSubspace,
                        keyPathsCount: keyPathsCount,
                        indexName: scannedIndexName
                    ))
                }
            }

            return ids
        }

        // If no IDs found, return empty result
        if ids.isEmpty {
            return IndexFetchResult(models: [], needsPostFiltering: false)
        }

        // Fetch models by IDs
        let models = try await fetchByIds(T.self, ids: ids)
        try evaluateReadResults(models)

        // Determine if post-filtering is needed
        // (needed if predicate has additional conditions beyond the indexed field)
        let needsPostFiltering = selection.requiresPostFilter

        return IndexFetchResult(models: models, needsPostFiltering: needsPostFiltering)
    }

    /// Represents a pre-computed index scan range (Sendable)
    private enum IndexScanRange: Sendable {
        case exactMatch(begin: ByteString, end: ByteString, valueSubspace: Subspace)
        /// Range scan with keyPathsCount to know how many elements are index values vs ID
        case range(begin: ByteString, end: ByteString, baseSubspace: Subspace, keyPathsCount: Int)
    }

    /// A scalar comparison encoded for an index-key prefix.
    private struct ScalarIndexCondition: Sendable {
        let fieldName: String
        let op: ComparisonOperator
        let valueTuple: Tuple
        let matchedFieldCount: Int
    }

    /// The selected scalar index and its encoded lookup condition.
    ///
    /// Selection returns these values together so a compound-index decision
    /// cannot be accidentally rebound to another descriptor with the same
    /// leading field during execution.
    private struct ScalarIndexAccessPath: Sendable {
        let descriptor: IndexDescriptor
        let condition: ScalarIndexCondition
    }

    private func encodeScalarIndexAccess(
        _ selection: ScalarIndexAccessSelection
    ) throws -> ScalarIndexAccessPath {
        var elements: [any TupleElement] = []
        elements.reserveCapacity(selection.clauses.count)

        for clause in selection.clauses {
            let element: any TupleElement
            do {
                element = try clause.value.toTupleElement()
            } catch {
                throw CanonicalReadError.unencodablePredicateValue(
                    field: clause.fieldName,
                    valueDescription: TupleElementSemanticName.describe(
                        clause.value
                    )
                )
            }
            elements.append(element)
        }

        guard let firstClause = selection.clauses.first else {
            throw CanonicalReadError.unsupportedAccessPath(
                "Scalar index selection requires at least one predicate clause"
            )
        }
        let comparison: ComparisonOperator = selection.clauses.count > 1
            ? .equal
            : firstClause.comparison
        return ScalarIndexAccessPath(
            descriptor: selection.descriptor,
            condition: ScalarIndexCondition(
                fieldName: firstClause.fieldName,
                op: comparison,
                valueTuple: Tuple(elements),
                matchedFieldCount: selection.clauses.count
            )
        )
    }

    /// Hexadecimal rendering for error payloads. `String(describing:)` is
    /// unavailable in Embedded Swift, so key bytes are formatted manually.
    private static func hexadecimalString(_ bytes: ByteString) -> String {
        var characters = [UInt8]()
        characters.reserveCapacity(bytes.count * 2)
        for byte in bytes {
            let high = byte >> 4
            let low = byte & 0x0f
            characters.append(high < 10 ? 48 + high : 87 + high)
            characters.append(low < 10 ? 48 + low : 87 + low)
        }
        return String(decoding: characters, as: UTF8.self)
    }

    /// Extract ID from an index key given a value subspace
    ///
    /// A key that unpacks to an empty tuple carries no primary-key elements.
    /// Such an entry is physically corrupt; skipping it would silently shrink
    /// query results, so the read fails instead.
    private func extractIDFromIndexKey(
        _ key: ByteString,
        subspace: Subspace,
        indexName: String
    ) throws -> Tuple {
        let tuple = try subspace.unpack(key)
        guard tuple.count > 0 else {
            throw CanonicalReadError.corruptedIndexEntry(
                indexName: indexName,
                reason: "index key unpacked to an empty tuple"
            )
        }
        return tuple
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
        _ key: ByteString,
        baseSubspace: Subspace,
        keyPathsCount: Int,
        indexName: String
    ) throws -> Tuple {
        let tuple = try baseSubspace.unpack(key)
        guard tuple.count > keyPathsCount else {
            throw CanonicalReadError.corruptedIndexEntry(
                indexName: indexName,
                reason: "index key holds \(tuple.count) elements but needs more than \(keyPathsCount) to carry a primary key"
            )
        }

        var idElements: [any TupleElement] = []
        idElements.reserveCapacity(tuple.count - keyPathsCount)
        for index in keyPathsCount..<tuple.count {
            if let element = tuple[index] {
                idElements.append(element)
            }
        }
        guard !idElements.isEmpty else {
            throw CanonicalReadError.corruptedIndexEntry(
                indexName: indexName,
                reason: "index key primary-key elements decoded as null"
            )
        }
        return Tuple(idElements)
    }

    /// Fetch models by IDs (parallel reads for 10-30× speedup)
    private func fetchByIds<T: Persistable>(_ type: T.Type, ids: [Tuple]) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)

        // Pre-compute keys outside transaction
        let keys = ids.map { typeSubspace.pack($0) }

        return try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )

            // Keep point-read concurrency bounded so candidate cardinality does
            // not become Task cardinality.
            return try await withThrowingTaskGroup(of: (Int, T?).self) { group in
                let maximumConcurrentReads = 32
                var nextIndex = 0
                while nextIndex < min(maximumConcurrentReads, keys.count) {
                    let index = nextIndex
                    let key = keys[index]
                    group.addTask {
                        if let bytes = try await storage.read(for: key) {
                            let model: T = try DataAccess.deserialize(bytes)
                            return (index, model)
                        }
                        return (index, nil)
                    }
                    nextIndex += 1
                }
                var ordered = [T?](repeating: nil, count: keys.count)
                while let result = try await group.next() {
                    ordered[result.0] = result.1
                    if nextIndex < keys.count {
                        let index = nextIndex
                        let key = keys[index]
                        group.addTask {
                            if let bytes = try await storage.read(for: key) {
                                let model: T = try DataAccess.deserialize(bytes)
                                return (index, model)
                            }
                            return (index, nil)
                        }
                        nextIndex += 1
                    }
                }

                // Missing rows are dropped, not raised: this fetch runs in a
                // second transaction after the index scan, so an absent row is
                // indistinguishable from a legitimate concurrent delete.
                // Same-transaction reads go through fetchByIdsWithTransaction,
                // which treats a missing row as a dangling index entry.
                return ordered.compactMap { $0 }
            }
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
            entity: T.persistableType,
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
            if query.forcedIndex != nil {
                throw CanonicalReadError.indexHintNotApplicable(
                    "A forced index requires an indexable filter predicate"
                )
            }
            let totalCount = try await countAll(T.self)
            return QueryResultWindow.resultCount(
                totalCount: totalCount,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            )
        }

        // Try to use index for counting
        if let predicate = combinedPredicate {
            if let selection = try ScalarIndexAccessPlanner.select(
                for: predicate,
                descriptors: indexDescriptors,
                forcedIndexName: query.forcedIndex?.indexName
            ) {
                let accessPath = try encodeScalarIndexAccess(selection)
                let state = try await indexLifecycleStore.state(
                    of: accessPath.descriptor.name
                )
                if state.isReadable {
                    if !selection.requiresPostFilter {
                        let totalCount = try await countUsingIndex(accessPath)
                        return QueryResultWindow.resultCount(
                            totalCount: totalCount,
                            limit: query.fetchLimit,
                            offset: query.fetchOffset
                        )
                    }
                } else if query.forcedIndex != nil {
                    throw CanonicalReadError.indexHintNotReadable(
                        indexName: accessPath.descriptor.name,
                        state: state.description
                    )
                }
            } else if let forcedIndex = query.forcedIndex {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(forcedIndex.indexName)' cannot serve the given predicate on type '\(T.persistableType)'"
                )
            }
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
           let indexResult = try await fetchUsingIndex(
               predicate,
               type: T.self,
               limit: nil,
               forcedIndexName: query.forcedIndex?.indexName
           ) {
            results = indexResult.models

            // If index didn't cover all predicate conditions, apply remaining filters
            if indexResult.needsPostFiltering {
                results = try results.filter { model in
                    try evaluatePredicate(predicate, on: model)
                }
            }
        } else {
            if query.forcedIndex != nil {
                throw CanonicalReadError.indexHintNotApplicable(
                    "A forced index requires an indexable filter predicate"
                )
            }
            // Fall back to full table scan
            results = try await fetchAllInternal(T.self)

            // Apply predicate filter
            if let predicate = combinedPredicate {
                results = try results.filter { model in
                    try evaluatePredicate(predicate, on: model)
                }
            }
        }

        // Apply sorting
        if !query.sortDescriptors.isEmpty {
            try results.sort { lhs, rhs in
                for sortDescriptor in query.sortDescriptors {
                    let result = try sortDescriptor.orderedComparison(lhs, rhs)
                    if result != .equal {
                        return result == .lessThan
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
            entity: T.persistableType,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )

        let results = try await fetchInternalWithTransaction(query, transaction: transaction)
        try evaluateReadResults(results)
        return results
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
                    return try evaluatePredicate(predicate, on: model)
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
                workMeter: query.executionWorkMeter,
                limit: query.executionWindowIsPushed ? query.fetchLimit : nil,
                offset: query.executionWindowIsPushed
                    ? query.executionStorageOffset
                    : 0,
                startingAfterIdentifier:
                    query.executionStartAfterIdentifier
            )

            // Apply predicate filter
            if let predicate = combinedPredicate {
                results = try results.filter { model in
                    try query.executionWorkMeter?.consume(
                        at: .filterEvaluation
                    )
                    return try evaluatePredicate(predicate, on: model)
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
                    let result = try sortDescriptor.orderedComparison(lhs, rhs)
                    if result != .equal {
                        return result == .lessThan
                    }
                }
                return false
            }
        }

        if !query.executionWindowIsPushed {
            QueryResultWindow.apply(
                to: &results,
                limit: query.fetchLimit,
                offset: query.fetchOffset
            )
        }

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
        workMeter: DatabaseWorkMeter?,
        limit: Int? = nil,
        offset: Int = 0,
        startingAfterIdentifier: ByteString? = nil
    ) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        let startTime = container.monotonicClock.now

        do {
            let storage = self.container.itemStorageFactory.make(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )
            var results: [T] = []
            if let limit {
                results.reserveCapacity(limit)
            }
            let startingAfter = try startingAfterIdentifier.map {
                typeSubspace.pack(try Tuple(packed: $0))
            }
            let scanLimit: Int
            if let limit {
                let (value, overflow) = limit.addingReportingOverflow(offset)
                scanLimit = overflow ? Int.max : value
            } else {
                scanLimit = 0
            }
            var skipped = 0

            // ItemStorage.scan handles both inline and external (split) values transparently
            var iterator = storage.scan(
                begin: begin,
                end: end,
                startingAfter: startingAfter,
                snapshot: true,
                limit: scanLimit
            ).makeAsyncIterator()
            while let (_, data) = try await iterator.next() {
                try workMeter?.consume(at: .storageRow)
                if skipped < offset {
                    skipped += 1
                    continue
                }
                let model: T = try DataAccess.deserialize(data)
                results.append(model)
            }

            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
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
        guard let selection = try ScalarIndexAccessPlanner.select(
            for: predicate,
            descriptors: indexDescriptors,
            forcedIndexName: forcedIndexName
        ) else {
            if let forcedIndexName {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(forcedIndexName)' cannot serve the given predicate on type '\(T.persistableType)'"
                )
            }
            return nil
        }
        let accessPath = try encodeScalarIndexAccess(selection)
        let condition = accessPath.condition
        let matchingIndex = accessPath.descriptor
        let needsPostFiltering = selection.requiresPostFilter
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
            if forcedIndexName != nil {
                throw CanonicalReadError.indexHintNotReadable(
                    indexName: matchingIndex.name,
                    state: indexState.description
                )
            }
            logger.debug("Index '\(matchingIndex.name)' is not readable (state: \(indexState)), falling back to scan")
            return nil
        }

        // Build index scan range based on condition
        let indexSubspaceForIndex = try indexLifecycleStore.indexSubspace(
            for: matchingIndex.name)
        let valueTuple = condition.valueTuple
        let indexedFieldCount = matchingIndex.fieldNames.count

        // Build value subspace using flat encoding (prefix + tuple.pack())
        // NOTE: Do NOT use indexSubspaceForIndex.subspace(valueTuple) because
        // Tuple conforms to TupleElement, which would create a NESTED tuple
        // encoding (type code 0x05) that doesn't match the flat key structure
        // written by ScalarIndexMaintainer.
        let valueSubspace = Subspace(
            prefix: indexSubspaceForIndex.prefix.appending(
                contentsOf: valueTuple.pack()
            )
        )

        // Compute key range
        let scanRange: IndexScanRange
        switch condition.op {
        case .equal:
            let (begin, end) = valueSubspace.range()
            if condition.matchedFieldCount == indexedFieldCount {
                scanRange = .exactMatch(
                    begin: begin,
                    end: end,
                    valueSubspace: valueSubspace
                )
            } else {
                scanRange = .range(
                    begin: begin,
                    end: end,
                    baseSubspace: indexSubspaceForIndex,
                    keyPathsCount: indexedFieldCount
                )
            }

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
            throw CanonicalReadError.unsupportedAccessPath(
                "Scalar index '\(matchingIndex.name)' cannot execute comparison operator '\(condition.op)'"
            )
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
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: storageLimit ?? 0,
                reverse: false,
                snapshot: true,
                streamingMode: streamingMode
            )
            for (key, _) in sequence {
                try workMeter?.consume(at: .storageRow)
                ids.append(try self.extractIDFromIndexKey(
                    key,
                    subspace: valueSubspace,
                    indexName: matchingIndex.name
                ))
            }

        case .range(let begin, let end, let baseSubspace, let keyPathsCount):
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: storageLimit ?? 0,
                reverse: false,
                snapshot: true,
                streamingMode: streamingMode
            )
            for (key, _) in sequence {
                try workMeter?.consume(at: .storageRow)
                ids.append(try self.extractIDFromIndexKey(
                    key,
                    baseSubspace: baseSubspace,
                    keyPathsCount: keyPathsCount,
                    indexName: matchingIndex.name
                ))
            }
        }

        // If no IDs found, return empty result
        if ids.isEmpty {
            return IndexFetchResult(models: [], needsPostFiltering: false)
        }

        // Fetch models by IDs with provided transaction
        let models = try await fetchByIdsWithTransaction(
            T.self,
            ids: ids,
            indexName: matchingIndex.name,
            transaction: transaction,
            workMeter: workMeter
        )
        try evaluateReadResults(models)

        return IndexFetchResult(models: models, needsPostFiltering: needsPostFiltering)
    }

    /// Fetch models by IDs with an existing transaction (parallel reads)
    ///
    /// The index scan that produced `ids` and these row reads share one
    /// transaction snapshot, so a missing row is a real index/row divergence,
    /// not a racing delete, and surfaces as `danglingIndexEntry`.
    private func fetchByIdsWithTransaction<T: Persistable>(
        _ type: T.Type,
        ids: [Tuple],
        indexName: String,
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

        // Keep point-read concurrency bounded so candidate cardinality does
        // not become Task cardinality.
        return try await withThrowingTaskGroup(of: (Int, T).self) { group in
            let maximumConcurrentReads = 32
            var nextIndex = 0
            while nextIndex < min(maximumConcurrentReads, keys.count) {
                let index = nextIndex
                let key = keys[index]
                let primaryKey = ids[index]
                group.addTask {
                    guard let bytes = try await storage.read(for: key) else {
                        throw CanonicalReadError.danglingIndexEntry(
                            indexName: indexName,
                            primaryKey: Self.hexadecimalString(primaryKey.pack())
                        )
                    }
                    let model: T = try DataAccess.deserialize(bytes)
                    return (index, model)
                }
                nextIndex += 1
            }
            var ordered = [T?](repeating: nil, count: keys.count)
            while let result = try await group.next() {
                ordered[result.0] = result.1
                if nextIndex < keys.count {
                    let index = nextIndex
                    let key = keys[index]
                    let primaryKey = ids[index]
                    group.addTask {
                        guard let bytes = try await storage.read(for: key) else {
                            throw CanonicalReadError.danglingIndexEntry(
                                indexName: indexName,
                                primaryKey: Self.hexadecimalString(primaryKey.pack())
                            )
                        }
                        let model: T = try DataAccess.deserialize(bytes)
                        return (index, model)
                    }
                    nextIndex += 1
                }
            }
            return ordered.map { value in
                guard let value else {
                    preconditionFailure(
                        "Every strict point read must complete with a value"
                    )
                }
                return value
            }
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
            entity: T.persistableType,
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
            if query.forcedIndex != nil {
                throw CanonicalReadError.indexHintNotApplicable(
                    "A forced index requires an indexable filter predicate"
                )
            }
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
        if let predicate = combinedPredicate {
            if let selection = try ScalarIndexAccessPlanner.select(
                for: predicate,
                descriptors: indexDescriptors,
                forcedIndexName: query.forcedIndex?.indexName
            ) {
                let accessPath = try encodeScalarIndexAccess(selection)
                let state = try await indexLifecycleStore.state(
                    of: accessPath.descriptor.name,
                    transaction: transaction
                )
                if state.isReadable {
                    if !selection.requiresPostFilter {
                        let totalCount = try await countUsingIndexWithTransaction(
                            accessPath,
                            transaction: transaction
                        )
                        return QueryResultWindow.resultCount(
                            totalCount: totalCount,
                            limit: query.fetchLimit,
                            offset: query.fetchOffset
                        )
                    }
                } else if query.forcedIndex != nil {
                    throw CanonicalReadError.indexHintNotReadable(
                        indexName: accessPath.descriptor.name,
                        state: state.description
                    )
                }
            } else if let forcedIndex = query.forcedIndex {
                throw CanonicalReadError.indexHintNotApplicable(
                    "Forced index '\(forcedIndex.indexName)' cannot serve the given predicate on type '\(T.persistableType)'"
                )
            }
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

        let persistedModel = try DataAccess.deserializePersistedModel(
            bytes,
            expectedEntity: T.persistableType
        )
        let result = try persistedModel.decode(as: T.self)

        // Evaluate GET security via delegate after fetch
        try securityDelegate?.evaluateGet(persistedModel, fields: nil)

        return result
    }

    private func itemKey(
        for persistableType: String,
        id: Tuple
    ) -> ByteString {
        let keyPrefix: ByteString
        if persistableType == entity.name {
            keyPrefix = defaultPointReadPrefix
        } else {
            let encodedType = Tuple([persistableType]).pack()
            keyPrefix = itemSubspace.prefix.appending(contentsOf: encodedType)
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
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
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
        _ accessPath: ScalarIndexAccessPath,
        transaction: any TransactionAccess
    ) async throws -> Int {
        let condition = accessPath.condition
        let index = accessPath.descriptor
        let indexSubspaceForIndex = try indexLifecycleStore.indexSubspace(
            for: index.name)
        let valueTuple = condition.valueTuple

        // Build value subspace using flat encoding (see fetchUsingIndexWithTransaction comment)
        let valueSubspace = Subspace(
            prefix: indexSubspaceForIndex.prefix.appending(
                contentsOf: valueTuple.pack()
            )
        )

        let beginKey: ByteString
        let endKey: ByteString

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
            throw CanonicalReadError.unsupportedAccessPath(
                "Scalar index '\(index.name)' cannot count comparison operator '\(condition.op)'"
            )
        }

        var count = 0
        let sequence = try await TransactionRangeCollection.collect(using: transaction,
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
    private func countUsingIndex(
        _ accessPath: ScalarIndexAccessPath
    ) async throws -> Int {
        let condition = accessPath.condition
        let index = accessPath.descriptor
        let indexSubspaceForIndex = try indexLifecycleStore.indexSubspace(
            for: index.name)
        let valueTuple = condition.valueTuple

        // Compute key range outside transaction to avoid capturing non-Sendable condition
        let beginKey: ByteString
        let endKey: ByteString

        switch condition.op {
        case .equal:
            let valueSubspace = Subspace(
                prefix: indexSubspaceForIndex.prefix.appending(
                    contentsOf: valueTuple.pack()
                )
            )
            (beginKey, endKey) = valueSubspace.range()

        case .greaterThan:
            let valueSubspace = Subspace(
                prefix: indexSubspaceForIndex.prefix.appending(
                    contentsOf: valueTuple.pack()
                )
            )
            beginKey = valueSubspace.range().1  // Start after value range
            endKey = indexSubspaceForIndex.range().1

        case .greaterThanOrEqual:
            beginKey = indexSubspaceForIndex.pack(valueTuple)
            endKey = indexSubspaceForIndex.range().1

        case .lessThan:
            beginKey = indexSubspaceForIndex.range().0
            endKey = indexSubspaceForIndex.pack(valueTuple)

        case .lessThanOrEqual:
            let valueSubspace = Subspace(
                prefix: indexSubspaceForIndex.prefix.appending(
                    contentsOf: valueTuple.pack()
                )
            )
            beginKey = indexSubspaceForIndex.range().0
            endKey = valueSubspace.range().1

        default:
            throw CanonicalReadError.unsupportedAccessPath(
                "Scalar index '\(index.name)' cannot count comparison operator '\(condition.op)'"
            )
        }

        return try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
            var count = 0
            // Use .wantAll for count operations - aggressive prefetch
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
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

        return try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
            var count = 0
            // Use .wantAll for count operations - aggressive prefetch
            let sequence = try await TransactionRangeCollection.collect(using: transaction,
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

        let sizeBytes = try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
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
        let indexSubspaceForIndex = try indexLifecycleStore.indexSubspace(
            for: index.name)
        let (begin, end) = indexSubspaceForIndex.range()

        let sizeBytes = try await container.transactionExecutor.withTransaction(configuration: .default, clock: container.monotonicClock) { transaction in
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

        let startTime = container.monotonicClock.now

        do {
            let mutations = try models.map {
                PersistableMutation.save(
                    identity: try EntityReferenceEncoder.encode($0),
                    model: try PersistedModel($0),
                    precondition: .none
                )
            }
            try await withTransaction(configuration: .default) { transaction in
                try await transaction.apply(mutations)
            }

            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didSave(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Saved \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didFailSave(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    // MARK: - Delete Operations

    /// Delete models
    func delete<T: Persistable>(_ models: [T]) async throws {
        guard !models.isEmpty else { return }

        let startTime = container.monotonicClock.now

        do {
            let mutations = try models.map {
                PersistableMutation.delete(
                    identity: try EntityReferenceEncoder.encode($0),
                    model: try PersistedModel($0),
                    precondition: .exists
                )
            }
            try await withTransaction(configuration: .default) { transaction in
                try await transaction.apply(mutations)
            }

            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didDelete(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Deleted \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
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
        inserts: [PersistedModel],
        deletes: [PersistedModel]
    ) async throws {
        let startTime = container.monotonicClock.now

        do {
            var mutations: [PersistableMutation] = []
            mutations.reserveCapacity(inserts.count + deletes.count)
            for model in inserts {
                let runtime = try runtime(for: model.entity)
                mutations.append(
                    .save(
                        identity: try runtime.identity(for: model),
                        model: model,
                        precondition: .none
                    )
                )
            }
            for model in deletes {
                let runtime = try runtime(for: model.entity)
                mutations.append(
                    .delete(
                        identity: try runtime.identity(for: model),
                        model: model,
                        precondition: .exists
                    )
                )
            }
            let capturedMutations = mutations
            try await withTransaction(configuration: .default) { transaction in
                try await transaction.apply(capturedMutations)
            }

            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didExecuteBatch(insertCount: inserts.count, deleteCount: deletes.count, duration: duration)

            logger.trace("Executed batch", metadata: [
                "inserts": "\(inserts.count)",
                "deletes": "\(deletes.count)",
                ])
        } catch {
            let duration = DatabaseMonotonicMeasurement.nanoseconds(
                from: startTime,
                to: container.monotonicClock.now
            )
            metricsDelegate.didFailBatch(error: error, duration: duration)
            throw error
        }
    }

    /// Resolves, validates, and encodes one write without mutating storage.
    func prepareSave(
        _ model: PersistedModel,
        identity: EntityReference,
        precondition: WritePrecondition,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter?
    ) async throws -> PersistablePreparedWrite {
        let persistableType = model.entity
        let runtime = try runtime(for: persistableType)
        let canonicalModel = try runtime.canonicalized(model)
        let idTuple = try PersistableIdentifierKeyCodec.tuple(
            for: identity,
            expectedType: runtime.entity.identifierType
        )

        let key = itemKey(for: persistableType, id: idTuple)

        // Use ItemStorage for large value handling (stores chunks in blobs subspace)
        let storage = self.container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )

        // Load the persisted value for security evaluation and index maintenance.
        var oldCanonicalModel: PersistedModel?
        let existingRowPresent: Bool

        if let oldData = try await storage.read(for: key) {
            let previousCanonicalModel = try DataAccess.deserializePersistedModel(
                oldData,
                expectedEntity: persistableType
            )
            let persistedPreviousModel = try runtime.canonicalized(
                previousCanonicalModel
            )
            oldCanonicalModel = persistedPreviousModel
            try securityDelegate?.evaluateUpdate(
                persistedPreviousModel,
                newResource: canonicalModel
            )
            existingRowPresent = true
        } else {
            try securityDelegate?.evaluateCreate(canonicalModel)
            existingRowPresent = false
        }

        // Validate intent before the first write so a mismatch cannot leave
        // partial primary or derived state.
        try Self.evaluateWritePrecondition(
            precondition,
            existingRowPresent: existingRowPresent,
            currentVersion: try oldCanonicalModel.map(Self.entityVersionDigest),
            identity: identity
        )

        let data = try PersistableStorageCodec.encode(canonicalModel)
        let transientReservation: DatabaseIntermediateReservation?
        if let workMeter {
            var footprint = try oldCanonicalModel.map {
                try DatabaseEntityMutationFootprintMeter.footprint(
                    of: $0,
                    workMeter: workMeter
                )
            } ?? DatabaseIntermediateFootprint()
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(bytes: UInt64(data.count))
            )
            transientReservation = try workMeter.reserveIntermediate(
                rows: footprint.rows,
                bytes: footprint.bytes,
                at: .mutationPlanning
            )
        } else {
            transientReservation = nil
        }

        return PersistablePreparedWrite(
            result: PersistableWriteResult(
                canonicalModel: canonicalModel,
                previousCanonicalModel: oldCanonicalModel,
                encodedValue: data,
                identifier: idTuple
            ),
            key: key,
            storage: storage,
            runtime: runtime,
            transientReservation: transientReservation
        )
    }

    /// Applies a prepared write after the transaction has admitted every
    /// retained model and encoded byte owned across the mutation pipeline.
    func commitPreparedWrite(
        _ prepared: PersistablePreparedWrite,
        transaction: any TransactionAccess
    ) async throws {
        try await prepared.storage.write(
            prepared.result.encodedValue,
            for: prepared.key
        )
        try await indexMaintenanceService.updateIndexesUntyped(
            runtime: prepared.runtime,
            oldModel: prepared.result.previousCanonicalModel,
            newModel: prepared.result.canonicalModel,
            id: prepared.result.identifier,
            transaction: transaction
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
        currentVersion: ByteString?,
        identity: EntityReference
    ) throws {
        switch precondition {
        case .none:
            return
        case .notExists:
            if existingRowPresent {
                throw DatabaseContextError.preconditionFailed(
                    identity: identity,
                    precondition: precondition,
                    reason: "row already exists"
                )
            }
        case .exists:
            if !existingRowPresent {
                throw DatabaseContextError.preconditionFailed(
                    identity: identity,
                    precondition: precondition,
                    reason: "row not found"
                )
            }
        case .matchesStored(let version):
            guard let currentVersion else {
                throw DatabaseContextError.preconditionFailed(
                    identity: identity,
                    precondition: precondition,
                    reason: "row not found"
                )
            }
            if currentVersion != version {
                throw DatabaseContextError.preconditionFailed(
                    identity: identity,
                    precondition: precondition,
                    reason: "entity version changed"
                )
            }
        case .matchesStoredOrAbsent(let version):
            guard let currentVersion else { return }
            if currentVersion != version {
                throw DatabaseContextError.preconditionFailed(
                    identity: identity,
                    precondition: precondition,
                    reason: "entity version changed"
                )
            }
        }
    }

    private static func entityVersionDigest(
        for model: PersistedModel
    ) throws -> ByteString {
        try PersistableVersionTokenCodec.digest(fields: model.fields)
    }

    /// Deletes the currently persisted value and its physical indexes.
    /// A missing value produces no mutation.
    func prepareDelete(
        _ model: PersistedModel,
        identity: EntityReference,
        precondition: WritePrecondition,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter?
    ) async throws -> PersistablePreparedDelete? {
        let persistableType = model.entity
        let runtime = try runtime(for: persistableType)
        let idTuple = try PersistableIdentifierKeyCodec.tuple(
            for: identity,
            expectedType: runtime.entity.identifierType
        )
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
                identity: identity
            )
            return nil
        }
        let canonicalModel = try DataAccess.deserializePersistedModel(
            existingData,
            expectedEntity: persistableType
        )
        let persistedModel = try runtime.canonicalized(canonicalModel)
        try Self.evaluateWritePrecondition(
            precondition,
            existingRowPresent: true,
            currentVersion: try Self.entityVersionDigest(for: persistedModel),
            identity: identity
        )
        try securityDelegate?.evaluateDelete(persistedModel)
        let transientReservation: DatabaseIntermediateReservation?
        if let workMeter {
            let footprint = try DatabaseEntityMutationFootprintMeter.footprint(
                of: persistedModel,
                workMeter: workMeter
            )
            transientReservation = try workMeter.reserveIntermediate(
                rows: footprint.rows,
                bytes: footprint.bytes,
                at: .mutationPlanning
            )
        } else {
            transientReservation = nil
        }
        return PersistablePreparedDelete(
            persistedModel: persistedModel,
            identifier: idTuple,
            key: key,
            storage: storage,
            runtime: runtime,
            transientReservation: transientReservation
        )
    }

    /// Applies a prepared delete after mutation ownership admission succeeds.
    func commitPreparedDelete(
        _ prepared: PersistablePreparedDelete,
        transaction: any TransactionAccess
    ) async throws {
        try await indexMaintenanceService.updateIndexesUntyped(
            runtime: prepared.runtime,
            oldModel: prepared.persistedModel,
            newModel: nil,
            id: prepared.identifier,
            transaction: transaction
        )
        try await prepared.storage.delete(for: prepared.key)
    }

    private func runtime(
        for entity: String
    ) throws -> EntityRuntimeRegistration {
        guard let runtime = container.runtimeConfiguration.entityRuntimes
            .registration(named: entity) else {
            throw DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: entity
            )
        }
        return runtime
    }

    // MARK: - Predicate Evaluation

    /// Evaluate a predicate on a model
    private func evaluatePredicate<T: Persistable>(
        _ predicate: Predicate<T>,
        on model: borrowing T
    ) throws(QueryEvaluationError) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return try comparison.evaluate(on: model)

        case .and(let predicates):
            for predicate in predicates {
                if try !evaluatePredicate(predicate, on: model) {
                    return false
                }
            }
            return true

        case .or(let predicates):
            for predicate in predicates {
                if try evaluatePredicate(predicate, on: model) {
                    return true
                }
            }
            return false

        case .not(let predicate):
            return try !evaluatePredicate(predicate, on: model)

        case .true:
            return true

        case .false:
            return false
        }
    }

    // evaluateFieldComparison and compareModels removed — unified into
    // FieldComparison.evaluate(on:) and SortDescriptor.orderedComparison()

    /// Authorizes every result without routing a generic method through the
    /// dynamically selected security delegate.
    private func evaluateReadResults<Model: Persistable>(
        _ resources: borrowing [Model]
    ) throws {
        guard let securityDelegate else { return }
        for index in resources.indices {
            try securityDelegate.evaluateGet(
                try PersistedModel(resources[index]),
                fields: nil
            )
        }
    }

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
        return try await container.transactionExecutor.withTransaction(configuration: configuration, clock: container.monotonicClock) { transaction in
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

    public var description: String {
        switch self {
        case .uniqueConstraintViolation(let indexName, let values):
            return "Unique constraint violation on index '\(indexName)': values [\(values.joined(separator: ", "))] already exist for another entity"
        case .indexNotFound(let indexName):
            return "Index '\(indexName)' not found in schema"
        }
    }
}
