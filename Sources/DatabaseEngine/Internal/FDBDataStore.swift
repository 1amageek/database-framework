import Foundation
import StorageKit
import Core
import Logging

/// Internal storage abstraction for FoundationDB
///
/// This class is not intended for direct user access. It provides the underlying
/// storage operations that FDBContext uses internally.
///
/// Key structure:
/// - Items: `[subspace]/items/[persistableType]/[id]` = serialized data
/// - Indexes: `[subspace]/indexes/[indexName]/[values]/[id]` = ''
///
/// **Metrics**: Operations are tracked via DataStoreDelegate (default: MetricsDataStoreDelegate).
/// Metrics include operation counts, durations, and item counts per type.
internal final class FDBDataStore: DataStore, Sendable {
    // MARK: - DataStore Protocol

    /// Configuration type for FDBDataStore
    typealias Configuration = DBConfiguration

    /// Security delegate for access control evaluation
    let securityDelegate: (any DataStoreSecurityDelegate)?

    // MARK: - Properties

    /// FDB Container reference for transaction execution
    let container: DBContainer

    let subspace: Subspace
    let schema: Schema
    private let logger: Logger

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
    let indexStateManager: IndexStateManager

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
        logger: Logger? = nil,
        metricsDelegate: DataStoreDelegate? = nil,
        securityDelegate: (any DataStoreSecurityDelegate)? = nil,
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        self.container = container
        self.subspace = subspace
        self.schema = container.schema
        self.logger = logger ?? Logger(label: "com.fdb.runtime.datastore")
        self.metricsDelegate = metricsDelegate ?? MetricsDataStoreDelegate.shared
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
        self.indexStateManager = IndexStateManager(
            container: container,
            subspace: subspace,
            logger: logger
        )
        self.violationTracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: subspace.subspace(SubspaceKey.metadata)
        )
        self.indexMaintenanceService = IndexMaintenanceService(
            indexStateManager: indexStateManager,
            violationTracker: violationTracker,
            indexSubspace: indexSubspace,
            configurations: indexConfigurations,
            logger: logger
        )
    }

    // MARK: - Fetch Operations
    //
    // **Design Intent - No ReadVersionCache**:
    // Fetch operations use `container.engine.withTransaction()` directly without
    // ReadVersionCache. This is a deliberate simplification:
    //
    // 1. FDBDataStore is a low-level storage component that doesn't own a cache
    // 2. Cache ownership is at the FDBContext level (per unit-of-work)
    // 3. For weak read semantics optimization, users should use FDBContext.withTransaction()
    //
    // Example for optimized reads:
    // ```swift
    // let users = try await context.withTransaction(configuration: .readOnly) { tx in
    //     try await tx.get(User.self, ids: userIds)
    // }
    // ```

    /// Fetch all models of a type
    func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T] {
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
        let startTime = DispatchTime.now()

        do {
            let results: [T] = try await container.engine.withTransaction(configuration: .default) { transaction in
                // Use ItemStorage for proper handling of large values
                let storage = ItemStorage(
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

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailFetch(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Fetch a single model by ID
    func fetch<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws -> T? {
        let result: T? = try await withAutoCommit { [self] transaction in
            try await self.fetchByIdInTransaction(type, id: id, transaction: transaction)
        }

        return result
    }

    /// Fetch models matching a query
    ///
    /// This method attempts to use indexes for efficient queries:
    /// 1. If predicate matches an index, use index scan instead of full table scan
    /// 2. If sorting matches an index, use index ordering
    /// 3. Fall back to full table scan + in-memory filtering if no suitable index
    func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
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
        guard let condition = extractIndexableCondition(from: predicate),
              let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) else {
            return nil
        }

        // Check index state - only use readable indexes for queries
        let indexState = try await indexStateManager.state(of: matchingIndex.name)
        guard indexState.isReadable else {
            logger.debug("Index '\(matchingIndex.name)' is not readable (state: \(indexState)), falling back to scan")
            return nil
        }

        // Build index scan range based on condition OUTSIDE transaction
        let indexSubspaceForIndex = indexSubspace.subspace(matchingIndex.name)
        let valueTuple = condition.valueTuple
        let keyPathsCount = matchingIndex.keyPaths.count

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
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        case .greaterThanOrEqual:
            let beginKey = indexSubspaceForIndex.pack(valueTuple)
            let (_, endKey) = indexSubspaceForIndex.range()
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        case .lessThan:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = indexSubspaceForIndex.pack(valueTuple)
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        case .lessThanOrEqual:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = valueSubspace.range().1
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

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
                    if let idTuple = self.extractIDFromIndexKey(key, subspace: valueSubspace) {
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
                    if let idTuple = self.extractIDFromIndexKey(key, baseSubspace: baseSubspace, keyPathsCount: keyPathsCount) {
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
        case exactMatch(begin: [UInt8], end: [UInt8], valueSubspace: Subspace)
        /// Range scan with keyPathsCount to know how many elements are index values vs ID
        case range(begin: [UInt8], end: [UInt8], baseSubspace: Subspace, keyPathsCount: Int)
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
    private func extractAllIndexableConditions<T: Persistable>(from predicate: Predicate<T>) -> [IndexableCondition] {
        switch predicate {
        case .comparison(let comparison):
            switch comparison.op {
            case .equal, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual:
                // Convert to Tuple here for Sendable compatibility
                // If conversion fails, skip this condition (fall back to full scan)
                guard let tuple = try? valueToTuple(comparison.value) else { return [] }
                return [IndexableCondition(fieldName: comparison.fieldName, op: comparison.op, valueTuple: tuple)]
            default:
                return []
            }

        case .and(let predicates):
            // Extract all indexable conditions from AND predicates
            return predicates.flatMap { extractAllIndexableConditions(from: $0) }

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
    private func extractIndexableCondition<T: Persistable>(from predicate: Predicate<T>) -> IndexableCondition? {
        let allConditions = extractAllIndexableConditions(from: predicate)
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
        let descriptors = T.indexDescriptors

        // Priority 1: Find compound index matching multiple equals conditions
        for descriptor in descriptors {
            guard descriptor.keyPaths.count > 1 else { continue }

            // Check if first keyPaths have matching equals conditions
            var matchCount = 0
            for keyPath in descriptor.keyPaths {
                guard let partialKeyPath = keyPath as? PartialKeyPath<T> else { break }
                let fieldName = T.fieldName(for: partialKeyPath)
                if let condition = conditionsByField[fieldName], condition.op == .equal {
                    matchCount += 1
                } else {
                    break  // Must match from the beginning
                }
            }

            if matchCount >= 2 {
                // Use first condition for this compound index
                if let firstKeyPath = descriptor.keyPaths.first,
                   let partialKeyPath = firstKeyPath as? PartialKeyPath<T> {
                    let firstFieldName = T.fieldName(for: partialKeyPath)
                    if let condition = conditionsByField[firstFieldName] {
                        return condition
                    }
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
        // Find an index where the first keyPath matches the condition's field
        for descriptor in descriptors {
            if let firstKeyPath = descriptor.keyPaths.first,
               let partialKeyPath = firstKeyPath as? PartialKeyPath<T> {
                let fieldName = T.fieldName(for: partialKeyPath)
                if fieldName == condition.fieldName {
                    return descriptor
                }
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
            return Tuple([fieldValue.toTupleElement()])
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
    private func extractIDFromIndexKey(_ key: [UInt8], subspace: Subspace) -> Tuple? {
        do {
            let tuple = try subspace.unpack(key)
            // The tuple should be the ID portion
            if tuple.count > 0 {
                return tuple
            }
        } catch {
            // Key doesn't belong to this subspace
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
    private func extractIDFromIndexKey(_ key: [UInt8], baseSubspace: Subspace, keyPathsCount: Int) -> Tuple? {
        do {
            let tuple = try baseSubspace.unpack(key)
            // tuple = [value1, value2, ..., id1, id2, ...]
            // Skip first keyPathsCount elements (index values), rest is ID
            guard tuple.count > keyPathsCount else {
                return nil
            }

            // Extract ID elements (everything after index values)
            var idElements: [any TupleElement] = []
            for i in keyPathsCount..<tuple.count {
                if let element = tuple[i] {
                    idElements.append(element)
                }
            }

            guard !idElements.isEmpty else {
                return nil
            }

            return Tuple(idElements)
        } catch {
            // Key doesn't belong to this subspace
        }
        return nil
    }

    /// Fetch models by IDs (parallel reads for 10-30× speedup)
    private func fetchByIds<T: Persistable>(_ type: T.Type, ids: [Tuple]) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)

        // Pre-compute keys outside transaction
        let keys = ids.map { typeSubspace.pack($0) }

        return try await container.engine.withTransaction(configuration: .default) { transaction in
            let storage = ItemStorage(
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
    /// 1. If no predicate, count all records without deserialization
    /// 2. If predicate matches an index, count using index scan
    /// 3. Fall back to fetch and count if no optimization possible
    func fetchCount<T: Persistable>(_ query: Query<T>) async throws -> Int {
        // Evaluate LIST security via delegate
        let orderByFields = query.sortDescriptors.map { $0.fieldName }
        try securityDelegate?.evaluateList(
            type: T.self,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )
        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // For count, we can optimize by not deserializing if no predicate
        if combinedPredicate == nil {
            return try await countAll(T.self)
        }

        // Try to use index for counting
        if let predicate = combinedPredicate,
           let condition = extractIndexableCondition(from: predicate),
           let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) {
            return try await countUsingIndex(condition: condition, index: matchingIndex)
        }

        // Otherwise, fetch and count (security already evaluated above)
        let results = try await fetchInternal(query)
        return results.count
    }

    /// Internal fetch without security evaluation (for internal use after security is already evaluated)
    private func fetchInternal<T: Persistable>(_ query: Query<T>) async throws -> [T] {
        var results: [T]

        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // Try index-optimized fetch
        if let predicate = combinedPredicate,
           let indexResult = try await fetchUsingIndex(predicate, type: T.self, limit: query.fetchLimit) {
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

        // Apply offset
        if let offset = query.fetchOffset, offset > 0 {
            results = Array(results.dropFirst(offset))
        }

        // Apply limit
        if let limit = query.fetchLimit {
            results = Array(results.prefix(limit))
        }

        return results
    }

    // MARK: - Transaction-Injected Fetch (for FDBContext)

    /// Fetch items within an existing transaction
    ///
    /// This method is called by FDBContext.fetch() which manages the transaction
    /// and ReadVersionCache. FDBDataStore does not create transactions for this path.
    ///
    /// - Parameters:
    ///   - query: Query to execute
    ///   - transaction: Transaction to use for fetch
    /// - Returns: Array of matching items
    func fetchInTransaction<T: Persistable>(
        _ query: Query<T>,
        transaction: any Transaction
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
    /// Called by FDBContext.fetch() which manages transaction and cache.
    private func fetchInternalWithTransaction<T: Persistable>(
        _ query: Query<T>,
        transaction: any Transaction
    ) async throws -> [T] {
        var results: [T]

        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // Try index-optimized fetch
        if let predicate = combinedPredicate,
           let indexResult = try await fetchUsingIndexWithTransaction(predicate, type: T.self, limit: query.fetchLimit, transaction: transaction) {
            results = indexResult.models

            // If index didn't cover all predicate conditions, apply remaining filters
            if indexResult.needsPostFiltering {
                results = results.filter { model in
                    evaluatePredicate(predicate, on: model)
                }
            }
        } else {
            // Fall back to full table scan
            results = try await fetchAllWithTransaction(T.self, transaction: transaction)

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

        // Apply offset
        if let offset = query.fetchOffset, offset > 0 {
            results = Array(results.dropFirst(offset))
        }

        // Apply limit
        if let limit = query.fetchLimit {
            results = Array(results.prefix(limit))
        }

        return results
    }

    /// Fetch all models with an existing transaction
    private func fetchAllWithTransaction<T: Persistable>(
        _ type: T.Type,
        transaction: any Transaction
    ) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        let startTime = DispatchTime.now()

        do {
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )
            var results: [T] = []

            // ItemStorage.scan handles both inline and external (split) values transparently
            for try await (_, data) in storage.scan(begin: begin, end: end, snapshot: true) {
                let model: T = try DataAccess.deserialize(data)
                results.append(model)
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailFetch(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Attempt to fetch using an index with an existing transaction
    private func fetchUsingIndexWithTransaction<T: Persistable>(
        _ predicate: Predicate<T>,
        type: T.Type,
        limit: Int?,
        transaction: any Transaction
    ) async throws -> IndexFetchResult<T>? {
        // Extract indexable condition from predicate
        guard let condition = extractIndexableCondition(from: predicate),
              let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) else {
            return nil
        }

        // Check index state - only use readable indexes for queries
        // Use transaction-aware overload to avoid nested transaction deadlock
        let indexState = try await indexStateManager.state(of: matchingIndex.name, transaction: transaction)
        guard indexState.isReadable else {
            logger.debug("Index '\(matchingIndex.name)' is not readable (state: \(indexState)), falling back to scan")
            return nil
        }

        // Build index scan range based on condition
        let indexSubspaceForIndex = indexSubspace.subspace(matchingIndex.name)
        let valueTuple = condition.valueTuple
        let keyPathsCount = matchingIndex.keyPaths.count

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
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        case .greaterThanOrEqual:
            let beginKey = indexSubspaceForIndex.pack(valueTuple)
            let (_, endKey) = indexSubspaceForIndex.range()
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        case .lessThan:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = indexSubspaceForIndex.pack(valueTuple)
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        case .lessThanOrEqual:
            let (beginKey, _) = indexSubspaceForIndex.range()
            let endKey = valueSubspace.range().1
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        default:
            // Other comparisons (contains, hasPrefix, etc.) are not index-optimizable
            return nil
        }

        // Execute scan with provided transaction
        let streamingMode: StreamingMode = StreamingMode.forQuery(limit: limit)

        var ids: [Tuple] = []
        if let limit = limit {
            ids.reserveCapacity(limit)
        }

        switch scanRange {
        case .exactMatch(let begin, let end, let valueSubspace):
            let sequence = try await transaction.collectRange(
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: limit ?? 0,
                reverse: false,
                snapshot: true,
                streamingMode: streamingMode
            )
            for (key, _) in sequence {
                if let idTuple = self.extractIDFromIndexKey(key, subspace: valueSubspace) {
                    ids.append(idTuple)
                }
            }

        case .range(let begin, let end, let baseSubspace, let keyPathsCount):
            let sequence = try await transaction.collectRange(
                from: KeySelector.firstGreaterOrEqual(begin),
                to: KeySelector.firstGreaterOrEqual(end),
                limit: limit ?? 0,
                reverse: false,
                snapshot: true,
                streamingMode: streamingMode
            )
            for (key, _) in sequence {
                if let idTuple = self.extractIDFromIndexKey(key, baseSubspace: baseSubspace, keyPathsCount: keyPathsCount) {
                    ids.append(idTuple)
                }
            }
        }

        // If no IDs found, return empty result
        if ids.isEmpty {
            return IndexFetchResult(models: [], needsPostFiltering: false)
        }

        // Fetch models by IDs with provided transaction
        var models = try await fetchByIdsWithTransaction(T.self, ids: ids, transaction: transaction)

        // Apply GET security filter
        if let delegate = securityDelegate {
            models = delegate.filterByGetAccess(models)
        }

        // Determine if post-filtering is needed
        let needsPostFiltering = !isSimpleFieldPredicate(predicate, fieldName: condition.fieldName)

        return IndexFetchResult(models: models, needsPostFiltering: needsPostFiltering)
    }

    /// Fetch models by IDs with an existing transaction (parallel reads)
    private func fetchByIdsWithTransaction<T: Persistable>(
        _ type: T.Type,
        ids: [Tuple],
        transaction: any Transaction
    ) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let keys = ids.map { typeSubspace.pack($0) }

        let storage = ItemStorage(
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

    /// Fetch count within an existing transaction
    ///
    /// Called by FDBContext.fetchCount() which manages transaction and ReadVersionCache.
    func fetchCountInTransaction<T: Persistable>(
        _ query: Query<T>,
        transaction: any Transaction
    ) async throws -> Int {
        // Security evaluation
        let orderByFields = query.sortDescriptors.map { $0.fieldName }
        try securityDelegate?.evaluateList(
            type: T.self,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            orderBy: orderByFields.isEmpty ? nil : orderByFields
        )

        // Combine predicates into single predicate for evaluation
        let combinedPredicate: Predicate<T>? = query.predicates.isEmpty ? nil :
            (query.predicates.count == 1 ? query.predicates[0] : .and(query.predicates))

        // For count, we can optimize by not deserializing if no predicate
        if combinedPredicate == nil {
            return try await countAllWithTransaction(T.self, transaction: transaction)
        }

        // Try to use index for counting
        if let predicate = combinedPredicate,
           let condition = extractIndexableCondition(from: predicate),
           let matchingIndex = findMatchingIndex(for: condition, in: T.indexDescriptors, type: T.self) {
            return try await countUsingIndexWithTransaction(condition: condition, index: matchingIndex, transaction: transaction)
        }

        // Otherwise, fetch and count
        let results = try await fetchInternalWithTransaction(query, transaction: transaction)
        return results.count
    }

    /// Fetch a single model by ID within an existing transaction
    ///
    /// This method performs a direct key lookup (O(1)) rather than a query scan.
    /// Called by FDBContext.model(for:as:) which manages transaction and ReadVersionCache.
    ///
    /// - Parameters:
    ///   - type: The model type
    ///   - id: The model's identifier
    ///   - transaction: The transaction to use
    /// - Returns: The model if found, nil if not found
    /// - Throws: SecurityError if GET not allowed, or other errors on failure
    func fetchByIdInTransaction<T: Persistable>(
        _ type: T.Type,
        id: any TupleElement,
        transaction: any Transaction
    ) async throws -> T? {
        let key = itemKey(for: T.persistableType, id: id)

        guard let bytes = try await ItemStorage.read(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace,
            for: key
        ) else {
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
        id: any TupleElement
    ) -> Bytes {
        let encodedID: Bytes
        if let tuple = id as? Tuple {
            encodedID = tuple.pack()
        } else {
            encodedID = id.encodeTuple()
        }

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

        var key = keyPrefix
        key.reserveCapacity(key.count + encodedID.count)
        key.append(contentsOf: encodedID)
        return key
    }

    /// Count all models with an existing transaction
    private func countAllWithTransaction<T: Persistable>(
        _ type: T.Type,
        transaction: any Transaction
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
        transaction: any Transaction
    ) async throws -> Int {
        let indexSubspaceForIndex = indexSubspace.subspace(index.name)
        let valueTuple = condition.valueTuple

        // Build value subspace using flat encoding (see fetchUsingIndexWithTransaction comment)
        let valueSubspace = Subspace(prefix: indexSubspaceForIndex.prefix + valueTuple.pack())

        let beginKey: [UInt8]
        let endKey: [UInt8]

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

    /// Count using index scan (without deserializing records)
    private func countUsingIndex(condition: IndexableCondition, index: IndexDescriptor) async throws -> Int {
        let indexSubspaceForIndex = indexSubspace.subspace(index.name)
        let valueTuple = condition.valueTuple

        // Compute key range outside transaction to avoid capturing non-Sendable condition
        let beginKey: [UInt8]
        let endKey: [UInt8]

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

        let startTime = DispatchTime.now()

        do {
            try await container.engine.withTransaction(configuration: .default) { transaction in
                for model in models {
                    try await self.saveModel(model, transaction: transaction)
                }
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didSave(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Saved \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailSave(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Save a single model within a transaction
    private func saveModel<T: Persistable>(
        _ model: T,
        transaction: any Transaction
    ) async throws {
        // Validate and get ID
        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        // Serialize using Protobuf via DataAccess
        let data = try DataAccess.serialize(model)

        // Build key
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Use ItemStorage for large value handling (stores chunks in blobs subspace)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )

        // Check for existing record (for index updates)
        let oldModel: T?
        if let existingBytes = try await storage.read(for: key) {
            // Use Protobuf deserialization via DataAccess
            oldModel = try DataAccess.deserialize(existingBytes)
        } else {
            oldModel = nil
        }

        // Save the record (handles compression + external storage for >90KB)
        try await storage.write(data, for: key)

        // Update indexes via IndexMaintenanceService
        try await indexMaintenanceService.updateIndexes(oldModel: oldModel, newModel: model, id: idTuple, transaction: transaction)
    }

    // MARK: - Delete Operations

    /// Delete models
    func delete<T: Persistable>(_ models: [T]) async throws {
        guard !models.isEmpty else { return }

        let startTime = DispatchTime.now()

        do {
            try await container.engine.withTransaction(configuration: .default) { transaction in
                for model in models {
                    try await self.deleteModel(model, transaction: transaction)
                }
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didDelete(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Deleted \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailDelete(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Delete a single model within a transaction
    private func deleteModel<T: Persistable>(
        _ model: T,
        transaction: any Transaction
    ) async throws {
        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove index entries first via IndexMaintenanceService
        try await indexMaintenanceService.updateIndexes(oldModel: model, newModel: nil as T?, id: idTuple, transaction: transaction)

        // Delete the record (handles external blob chunks)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )
        try await storage.delete(for: key)
    }

    /// Delete model by ID
    func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws {
        let idTuple = (id as? Tuple) ?? Tuple([id])
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        try await container.engine.withTransaction(configuration: .default) { transaction in
            let storage = ItemStorage(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )

            // Load the model first for index cleanup
            if let bytes = try await storage.read(for: key) {
                // Use Protobuf deserialization via DataAccess
                let model: T = try DataAccess.deserialize(bytes)

                // Remove index entries via IndexMaintenanceService
                try await self.indexMaintenanceService.updateIndexes(oldModel: model, newModel: nil as T?, id: idTuple, transaction: transaction)
            }

            // Delete the record (handles external blob chunks)
            try await storage.delete(for: key)
        }
    }

    // MARK: - Batch Operations

    /// Execute a batch of saves and deletes in a single transaction
    func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable]
    ) async throws {
        let startTime = DispatchTime.now()

        do {
            if let fastPath = try await executeBatchFastPathIfEligible(
                inserts: inserts,
                deletes: deletes
            ) {
                _ = fastPath
            } else {
                _ = try await container.engine.withTransaction(configuration: .default) { transaction in
                    try await self.executeBatchInTransaction(
                        inserts: inserts,
                        deletes: deletes,
                        transaction: transaction
                    )
                }
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didExecuteBatch(insertCount: inserts.count, deleteCount: deletes.count, duration: duration)

            logger.trace("Executed batch", metadata: [
                "inserts": "\(inserts.count)",
                "deletes": "\(deletes.count)"
            ])
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            metricsDelegate.didFailBatch(error: error, duration: duration)
            throw error
        }
    }

    /// Attempt a single-model fast path for direct DataStore callers.
    ///
    /// Mirrors the FDBContext fast path for the same safe subset:
    /// exactly one insert or delete, security disabled, and no indexes.
    /// In that case, we can use auto-commit and skip the existing-record read,
    /// because overwrite semantics are identical for no-index/no-security types.
    private func executeBatchFastPathIfEligible(
        inserts: [any Persistable],
        deletes: [any Persistable]
    ) async throws -> Bool? {
        let isSingleInsert = inserts.count == 1 && deletes.isEmpty
        let isSingleDelete = inserts.isEmpty && deletes.count == 1
        guard isSingleInsert || isSingleDelete else { return nil }

        let model = isSingleInsert ? inserts[0] : deletes[0]
        guard securityDelegate == nil else { return nil }
        guard type(of: model).indexDescriptors.isEmpty else { return nil }

        if isSingleInsert {
            _ = try await withAutoCommit { transaction in
                try await self.executeBatchInTransaction(
                    inserts: inserts,
                    deletes: [],
                    transaction: transaction,
                    skipExistingCheck: true
                )
            }
        } else {
            _ = try await withAutoCommit { transaction in
                try await self.executeBatchInTransaction(
                    inserts: [],
                    deletes: deletes,
                    transaction: transaction,
                    skipExistingCheck: true
                )
            }
        }

        return true
    }

    // MARK: - Transaction-Scoped Operations (DataStore Protocol)

    /// Execute batch operations within an externally-provided transaction
    ///
    /// Optimized for batch processing:
    /// - Single encoder reused across all models
    /// - Returns serialized data for dual-write optimization
    ///
    /// - Parameters:
    ///   - inserts: Models to insert or update
    ///   - deletes: Models to delete
    ///   - transaction: The transaction to use
    /// - Returns: Serialized data for inserted models
    @discardableResult
    func executeBatchInTransaction(
        inserts: [any Persistable],
        deletes: [any Persistable],
        transaction: any Transaction,
        skipExistingCheck: Bool = false
    ) async throws -> [SerializedModel] {
        let encoder = ProtobufEncoder()
        var serializedModels: [SerializedModel] = []

        // Process deletes first (security evaluation)
        for model in deletes {
            try securityDelegate?.evaluateDelete(model)
        }

        // Process inserts with single encoder
        for model in inserts {
            // Determine if we can skip reading existing records per model type:
            // Safe only when ALL of:
            // 1. Caller signals known inserts (skipExistingCheck)
            // 2. Security is disabled (no CREATE/UPDATE evaluation needed)
            // 3. Model type has no indexes (no oldModel needed for diff-based index update)
            let hasIndexes = !type(of: model).indexDescriptors.isEmpty
            let canSkip = skipExistingCheck && securityDelegate == nil && !hasIndexes

            let serialized = try await saveModelUntypedWithSecurityReturningData(
                model,
                transaction: transaction,
                encoder: encoder,
                skipExistingCheck: canSkip
            )
            serializedModels.append(serialized)
        }

        // Process deletes
        for model in deletes {
            // Skip blob cleanup when security is disabled and no indexes:
            // blob chunks only exist for >90KB items, and the clearRange DELETE
            // query is unnecessary overhead for typical inline-sized records.
            let hasIndexes = !type(of: model).indexDescriptors.isEmpty
            let canSkipBlobs = skipExistingCheck && securityDelegate == nil && !hasIndexes
            try await deleteModelUntyped(
                model,
                transaction: transaction,
                skipBlobCleanup: canSkipBlobs
            )
        }

        return serializedModels
    }

    /// Execute operations within a raw transaction
    ///
    /// Provides raw Transaction for coordinating operations
    /// across multiple DataStores in a single atomic transaction.
    ///
    /// **Design Intent - No ReadVersionCache**:
    /// This method uses `container.engine.withTransaction()` directly, bypassing
    /// any ReadVersionCache. This is intentional because:
    ///
    /// 1. **Write operations need latest data**: FDBDataStore primarily handles writes
    ///    (`save()`, `delete()`) which require strong consistency, not stale cached reads.
    ///
    /// 2. **DataStore is a low-level component**: FDBDataStore doesn't own a cache;
    ///    cache ownership is at the FDBContext level (per unit-of-work).
    ///
    /// 3. **Called by FDBContext.save()**: When FDBContext.save() uses this method,
    ///    it's for write operations where cache staleness would be problematic.
    ///
    /// For read operations that benefit from weak read semantics, users should use
    /// `FDBContext.withTransaction()` which properly uses the context's cache.
    func withRawTransaction<T: Sendable>(
        _ body: @Sendable @escaping (any Transaction) async throws -> T
    ) async throws -> T {
        // Use StorageEngine.withTransaction directly for eager connection acquisition.
        // This avoids the TransactionRunner → createTransaction → lazy connection path,
        // which adds overhead from Task + withCheckedContinuation for deferred
        // connection pool access.
        return try await container.engine.withTransaction(body)
    }

    /// Execute an operation in auto-commit mode (no BEGIN/COMMIT).
    ///
    /// Delegates to StorageEngine.withAutoCommit() which skips BEGIN/COMMIT
    /// for PostgreSQL, saving 2 SQL round-trips per operation.
    func withAutoCommit<T: Sendable>(
        _ body: @Sendable @escaping (any Transaction) async throws -> T
    ) async throws -> T {
        return try await container.engine.withAutoCommit(body)
    }

    /// Save model with security evaluation, returning serialized data for dual-write
    ///
    /// - Parameters:
    ///   - model: The model to save
    ///   - transaction: The transaction to use
    ///   - encoder: Reusable Protobuf encoder
    ///   - skipExistingCheck: When true, skips reading existing record from storage.
    ///     The caller (executeBatchInTransaction) ensures this is only true when ALL of:
    ///     (1) security is disabled, (2) model type has no indexes, and
    ///     (3) the operation is from FDBContext's insert path.
    ///     This eliminates one storage read round-trip per insert.
    private func saveModelUntypedWithSecurityReturningData(
        _ model: any Persistable,
        transaction: any Transaction,
        encoder: ProtobufEncoder,
        skipExistingCheck: Bool = false
    ) async throws -> SerializedModel {
        let modelType = type(of: model)
        let persistableType = modelType.persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let key = itemKey(for: persistableType, id: idTuple)

        // Use ItemStorage for large value handling (stores chunks in blobs subspace)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )

        // Check for existing record and deserialize for security evaluation and index update
        var oldModel: (any Persistable)?
        let isNewRecord: Bool

        if skipExistingCheck {
            // Fast path: caller guarantees this is a new insert with no security checks needed
            isNewRecord = true
        } else if let oldData = try await storage.read(for: key) {
            // Update - deserialize old model (used for both security and index update)
            oldModel = try DataAccess.deserializeAny(oldData, as: modelType)
            try securityDelegate?.evaluateUpdate(oldModel!, newResource: model)
            isNewRecord = false
        } else {
            // Create
            try securityDelegate?.evaluateCreate(model)
            isNewRecord = true
        }

        // Serialize using Protobuf (encoder reused across all models)
        let data = Array(try encoder.encode(model))

        // Save the record (handles compression + external storage for >90KB)
        // Skip blob cleanup for known new records (no old blobs to clear)
        try await storage.write(data, for: key, isNewRecord: isNewRecord)

        // Update indexes via IndexMaintenanceService (efficient diff-based update)
        try await indexMaintenanceService.updateIndexesUntyped(
            oldModel: oldModel,
            newModel: model,
            id: idTuple,
            transaction: transaction
        )

        // Return serialized data for dual-write optimization
        return SerializedModel(model: model, data: data, idTuple: idTuple)
    }

    /// Delete model without type parameter (for batch operations)
    ///
    /// - Parameters:
    ///   - model: The model to delete
    ///   - transaction: The transaction to use
    ///   - skipBlobCleanup: When true, skips clearing blob chunks in storage.
    ///     Safe when security is disabled and no indexes exist (inline-only data).
    private func deleteModelUntyped(
        _ model: any Persistable,
        transaction: any Transaction,
        skipBlobCleanup: Bool = false
    ) async throws {
        let persistableType = type(of: model).persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let key = itemKey(for: persistableType, id: idTuple)

        // Remove index entries first via IndexMaintenanceService (efficient diff-based update)
        try await indexMaintenanceService.updateIndexesUntyped(
            oldModel: model,
            newModel: nil,
            id: idTuple,
            transaction: transaction
        )

        // Delete the record (skip blob cleanup when caller guarantees inline-only data)
        let storage = ItemStorage(
            transaction: transaction,
            blobsSubspace: self.blobsSubspace
        )
        try await storage.delete(for: key, skipBlobCleanup: skipBlobCleanup)
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

    // MARK: - Clear Operations

    /// Clear all records of a type
    func clearAll<T: Persistable>(_ type: T.Type) async throws {
        // Admin-only operation
        try securityDelegate?.requireAdmin(operation: "clearAll", targetType: T.persistableType)

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            let typeSubspace = self.itemSubspace.subspace(T.persistableType)
            let (begin, end) = typeSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)

            // Also clear indexes for this type
            for descriptor in T.indexDescriptors {
                let indexRange = self.indexSubspace.subspace(descriptor.name).range()
                transaction.clearRange(beginKey: indexRange.0, endKey: indexRange.1)
            }
        }
    }

    // MARK: - Transaction Operations

    /// Execute operations within a transaction
    ///
    /// **Note**: This uses the database directly without ReadVersionCache.
    /// For application operations that benefit from caching, use FDBContext.withTransaction().
    func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable @escaping (any TransactionContextProtocol) async throws -> T
    ) async throws -> T {
        return try await container.engine.withTransaction(configuration: configuration) { transaction in
            // Create a secure transaction context
            let context = SecureTransactionContext(
                transaction: transaction,
                itemSubspace: self.itemSubspace,
                indexSubspace: self.indexSubspace,
                blobsSubspace: self.blobsSubspace,
                indexMaintenanceService: self.indexMaintenanceService,
                securityDelegate: self.securityDelegate
            )
            return try await operation(context)
        }
    }
}

// MARK: - FDBIndexError

/// Errors that can occur during index operations
public enum FDBIndexError: Error, CustomStringConvertible {
    /// Unique constraint violation: duplicate value exists for another record
    case uniqueConstraintViolation(indexName: String, values: [String])

    /// Index not found in schema
    case indexNotFound(indexName: String)

    /// Unsupported index kind for operation
    case unsupportedIndexKind(indexName: String, kindIdentifier: String)

    public var description: String {
        switch self {
        case .uniqueConstraintViolation(let indexName, let values):
            return "Unique constraint violation on index '\(indexName)': values [\(values.joined(separator: ", "))] already exist for another record"
        case .indexNotFound(let indexName):
            return "Index '\(indexName)' not found in schema"
        case .unsupportedIndexKind(let indexName, let kindIdentifier):
            return "Unsupported index kind '\(kindIdentifier)' for index '\(indexName)'"
        }
    }
}
