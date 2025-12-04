import Foundation
import FoundationDB
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
    typealias Configuration = FDBConfiguration
    // MARK: - Properties

    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let schema: Schema
    private let logger: Logger

    /// Delegate for operation callbacks (metrics, etc.)
    private let delegate: DataStoreDelegate

    /// Items subspace: [subspace]/items/
    let itemSubspace: Subspace

    /// Indexes subspace: [subspace]/indexes/
    let indexSubspace: Subspace

    /// Index state manager for checking index readability
    let indexStateManager: IndexStateManager

    // MARK: - Initialization

    init(
        database: any DatabaseProtocol,
        subspace: Subspace,
        schema: Schema,
        logger: Logger? = nil,
        delegate: DataStoreDelegate? = nil
    ) {
        self.database = database
        self.subspace = subspace
        self.schema = schema
        self.logger = logger ?? Logger(label: "com.fdb.runtime.datastore")
        self.delegate = delegate ?? MetricsDataStoreDelegate.shared
        self.itemSubspace = subspace.subspace(SubspaceKey.items)
        self.indexSubspace = subspace.subspace(SubspaceKey.indexes)
        self.indexStateManager = IndexStateManager(database: database, subspace: subspace, logger: logger)
    }

    // MARK: - Fetch Operations

    /// Fetch all models of a type
    func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()
        let startTime = DispatchTime.now()

        do {
            let results: [T] = try await database.withTransaction { transaction in
                var results: [T] = []
                // Use .wantAll for full table scan - aggressive prefetch reduces round-trips
                let sequence = transaction.getRange(
                    from: FDB.KeySelector.firstGreaterOrEqual(begin),
                    to: FDB.KeySelector.firstGreaterOrEqual(end),
                    limit: 0,  // unlimited
                    reverse: false,
                    snapshot: true,
                    streamingMode: .wantAll
                )

                for try await (_, value) in sequence {
                    // Use Protobuf deserialization via DataAccess
                    let model: T = try DataAccess.deserialize(value)
                    results.append(model)
                }
                return results
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didFetch(itemType: T.persistableType, count: results.count, duration: duration)

            return results
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didFailFetch(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Fetch a single model by ID
    func fetch<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws -> T? {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let keyTuple = (id as? Tuple) ?? Tuple([id])
        let key = typeSubspace.pack(keyTuple)

        return try await database.withTransaction { transaction in
            guard let bytes = try await transaction.getValue(for: key, snapshot: false) else {
                return nil
            }

            // Use Protobuf deserialization via DataAccess
            return try DataAccess.deserialize(bytes)
        }
    }

    /// Fetch models matching a query
    ///
    /// This method attempts to use indexes for efficient queries:
    /// 1. If predicate matches an index, use index scan instead of full table scan
    /// 2. If sorting matches an index, use index ordering
    /// 3. Fall back to full table scan + in-memory filtering if no suitable index
    func fetch<T: Persistable>(_ query: Query<T>) async throws -> [T] {
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
            results = try await fetchAll(T.self)

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
                    let comparison = compareModels(lhs, rhs, by: sortDescriptor.fieldName)
                    if comparison != .orderedSame {
                        switch sortDescriptor.order {
                        case .ascending:
                            return comparison == .orderedAscending
                        case .descending:
                            return comparison == .orderedDescending
                        }
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

        // Compute key range outside transaction to avoid capturing non-Sendable condition
        let scanRange: IndexScanRange
        switch condition.op {
        case .equal:
            let valueSubspace = indexSubspaceForIndex.subspace(valueTuple)
            let (begin, end) = valueSubspace.range()
            scanRange = .exactMatch(begin: begin, end: end, valueSubspace: valueSubspace)

        case .greaterThan:
            let valueSubspace = indexSubspaceForIndex.subspace(valueTuple)
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
            let valueSubspace = indexSubspaceForIndex.subspace(valueTuple)
            let endKey = valueSubspace.range().1
            scanRange = .range(begin: beginKey, end: endKey, baseSubspace: indexSubspaceForIndex, keyPathsCount: keyPathsCount)

        default:
            // Other comparisons (contains, hasPrefix, etc.) are not index-optimizable
            return nil
        }

        // Execute scan in transaction - all captured values are now Sendable
        // Select optimal StreamingMode based on limit
        let streamingMode: FDB.StreamingMode = FDB.StreamingMode.forQuery(limit: limit)

        let ids: [Tuple] = try await database.withTransaction { transaction in
            var ids: [Tuple] = []

            switch scanRange {
            case .exactMatch(let begin, let end, let valueSubspace):
                // Apply limit pushdown to reduce server-side work
                let sequence = transaction.getRange(
                    from: FDB.KeySelector.firstGreaterOrEqual(begin),
                    to: FDB.KeySelector.firstGreaterOrEqual(end),
                    limit: limit ?? 0,  // 0 = unlimited in FDB
                    reverse: false,
                    snapshot: true,
                    streamingMode: streamingMode
                )
                for try await (key, _) in sequence {
                    if let idTuple = self.extractIDFromIndexKey(key, subspace: valueSubspace) {
                        ids.append(idTuple)
                    }
                }

            case .range(let begin, let end, let baseSubspace, let keyPathsCount):
                // Apply limit pushdown to reduce server-side work
                let sequence = transaction.getRange(
                    from: FDB.KeySelector.firstGreaterOrEqual(begin),
                    to: FDB.KeySelector.firstGreaterOrEqual(end),
                    limit: limit ?? 0,  // 0 = unlimited in FDB
                    reverse: false,
                    snapshot: true,
                    streamingMode: streamingMode
                )
                for try await (key, _) in sequence {
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
        let models = try await fetchByIds(T.self, ids: ids)

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
                let tuple = valueToTuple(comparison.value)
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
    private func valueToTuple(_ value: Any) -> Tuple {
        // Handle FieldValue first (most common case after refactoring)
        if let fieldValue = value as? FieldValue {
            return Tuple([fieldValue.toTupleElement()])
        }

        if let tupleElement = value as? any TupleElement {
            return Tuple([tupleElement])
        }

        // Handle common types
        switch value {
        case let v as Int: return Tuple([Int64(v)])
        case let v as Int32: return Tuple([Int64(v)])
        case let v as Int16: return Tuple([Int64(v)])
        case let v as Int8: return Tuple([Int64(v)])
        case let v as UInt: return Tuple([Int64(v)])
        case let v as UInt32: return Tuple([Int64(v)])
        case let v as UInt16: return Tuple([Int64(v)])
        case let v as UInt8: return Tuple([Int64(v)])
        default:
            // Convert to string as fallback
            return Tuple([String(describing: value)])
        }
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

    /// Fetch models by IDs
    private func fetchByIds<T: Persistable>(_ type: T.Type, ids: [Tuple]) async throws -> [T] {
        let typeSubspace = itemSubspace.subspace(T.persistableType)

        // Pre-compute keys outside transaction
        let keys = ids.map { typeSubspace.pack($0) }

        return try await database.withTransaction { transaction in
            var results: [T] = []
            for key in keys {
                if let bytes = try await transaction.getValue(for: key, snapshot: true) {
                    let model: T = try DataAccess.deserialize(bytes)
                    results.append(model)
                }
            }
            return results
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

        // Otherwise, fetch and count
        let results = try await fetch(query)
        return results.count
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

        return try await database.withTransaction { transaction in
            var count = 0
            // Use .wantAll for count operations - aggressive prefetch
            let sequence = transaction.getRange(
                from: FDB.KeySelector.firstGreaterOrEqual(beginKey),
                to: FDB.KeySelector.firstGreaterOrEqual(endKey),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            for try await _ in sequence {
                count += 1
            }
            return count
        }
    }

    /// Count all models of a type
    private func countAll<T: Persistable>(_ type: T.Type) async throws -> Int {
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let (begin, end) = typeSubspace.range()

        return try await database.withTransaction { transaction in
            var count = 0
            // Use .wantAll for count operations - aggressive prefetch
            let sequence = transaction.getRange(
                from: FDB.KeySelector.firstGreaterOrEqual(begin),
                to: FDB.KeySelector.firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )

            for try await _ in sequence {
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

        let sizeBytes = try await database.withTransaction { transaction in
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

        let sizeBytes = try await database.withTransaction { transaction in
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
            try await database.withTransaction { transaction in
                for model in models {
                    try await self.saveModel(model, transaction: transaction)
                }
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didSave(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Saved \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didFailSave(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Save a single model within a transaction
    private func saveModel<T: Persistable>(
        _ model: T,
        transaction: any TransactionProtocol
    ) async throws {
        // Validate and get ID
        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        // Serialize using Protobuf via DataAccess
        let data = try DataAccess.serialize(model)

        // Build key
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Check for existing record (for index updates)
        let oldModel: T?
        if let existingBytes = try await transaction.getValue(for: key, snapshot: false) {
            // Use Protobuf deserialization via DataAccess
            oldModel = try DataAccess.deserialize(existingBytes)
        } else {
            oldModel = nil
        }

        // Save the record
        transaction.setValue(data, for: key)

        // Update indexes
        try await updateIndexes(oldModel: oldModel, newModel: model, id: idTuple, transaction: transaction)
    }

    // MARK: - Delete Operations

    /// Delete models
    func delete<T: Persistable>(_ models: [T]) async throws {
        guard !models.isEmpty else { return }

        let startTime = DispatchTime.now()

        do {
            try await database.withTransaction { transaction in
                for model in models {
                    try await self.deleteModel(model, transaction: transaction)
                }
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didDelete(itemType: T.persistableType, count: models.count, duration: duration)

            logger.trace("Deleted \(models.count) models", metadata: [
                "type": "\(T.persistableType)"
            ])
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didFailDelete(itemType: T.persistableType, error: error, duration: duration)
            throw error
        }
    }

    /// Delete a single model within a transaction
    private func deleteModel<T: Persistable>(
        _ model: T,
        transaction: any TransactionProtocol
    ) async throws {
        let validatedID = try model.validateIDForStorage()
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove index entries first
        try await updateIndexes(oldModel: model, newModel: nil, id: idTuple, transaction: transaction)

        // Delete the record
        transaction.clear(key: key)
    }

    /// Delete model by ID
    func delete<T: Persistable>(_ type: T.Type, id: any TupleElement) async throws {
        let idTuple = (id as? Tuple) ?? Tuple([id])
        let typeSubspace = itemSubspace.subspace(T.persistableType)
        let key = typeSubspace.pack(idTuple)

        try await database.withTransaction { transaction in
            // Load the model first for index cleanup
            if let bytes = try await transaction.getValue(for: key, snapshot: false) {
                // Use Protobuf deserialization via DataAccess
                let model: T = try DataAccess.deserialize(bytes)

                // Remove index entries
                try await self.updateIndexes(oldModel: model, newModel: nil, id: idTuple, transaction: transaction)
            }

            // Delete the record
            transaction.clear(key: key)
        }
    }

    // MARK: - Batch Operations

    /// Execute a batch of saves and deletes in a single transaction
    func executeBatch(
        inserts: [any Persistable],
        deletes: [any Persistable]
    ) async throws {
        let startTime = DispatchTime.now()
        let encoder = ProtobufEncoder()  // Reuse across all inserts (Sendable)

        do {
            try await database.withTransaction { transaction in
                try transaction.setOption(forOption: .priorityBatch)
                // Process inserts
                for model in inserts {
                    try await self.saveModelUntyped(model, transaction: transaction, encoder: encoder)
                }

                // Process deletes
                for model in deletes {
                    try await self.deleteModelUntyped(model, transaction: transaction)
                }
            }

            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didExecuteBatch(insertCount: inserts.count, deleteCount: deletes.count, duration: duration)

            logger.trace("Executed batch", metadata: [
                "inserts": "\(inserts.count)",
                "deletes": "\(deletes.count)"
            ])
        } catch {
            let duration = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            delegate.didFailBatch(error: error, duration: duration)
            throw error
        }
    }

    /// Save model without type parameter (for batch operations)
    private func saveModelUntyped(
        _ model: any Persistable,
        transaction: any TransactionProtocol,
        encoder: ProtobufEncoder
    ) async throws {
        let persistableType = type(of: model).persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        // Serialize using Protobuf (encoder reused across all models)
        let data = try encoder.encode(model)

        let typeSubspace = itemSubspace.subspace(persistableType)
        let key = typeSubspace.pack(idTuple)

        // Check for existing record (for index updates)
        let oldData = try await transaction.getValue(for: key, snapshot: false)

        // Save the record
        transaction.setValue(Array(data), for: key)

        // Update indexes using type-erased helper
        try await updateIndexesUntyped(
            oldData: oldData,
            newModel: model,
            id: idTuple,
            transaction: transaction
        )
    }

    /// Delete model without type parameter (for batch operations)
    private func deleteModelUntyped(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws {
        let persistableType = type(of: model).persistableType
        let validatedID = try validateID(model.id, for: persistableType)
        let idTuple = (validatedID as? Tuple) ?? Tuple([validatedID])

        let typeSubspace = itemSubspace.subspace(persistableType)
        let key = typeSubspace.pack(idTuple)

        // Remove index entries first
        try await updateIndexesUntyped(
            oldData: nil,  // We use the model directly for old values
            newModel: nil,
            id: idTuple,
            transaction: transaction,
            deletingModel: model  // Pass the model being deleted
        )

        // Delete the record
        transaction.clear(key: key)
    }

    // MARK: - Index Operations

    /// Update indexes for a model change
    ///
    /// This method handles different IndexKind behaviors:
    /// - **ScalarIndexKind/VersionIndexKind**: Standard key-value index
    /// - **CountIndexKind**: Atomic increment/decrement counter
    /// - **SumIndexKind**: Atomic add/subtract aggregation
    /// - **MinIndexKind/MaxIndexKind**: Sorted value tracking
    ///
    /// For unique indexes, validates that no duplicate values exist.
    ///
    /// **Index State Handling**:
    /// - Only indexes with `shouldMaintain == true` are updated
    /// - `disabled` indexes are skipped entirely (no maintenance, no unique checks)
    /// - `writeOnly` and `readable` indexes are maintained normally
    private func updateIndexes<T: Persistable>(
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let indexDescriptors = T.indexDescriptors
        guard !indexDescriptors.isEmpty else { return }

        // Batch fetch all index states for performance
        let indexNames = indexDescriptors.map(\.name)
        let indexStates = try await indexStateManager.states(of: indexNames, transaction: transaction)

        for descriptor in indexDescriptors {
            // Check if index should be maintained based on its state
            let state = indexStates[descriptor.name] ?? .disabled
            guard state.shouldMaintain else {
                logger.trace("Skipping index '\(descriptor.name)' maintenance (state: \(state))")
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)
            let kindIdentifier = type(of: descriptor.kind).identifier

            switch kindIdentifier {
            case "count":
                // CountIndexKind: Atomic counter per group key
                try await updateCountIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    transaction: transaction
                )

            case "sum":
                // SumIndexKind: Atomic sum per group key
                try await updateSumIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    transaction: transaction
                )

            case "min", "max":
                // MinIndexKind/MaxIndexKind: Sorted value tracking with group key
                try await updateMinMaxIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    id: id,
                    transaction: transaction
                )

            default:
                // ScalarIndexKind, VersionIndexKind: Standard key-value index
                try await updateScalarIndex(
                    descriptor: descriptor,
                    subspace: indexSubspaceForIndex,
                    oldModel: oldModel,
                    newModel: newModel,
                    id: id,
                    transaction: transaction
                )
            }
        }
    }

    /// Update scalar index (ScalarIndexKind, VersionIndexKind)
    ///
    /// Key structure: `[indexSubspace][fieldValue][primaryKey] = ''`
    private func updateScalarIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old index entries
        if let old = oldModel {
            let oldValues = extractIndexValues(from: old, keyPaths: descriptor.keyPaths)
            if !oldValues.isEmpty {
                let oldIndexKey = buildIndexKey(
                    subspace: subspace,
                    values: oldValues,
                    id: id
                )
                transaction.clear(key: oldIndexKey)
            }
        }

        // Add new index entries
        if let new = newModel {
            let newValues = extractIndexValues(from: new, keyPaths: descriptor.keyPaths)
            if !newValues.isEmpty {
                // Check unique constraint
                if descriptor.isUnique {
                    try await checkUniqueConstraint(
                        descriptor: descriptor,
                        subspace: subspace,
                        values: newValues,
                        excludingId: id,
                        transaction: transaction
                    )
                }

                let newIndexKey = buildIndexKey(
                    subspace: subspace,
                    values: newValues,
                    id: id
                )
                transaction.setValue([], for: newIndexKey)
            }
        }
    }

    /// Check unique constraint for index
    ///
    /// Throws if another record with the same index value already exists.
    private func checkUniqueConstraint(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        values: [any TupleElement],
        excludingId: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // Create value subspace by appending packed values directly to prefix
        // Note: Don't use subspace.subspace(Tuple(values)) as that treats Tuple as a nested tuple element
        let valueSubspace = Subspace(prefix: subspace.prefix + Tuple(values).pack())
        let (begin, end) = valueSubspace.range()

        let sequence = transaction.getRange(begin: begin, end: end, snapshot: false)

        for try await (key, _) in sequence {
            // Check if this key belongs to a different record
            if let existingId = extractIDFromIndexKey(key, subspace: valueSubspace) {
                // Compare tuples using byte representation for type-safe comparison
                // This avoids issues with String(describing:) treating different types as equal
                let existingBytes = existingId.pack()
                let excludingBytes = excludingId.pack()

                if existingBytes != excludingBytes {
                    // Different ID, unique constraint violation
                    throw FDBIndexError.uniqueConstraintViolation(
                        indexName: descriptor.name,
                        values: values.map { String(describing: $0) }
                    )
                }
            }
        }
    }

    /// Update count index (CountIndexKind)
    ///
    /// Key structure: `[indexSubspace][groupKey] = Int64(count)`
    /// Uses atomic increment/decrement operations.
    private func updateCountIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        transaction: any TransactionProtocol
    ) async throws {
        // Decrement count for old group key
        if let old = oldModel {
            let groupValues = extractIndexValues(from: old, keyPaths: descriptor.keyPaths)
            if !groupValues.isEmpty {
                let key = subspace.pack(Tuple(groupValues))
                // Atomic decrement by -1
                let decrementValue = withUnsafeBytes(of: Int64(-1).littleEndian) { Array($0) }
                transaction.atomicOp(key: key, param: decrementValue, mutationType: .add)
            }
        }

        // Increment count for new group key
        if let new = newModel {
            let groupValues = extractIndexValues(from: new, keyPaths: descriptor.keyPaths)
            if !groupValues.isEmpty {
                let key = subspace.pack(Tuple(groupValues))
                // Atomic increment by +1
                let incrementValue = withUnsafeBytes(of: Int64(1).littleEndian) { Array($0) }
                transaction.atomicOp(key: key, param: incrementValue, mutationType: .add)
            }
        }
    }

    /// Update sum index (SumIndexKind)
    ///
    /// Key structure: `[indexSubspace][groupKey] = Double(sum)`
    /// Uses read-modify-write pattern. Last keyPath is the value field.
    ///
    /// **Note**: FDB's atomic `.add` only supports 64-bit signed integer addition.
    /// Using `Double.bitPattern` with `.add` is incorrect because IEEE 754 bit patterns
    /// don't produce correct sums when added as integers.
    /// We use read-modify-write instead, which is correct but not atomic across transactions.
    /// Conflicts are handled by FDB's optimistic concurrency control and automatic retry.
    private func updateSumIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        transaction: any TransactionProtocol
    ) async throws {
        guard descriptor.keyPaths.count >= 2,
              let valueKeyPath = descriptor.keyPaths.last else { return }

        let groupKeyPaths = Array(descriptor.keyPaths.dropLast())

        // Calculate delta: newValue - oldValue
        var oldNumeric: Double = 0.0
        var newNumeric: Double = 0.0
        var groupKey: [UInt8]?

        if let old = oldModel {
            let groupValues = extractIndexValues(from: old, keyPaths: groupKeyPaths)
            let valueValues = extractIndexValues(from: old, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, let oldValue = valueValues.first {
                groupKey = subspace.pack(Tuple(groupValues))
                oldNumeric = toDouble(oldValue) ?? 0.0
            }
        }

        if let new = newModel {
            let groupValues = extractIndexValues(from: new, keyPaths: groupKeyPaths)
            let valueValues = extractIndexValues(from: new, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, let newValue = valueValues.first {
                groupKey = subspace.pack(Tuple(groupValues))
                newNumeric = toDouble(newValue) ?? 0.0
            }
        }

        // Apply delta using read-modify-write
        guard let key = groupKey else { return }

        let delta = newNumeric - oldNumeric
        if delta == 0.0 { return }

        // Read current sum
        let currentBytes = try await transaction.getValue(for: key, snapshot: false)
        var currentSum: Double = 0.0
        if let bytes = currentBytes, bytes.count == 8 {
            let bitPattern = bytes.withUnsafeBytes { $0.load(as: UInt64.self) }
            currentSum = Double(bitPattern: UInt64(littleEndian: bitPattern))
        }

        // Write new sum
        let newSum = currentSum + delta
        let newSumBytes = withUnsafeBytes(of: newSum.bitPattern.littleEndian) { Array($0) }
        transaction.setValue(newSumBytes, for: key)
    }

    /// Convert TupleElement to Double for sum operations
    private func toDouble(_ value: any TupleElement) -> Double? {
        switch value {
        case let v as Int64: return Double(v)
        case let v as Double: return v
        case let v as Int: return Double(v)
        case let v as Float: return Double(v)
        default: return nil
        }
    }

    /// Update min/max index (MinIndexKind, MaxIndexKind)
    ///
    /// Key structure: `[indexSubspace][groupKey][value][primaryKey] = ''`
    /// Stores sorted values for efficient min/max queries.
    private func updateMinMaxIndex<T: Persistable>(
        descriptor: IndexDescriptor,
        subspace: Subspace,
        oldModel: T?,
        newModel: T?,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        guard descriptor.keyPaths.count >= 2,
              let valueKeyPath = descriptor.keyPaths.last else { return }

        let groupKeyPaths = Array(descriptor.keyPaths.dropLast())

        // Remove old entry
        if let old = oldModel {
            let groupValues = extractIndexValues(from: old, keyPaths: groupKeyPaths)
            let valueValues = extractIndexValues(from: old, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, !valueValues.isEmpty {
                var keyElements: [any TupleElement] = groupValues
                keyElements.append(contentsOf: valueValues)
                for i in 0..<id.count {
                    if let element = id[i] {
                        keyElements.append(element)
                    }
                }
                let oldKey = subspace.pack(Tuple(keyElements))
                transaction.clear(key: oldKey)
            }
        }

        // Add new entry
        if let new = newModel {
            let groupValues = extractIndexValues(from: new, keyPaths: groupKeyPaths)
            let valueValues = extractIndexValues(from: new, keyPaths: [valueKeyPath])

            if !groupValues.isEmpty, !valueValues.isEmpty {
                var keyElements: [any TupleElement] = groupValues
                keyElements.append(contentsOf: valueValues)
                for i in 0..<id.count {
                    if let element = id[i] {
                        keyElements.append(element)
                    }
                }
                let newKey = subspace.pack(Tuple(keyElements))
                transaction.setValue([], for: newKey)
            }
        }
    }

    /// Update indexes for type-erased models (batch operations)
    ///
    /// For Protobuf-serialized data, we use a "clear and re-add" strategy for updates
    /// since Protobuf is not self-describing. This scans for existing index entries
    /// with this ID and clears them before adding new ones.
    ///
    /// **Important**: This path now includes:
    /// - Index state checking (only maintains indexes with `shouldMaintain == true`)
    /// - Unique constraint checking (prevents duplicates, only for maintained indexes)
    /// - Proper cleanup of old index entries on update
    ///
    /// **Index State Handling**:
    /// - Only indexes with `shouldMaintain == true` are updated
    /// - `disabled` indexes are skipped entirely (no maintenance, no unique checks)
    /// - `writeOnly` and `readable` indexes are maintained normally
    ///
    /// - Parameters:
    ///   - oldData: The old record data (for update operations), nil for insert
    ///   - newModel: The new model being saved, nil for delete
    ///   - id: The model's ID tuple
    ///   - transaction: The FDB transaction
    ///   - deletingModel: The model being deleted (for delete operations)
    private func updateIndexesUntyped(
        oldData: [UInt8]?,
        newModel: (any Persistable)?,
        id: Tuple,
        transaction: any TransactionProtocol,
        deletingModel: (any Persistable)? = nil
    ) async throws {
        // Determine which model type we're working with
        let modelType: any Persistable.Type
        if let newModel = newModel {
            modelType = type(of: newModel)
        } else if let deletingModel = deletingModel {
            modelType = type(of: deletingModel)
        } else {
            return  // No model to process
        }

        let indexDescriptors = modelType.indexDescriptors
        guard !indexDescriptors.isEmpty else { return }

        // Batch fetch all index states for performance
        let indexNames = indexDescriptors.map(\.name)
        let indexStates = try await indexStateManager.states(of: indexNames, transaction: transaction)

        for descriptor in indexDescriptors {
            // Check if index should be maintained based on its state
            let state = indexStates[descriptor.name] ?? .disabled
            guard state.shouldMaintain else {
                logger.trace("Skipping index '\(descriptor.name)' maintenance (state: \(state))")
                continue
            }

            let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)

            // For update operations (oldData exists), clear existing index entries for this ID
            // Since Protobuf is not self-describing, we scan the index to find entries with this ID
            if oldData != nil {
                try await clearIndexEntriesForId(
                    indexSubspace: indexSubspaceForIndex,
                    id: id,
                    transaction: transaction
                )
            }

            // For delete operations, extract values from the model being deleted
            if let deletingModel = deletingModel {
                let oldValues = extractIndexValuesUntyped(from: deletingModel, keyPaths: descriptor.keyPaths)
                if !oldValues.isEmpty {
                    let oldIndexKey = buildIndexKey(
                        subspace: indexSubspaceForIndex,
                        values: oldValues,
                        id: id
                    )
                    transaction.clear(key: oldIndexKey)
                }
            }

            // Add new index entries
            if let newModel = newModel {
                let newValues = extractIndexValuesUntyped(from: newModel, keyPaths: descriptor.keyPaths)
                if !newValues.isEmpty {
                    // Check unique constraint before adding
                    if descriptor.isUnique {
                        try await checkUniqueConstraint(
                            descriptor: descriptor,
                            subspace: indexSubspaceForIndex,
                            values: newValues,
                            excludingId: id,
                            transaction: transaction
                        )
                    }

                    let newIndexKey = buildIndexKey(
                        subspace: indexSubspaceForIndex,
                        values: newValues,
                        id: id
                    )
                    transaction.setValue([], for: newIndexKey)
                }
            }
        }
    }

    /// Clear all index entries for a given ID by scanning the index
    ///
    /// This is used in the type-erased path where we can't deserialize the old values.
    /// We scan the index subspace and clear any entries that end with the given ID.
    ///
    /// **Performance note**: This is O(n) where n is the number of index entries.
    /// For better performance, use the typed path which can extract old values directly.
    ///
    /// - Parameters:
    ///   - indexSubspace: The subspace for this specific index
    ///   - id: The model's ID tuple to match
    ///   - transaction: The FDB transaction
    private func clearIndexEntriesForId(
        indexSubspace: Subspace,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let (begin, end) = indexSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: false)

        for try await (key, _) in sequence {
            // Check if this key ends with the given ID
            if let extractedId = extractIDFromIndexKey(key, subspace: indexSubspace),
               extractedId.pack() == id.pack() {
                transaction.clear(key: key)
            }
        }
    }

    /// Extract index values from a type-erased model
    ///
    /// Uses KeyPath direct extraction for optimal performance.
    /// Swift 5.7+ implicit existential opening allows passing `any Persistable`
    /// directly to generic functions.
    private func extractIndexValuesUntyped(from model: any Persistable, keyPaths: [AnyKeyPath]) -> [any TupleElement] {
        (try? DataAccess.extractFieldsUsingKeyPaths(from: model, keyPaths: keyPaths)) ?? []
    }

    /// Extract index values from a model
    ///
    /// Uses KeyPath direct extraction for optimal performance.
    /// Avoids string-based field lookup and Mirror reflection.
    private func extractIndexValues<T: Persistable>(from model: T, keyPaths: [AnyKeyPath]) -> [any TupleElement] {
        (try? DataAccess.extractFieldsUsingKeyPaths(from: model, keyPaths: keyPaths)) ?? []
    }

    /// Build index key
    private func buildIndexKey(subspace: Subspace, values: [any TupleElement], id: Tuple) -> [UInt8] {
        var elements: [any TupleElement] = values
        for i in 0..<id.count {
            if let element = id[i] {
                elements.append(element)
            }
        }
        return subspace.pack(Tuple(elements))
    }

    // MARK: - Predicate Evaluation

    /// Evaluate a predicate on a model
    private func evaluatePredicate<T: Persistable>(_ predicate: Predicate<T>, on model: T) -> Bool {
        switch predicate {
        case .comparison(let comparison):
            return evaluateFieldComparison(model: model, comparison: comparison)

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

    /// Evaluate a field comparison with type-safe comparisons
    ///
    /// Uses FieldValue-based comparison for consistency with PlanExecutor
    private func evaluateFieldComparison<T: Persistable>(
        model: T,
        comparison: FieldComparison<T>
    ) -> Bool {
        let fieldName = comparison.fieldName
        let expectedValue = comparison.value

        // Handle nil checks separately
        switch comparison.op {
        case .isNil:
            let fieldValues = try? DataAccess.extractField(from: model, keyPath: fieldName)
            return fieldValues == nil || fieldValues?.isEmpty == true
        case .isNotNil:
            let fieldValues = try? DataAccess.extractField(from: model, keyPath: fieldName)
            return fieldValues != nil && fieldValues?.isEmpty == false
        default:
            break
        }

        guard let fieldValues = try? DataAccess.extractField(from: model, keyPath: fieldName),
              let rawFieldValue = fieldValues.first else {
            return false
        }

        // Convert model field value to FieldValue for type-safe comparison
        let modelFieldValue = FieldValue(rawFieldValue) ?? .null

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
            if let fieldStr = rawFieldValue as? String, let substr = expectedValue.stringValue {
                return fieldStr.contains(substr)
            }
            return false
        case .hasPrefix:
            if let fieldStr = rawFieldValue as? String, let prefix = expectedValue.stringValue {
                return fieldStr.hasPrefix(prefix)
            }
            return false
        case .hasSuffix:
            if let fieldStr = rawFieldValue as? String, let suffix = expectedValue.stringValue {
                return fieldStr.hasSuffix(suffix)
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

    /// Compare two models by a field with type-safe comparison
    private func compareModels<T: Persistable>(_ lhs: T, _ rhs: T, by fieldName: String) -> ComparisonResult {
        let lhsValues = try? DataAccess.extractField(from: lhs, keyPath: fieldName)
        let rhsValues = try? DataAccess.extractField(from: rhs, keyPath: fieldName)

        guard let lhsRaw = lhsValues?.first,
              let rhsRaw = rhsValues?.first else {
            return .orderedSame
        }

        // Use FieldValue for consistent comparison
        let lhsField = FieldValue(lhsRaw) ?? .null
        let rhsField = FieldValue(rhsRaw) ?? .null

        return lhsField.compare(to: rhsField) ?? .orderedSame
    }

    // MARK: - Clear Operations

    /// Clear all records of a type
    func clearAll<T: Persistable>(_ type: T.Type) async throws {
        try await database.withTransaction { transaction in
            try transaction.setOption(forOption: .priorityBatch)
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
