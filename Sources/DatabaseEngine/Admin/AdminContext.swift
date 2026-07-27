#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Administrative operations for statistics, query analysis, and indexes.
///
/// **Usage**:
/// ```swift
/// let admin = container.newAdminContext()
///
/// // Collection statistics
/// let stats = try await admin.collectionStatistics(User.self)
///
/// // Query analysis
/// let plan = try await admin.explain(
///     Query<User>().where(User.fields.age > 18)
/// )
/// ```
public final class AdminContext: AdminContextProtocol, Sendable {
    // MARK: - Properties

    private let container: DBContainer

    // MARK: - Initialization

    public init(container: DBContainer) {
        self.container = container
    }

    // MARK: - Private: Index State

    /// Get index build state from IndexLifecycleStore
    ///
    /// Uses the entity's directory subspace for index state storage,
    /// consistent with DatabaseDataStore and DBContainer.ensureIndexesReady().
    ///
    /// - Parameters:
    ///   - indexName: Name of the index
    ///   - entitySubspace: The entity's resolved directory subspace
    private func getIndexBuildState(
        _ indexName: String,
        entitySubspace: Subspace
    ) async throws -> AdminIndexState {
        let indexLifecycleStore = IndexLifecycleStore(container: container, subspace: entitySubspace)
        let internalState = try await indexLifecycleStore.state(of: indexName)

        switch internalState {
        case .readable:
            return .ready
        case .writeOnly:
            return .building
        case .disabled:
            return .disabled
        }
    }

    // MARK: - Collection Statistics

    public func collectionStatistics<T: Persistable>(
        _ type: T.Type
    ) async throws -> AdminCollectionStatistics {
        let subspace = try await container.resolveDirectory(for: type)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(T.persistableType)
        let (begin, end) = itemSubspace.range()

        // Use server-side estimation for size and count
        let (documentCount, storageSize) = try await container.engine.withTransaction(configuration: .batch) { transaction in
            // Get estimated range size
            let sizeBytes = try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )

            // Count documents (sample-based for large collections)
            var count: Int64 = 0
            for _ in try await transaction.collectRange(from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end), snapshot: true) {
                count += 1
                // Limit to avoid timeout
                if count >= 100_000 {
                    break
                }
            }

            return (count, Int64(sizeBytes))
        }

        let avgDocumentSize = documentCount > 0 ? Int(storageSize / documentCount) : 0

        return AdminCollectionStatistics(
            entityName: T.persistableType,
            documentCount: documentCount,
            storageByteCount: storageSize,
            averageDocumentByteCount: avgDocumentSize,
            lastModified: nil,
            keyRangeStart: ByteString(retaining: begin),
            keyRangeEnd: ByteString(retaining: end)
        )
    }

    // MARK: - Index Statistics

    public func indexStatistics(
        _ indexName: String
    ) async throws -> AdminIndexStatistics {
        // Find index descriptor from schema
        guard let indexDescriptor = findIndexDescriptor(name: indexName) else {
            throw AdminError.indexNotFound(indexName)
        }

        // Get the entity that owns this index
        guard let entity = container.schema.entities.first(where: { entity in
            entity.indexDescriptors.contains { $0.name == indexName }
        }) else {
            throw AdminError.indexNotFound(indexName)
        }

        // Resolve directory for the entity
        let subspace = try await resolveDirectoryForEntity(entity)
        let indexSubspace = subspace.subspace(SubspaceKey.indexes).subspace(indexName)
        let (begin, end) = indexSubspace.range()

        // Get index statistics
        let (entryCount, storageSize) = try await container.engine.withTransaction(configuration: .batch) { transaction in
            let sizeBytes = try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )

            var count: Int64 = 0
            for _ in try await transaction.collectRange(from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end), snapshot: true) {
                count += 1
                if count >= 100_000 {
                    break
                }
            }

            return (count, Int64(sizeBytes))
        }

        // Determine index state from IndexLifecycleStore (using entity subspace)
        let state = try await getIndexBuildState(indexName, entitySubspace: subspace)

        return AdminIndexStatistics(
            indexName: indexName,
            kindIdentifier: indexDescriptor.kindIdentifier,
            entryCount: entryCount,
            storageByteCount: storageSize,
            uniqueKeyCount: nil, // Would need HyperLogLog to estimate
            state: state,
            lastUsed: nil,
            usageCount: nil
        )
    }

    public func allIndexStatistics() async throws -> [AdminIndexStatistics] {
        var results: [AdminIndexStatistics] = []

        for entity in container.schema.entities {
            for indexDescriptor in entity.indexDescriptors {
                let stats = try await indexStatistics(indexDescriptor.name)
                results.append(stats)
            }
        }

        return results
    }

    // MARK: - Query Analysis

    public func explain<T: Persistable>(
        _ query: Query<T>
    ) async throws -> AdminQueryPlan {
        let context = container.newContext()
        let accessPlan = try await context.executionPlan(for: query)
        let planKind: AdminQueryPlanKind
        let selectedIndex: String?
        switch accessPlan.accessPath {
        case .fullScan:
            planKind = .tableScan
            selectedIndex = nil
        case .scalarIndex(let name, _, _):
            planKind = .indexScan
            selectedIndex = name
        }

        return AdminQueryPlan(
            kind: planKind,
            selectedIndexName: selectedIndex,
            indexConditions: accessPlan.indexedConditions.map {
                "\($0.fieldName) \($0.comparison) \($0.value)"
            },
            filterConditions: accessPlan.residualFilterRequired
                ? query.predicates.map { describeCondition($0) }
                : [],
            requiresSort: accessPlan.sortRequired
        )
    }

    public func explainAnalyze<T: Persistable>(
        _ query: Query<T>
    ) async throws -> AdminQueryExecutionStatistics {
        let startTime = MonotonicClock.now()
        let plan = try await explain(query)

        // Execute the query to get actual stats
        let store = try await container.store(for: T.self)
        let results = try await store.fetch(query)

        let elapsedNanoseconds = MonotonicClock.now().uptimeNanoseconds
            - startTime.uptimeNanoseconds
        let duration = try TimeSpan(
            seconds: Int64(elapsedNanoseconds / 1_000_000_000),
            nanoseconds: UInt32(elapsedNanoseconds % 1_000_000_000)
        )

        // Get current read version
        let readVersion = try await currentReadVersion()

        return AdminQueryExecutionStatistics(
            plan: plan,
            actualRowCount: Int64(results.count),
            executionDuration: duration,
            readVersion: readVersion
        )
    }

    // MARK: - Index Management

    /// Rebuild an index from scratch
    ///
    /// This method uses the EntityIndexBuilder to properly rebuild the index
    /// using the correct IndexMaintainer for the index type.
    ///
    /// **Process**:
    /// 1. Disable the index
    /// 2. Clear existing index entries
    /// 3. Enable index (write-only mode)
    /// 4. Scan all items and rebuild index entries via IndexMaintainer
    /// 5. Mark index as readable
    ///
    /// - Parameters:
    ///   - indexName: Name of the index to rebuild
    ///   - progress: Optional progress reporting action (0.0 to 1.0)
    public func rebuildIndex(_ indexName: String, progress: (@Sendable (Double) -> Void)?) async throws {
        // Find the index and its owning entity
        guard let (entity, indexDescriptor) = findEntityAndIndex(name: indexName) else {
            throw AdminError.indexNotFound(indexName)
        }

        progress?(0.05)

        // Resolve directory for the entity
        let subspace = try await resolveDirectoryForEntity(entity)
        let indexSubspace = subspace.subspace(SubspaceKey.indexes)

        // Create IndexLifecycleStore using entity subspace (consistent with DatabaseDataStore)
        let indexLifecycleStore = IndexLifecycleStore(container: container, subspace: subspace)

        progress?(0.1)

        // Step 1: Disable index and clear existing entries atomically
        let indexDataSubspace = indexSubspace.subspace(indexName)
        let indexRange = indexDataSubspace.range()

        try await container.engine.withTransaction(configuration: .batch) { transaction in
            // Disable index (from any state)
            try await indexLifecycleStore.disable(indexName, transaction: transaction)

            // Clear existing index data
            try transaction.clearRange(beginKey: indexRange.begin, endKey: indexRange.end)

            // Enable index (disabled → writeOnly)
            try await indexLifecycleStore.enable(indexName, transaction: transaction)
        }

        progress?(0.2)

        // Step 2: Build Index object from IndexDescriptor
        let index = buildIndex(from: indexDescriptor, persistableType: entity.name)

        // Step 3: Get index configurations from container
        let configs = container.indexConfigurations[indexName] ?? []

        progress?(0.3)

        // Step 4: Build index using EntityIndexBuilder
        // This handles type dispatch and uses OnlineIndexer internally
        guard let persistableType = container.runtimeConfiguration
            .persistableTypes.type(named: entity.name) else {
            throw AdminError.operationFailed(
                "Entity '\(entity.name)' has no Persistable type"
            )
        }

        try await EntityIndexBuilder.buildIndex(
            for: persistableType,
            container: container,
            storeSubspace: subspace,
            index: index,
            indexLifecycleStore: indexLifecycleStore,
            batchSize: 100,
            configurations: configs
        )

        progress?(1.0)
    }

    /// Build Index from IndexDescriptor
    ///
    /// Creates an Index object from an IndexDescriptor for use with IndexMaintainers.
    private func buildIndex(from descriptor: IndexDescriptor, persistableType: String) -> Index {
        let rootExpression = KeyExpressionFactory.from(keyPaths: descriptor.fieldNames)

        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: rootExpression,
            subspaceKey: descriptor.name,
            itemTypes: Set([persistableType]),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
        )
    }

    /// Update statistics for all types in the schema
    ///
    /// Collects PostgreSQL ANALYZE-style statistics for all entities:
    /// - Table row counts and average row sizes
    /// - Per-field cardinality (HyperLogLog++)
    /// - Most Common Values (MCV) lists
    /// - Histograms (equi-depth)
    /// - Null fractions
    ///
    /// **Note**: For complete statistics collection, use the typed version
    /// `updateStatistics(for: Type.self)` for each type. This bulk method
    /// only collects index-level statistics due to type erasure limitations.
    public func updateStatistics() async throws {
        // Get statistics subspace from metadata
        let statisticsSubspace = try await getStatisticsSubspace()
        let statisticsService = QueryStatisticsService(
            container: container,
            subspace: statisticsSubspace,
            configuration: .default
        )

        // Collect index statistics for all indexes
        for entity in container.schema.entities {
            let subspace = try await resolveDirectoryForEntity(entity)
            let indexSubspace = subspace.subspace(SubspaceKey.indexes)

            for indexDescriptor in entity.indexDescriptors {
                let indexDataSubspace = indexSubspace.subspace(indexDescriptor.name)
                try await statisticsService.collectIndexStatistics(
                    index: indexDescriptor,
                    indexSubspace: indexDataSubspace
                )
            }
        }
    }

    /// Update statistics for a specific type
    ///
    /// Implements PostgreSQL ANALYZE-style statistics collection:
    /// 1. Sample entities using reservoir sampling
    /// 2. Build MCV (Most Common Values) list
    /// 3. Build histogram excluding MCV values
    /// 4. Estimate cardinality using HyperLogLog++
    ///
    /// - Parameter type: The Persistable type to analyze
    public func updateStatistics<T: Persistable>(for type: T.Type) async throws {
        // Get statistics subspace from metadata
        let statisticsSubspace = try await getStatisticsSubspace()
        let statisticsService = QueryStatisticsService(
            container: container,
            subspace: statisticsSubspace,
            configuration: .default
        )

        // Get data store for this type
        let dataStore = try await container.store(for: type)

        // Collect statistics through the query-planning statistics service.
        try await statisticsService.collectStatistics(
            for: type,
            using: dataStore,
            sampleRate: nil,
            fields: nil
        )
    }

    /// Get statistics subspace from DirectoryLayer
    private func getStatisticsSubspace() async throws -> Subspace {
        return try await container.engine.createOrOpenDirectory(
            path: ["_metadata", "statistics"]
        )
    }

    // MARK: - FDB-Specific Features

    public func currentReadVersion() async throws -> UInt64 {
        let version: Int64 = try await container.engine.withTransaction(configuration: .batch) { transaction in
            try await transaction.getReadVersion()
        }
        return UInt64(version)
    }

    public func estimatedStorageSize<T: Persistable>(for type: T.Type) async throws -> Int64 {
        let subspace = try await container.resolveDirectory(for: type)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(T.persistableType)
        let (begin, end) = itemSubspace.range()

        let sizeBytes = try await container.engine.withTransaction(configuration: .batch) { transaction in
            try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )
        }

        return Int64(sizeBytes)
    }

    // MARK: - Private Helpers

    private func findIndexDescriptor(name: String) -> IndexDescriptor? {
        for entity in container.schema.entities {
            if let indexDescriptor = entity.indexDescriptors.first(where: { $0.name == name }) {
                return indexDescriptor
            }
        }
        return nil
    }

    private func findEntityAndIndex(name: String) -> (Schema.Entity, IndexDescriptor)? {
        for entity in container.schema.entities {
            if let indexDescriptor = entity.indexDescriptors.first(where: { $0.name == name }) {
                return (entity, indexDescriptor)
            }
        }
        return nil
    }

    private func resolveDirectoryForEntity(_ entity: Schema.Entity) async throws -> Subspace {
        guard let persistableType = container.runtimeConfiguration
            .persistableTypes.type(named: entity.name) else {
            throw AdminError.operationFailed("Entity '\(entity.name)' has no Persistable type")
        }
        // Use container's resolveDirectory to respect #Directory definitions
        return try await container.resolveDirectory(for: persistableType)
    }

    private func describeCondition<T>(_ predicate: Predicate<T>) -> String {
        switch predicate {
        case .comparison(let comparison):
            return "\(comparison.fieldName) \(comparison.op) \(comparison.value)"
        case .and(let predicates):
            return "AND(\(predicates.count) conditions)"
        case .or(let predicates):
            return "OR(\(predicates.count) conditions)"
        case .not(let inner):
            return "NOT(\(describeCondition(inner)))"
        case .true:
            return "TRUE"
        case .false:
            return "FALSE"
        }
    }
}

// MARK: - Admin Errors

public enum AdminError: Error, Sendable {
    case indexNotFound(String)
    case entityNotFound(String)
    case operationFailed(String)
}

extension AdminError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .indexNotFound(let name):
            return "Index not found: \(name)"
        case .entityNotFound(let name):
            return "Entity not found: \(name)"
        case .operationFailed(let message):
            return "Operation failed: \(message)"
        }
    }
}
