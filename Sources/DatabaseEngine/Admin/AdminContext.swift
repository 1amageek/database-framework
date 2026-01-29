import Foundation
import Core
import FoundationDB

// Type aliases to match protocol (avoiding naming conflicts with internal types)
// These must match the typealiases in AdminContextProtocol
public typealias PublicPlanType = Core.PlanType
public typealias PublicIndexBuildState = Core.IndexBuildState

/// AdminContext - 管理操作の統合実装
///
/// StatisticsManager、QueryPlanner、WatchManagerなどの内部コンポーネントを
/// 統一されたAPIで公開する。
///
/// **Usage**:
/// ```swift
/// let admin = container.newAdminContext()
///
/// // 統計情報
/// let stats = try await admin.collectionStatistics(User.self)
///
/// // クエリ分析
/// let plan = try await admin.explain(Query<User>().where(\.age > 18))
///
/// // 変更監視
/// for await event in admin.watch(User.self, id: userId) {
///     // ...
/// }
/// ```
public final class AdminContext: AdminContextProtocol, Sendable {
    // MARK: - Properties

    private let container: FDBContainer
    private let watchManager: WatchManager

    // MARK: - Initialization

    public init(container: FDBContainer) {
        self.container = container
        self.watchManager = WatchManager(container: container)
    }

    // MARK: - Private: Metadata Subspace

    /// Get metadata subspace for index state storage using DirectoryLayer
    private func getMetadataSubspace() async throws -> Subspace {
        let directoryLayer = DirectoryLayer(database: container.database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: ["_metadata"])
        return dirSubspace.subspace.subspace("index")
    }

    /// Get index build state from IndexStateManager
    ///
    /// Converts internal IndexState to public IndexBuildState
    private func getIndexBuildState(_ indexName: String) async throws -> PublicIndexBuildState {
        let indexSubspace = try await getMetadataSubspace()
        let indexStateManager = IndexStateManager(container: container, subspace: indexSubspace)
        let internalState = try await indexStateManager.state(of: indexName)

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

    public func collectionStatistics<T: Persistable>(_ type: T.Type) async throws -> CollectionStatisticsPublic {
        let subspace = try await container.resolveDirectory(for: type)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(T.persistableType)
        let (begin, end) = itemSubspace.range()

        // Use server-side estimation for size and count
        let (documentCount, storageSize) = try await container.database.withTransaction(configuration: .batch) { transaction in
            // Get estimated range size
            let sizeBytes = try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )

            // Count documents (sample-based for large collections)
            var count: Int64 = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
                // Limit to avoid timeout
                if count >= 100_000 {
                    break
                }
            }

            return (count, Int64(sizeBytes))
        }

        let avgDocumentSize = documentCount > 0 ? Int(storageSize / documentCount) : 0

        return CollectionStatisticsPublic(
            typeName: T.persistableType,
            documentCount: documentCount,
            storageSize: storageSize,
            avgDocumentSize: avgDocumentSize,
            lastModified: nil,
            keyRangeStart: begin,
            keyRangeEnd: end
        )
    }

    // MARK: - Index Statistics

    public func indexStatistics(_ indexName: String) async throws -> IndexStatisticsPublic {
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
        let (entryCount, storageSize) = try await container.database.withTransaction(configuration: .batch) { transaction in
            let sizeBytes = try await transaction.getEstimatedRangeSizeBytes(
                beginKey: begin,
                endKey: end
            )

            var count: Int64 = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
                if count >= 100_000 {
                    break
                }
            }

            return (count, Int64(sizeBytes))
        }

        // Determine index state from IndexStateManager
        let state = try await getIndexBuildState(indexName)

        return IndexStatisticsPublic(
            indexName: indexName,
            kind: indexDescriptor.kindIdentifier,
            entryCount: entryCount,
            storageSize: storageSize,
            uniqueKeyCount: nil, // Would need HyperLogLog to estimate
            state: state,
            lastUsed: nil,
            usageCount: nil
        )
    }

    public func allIndexStatistics() async throws -> [IndexStatisticsPublic] {
        var results: [IndexStatisticsPublic] = []

        for entity in container.schema.entities {
            for indexDescriptor in entity.indexDescriptors {
                do {
                    let stats = try await indexStatistics(indexDescriptor.name)
                    results.append(stats)
                } catch {
                    // Skip indexes that fail to load
                    continue
                }
            }
        }

        return results
    }

    // MARK: - Query Analysis

    public func explain<T: Persistable>(_ query: Query<T>) async throws -> QueryPlanPublic {
        // Get indexes for the type
        guard let entity = container.schema.entities.first(where: { $0.name == T.persistableType }) else {
            // No indexes, return table scan plan
            return QueryPlanPublic(
                planType: .tableScan,
                selectedIndex: nil,
                estimatedCost: Double.infinity,
                estimatedRows: 0,
                indexConditions: [],
                filterConditions: query.predicates.map { describeCondition($0) },
                sortRequired: !query.sortDescriptors.isEmpty,
                alternatives: nil
            )
        }

        let indexes = entity.indexDescriptors
        let planner = QueryPlanner<T>(indexes: indexes)

        do {
            let internalPlan = try planner.plan(query: query)
            return convertToPublicPlan(internalPlan)
        } catch {
            // Fallback to table scan
            return QueryPlanPublic(
                planType: .tableScan,
                selectedIndex: nil,
                estimatedCost: Double.infinity,
                estimatedRows: 0,
                indexConditions: [],
                filterConditions: query.predicates.map { describeCondition($0) },
                sortRequired: !query.sortDescriptors.isEmpty,
                alternatives: nil
            )
        }
    }

    public func explainAnalyze<T: Persistable>(_ query: Query<T>) async throws -> QueryExecutionStatsPublic {
        let startTime = CFAbsoluteTimeGetCurrent()
        let plan = try await explain(query)

        // Execute the query to get actual stats
        let store = try await container.store(for: T.self)
        let results = try await store.fetch(query)

        let endTime = CFAbsoluteTimeGetCurrent()
        let executionTime = endTime - startTime

        // Get current read version
        let readVersion = try await currentReadVersion()

        return QueryExecutionStatsPublic(
            plan: plan,
            actualRows: Int64(results.count),
            executionTime: executionTime,
            bytesRead: 0, // Would need instrumentation to track
            transactionRetries: 0,
            readVersion: readVersion,
            conflictRanges: nil
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
    ///   - progress: Optional progress callback (0.0 to 1.0)
    public func rebuildIndex(_ indexName: String, progress: (@Sendable (Double) -> Void)?) async throws {
        // Find the index and its owning entity
        guard let (entity, indexDescriptor) = findEntityAndIndex(name: indexName) else {
            throw AdminError.indexNotFound(indexName)
        }

        progress?(0.05)

        // Resolve directory for the entity
        let subspace = try await resolveDirectoryForEntity(entity)
        let indexSubspace = subspace.subspace(SubspaceKey.indexes)
        let metadataSubspace = try await getMetadataSubspace()

        // Create IndexStateManager
        let indexStateManager = IndexStateManager(container: container, subspace: metadataSubspace)

        progress?(0.1)

        // Step 1: Disable index and clear existing entries atomically
        let indexDataSubspace = indexSubspace.subspace(indexName)
        let indexRange = indexDataSubspace.range()

        try await container.database.withTransaction(configuration: .batch) { transaction in
            // Disable index (from any state)
            try await indexStateManager.disable(indexName, transaction: transaction)

            // Clear existing index data
            transaction.clearRange(beginKey: indexRange.begin, endKey: indexRange.end)

            // Enable index (disabled → writeOnly)
            try await indexStateManager.enable(indexName, transaction: transaction)
        }

        progress?(0.2)

        // Step 2: Build Index object from IndexDescriptor
        let index = buildIndex(from: indexDescriptor, persistableType: entity.name)

        // Step 3: Get index configurations from container
        let configs = container.indexConfigurations[indexName] ?? []

        progress?(0.3)

        // Step 4: Build index using EntityIndexBuilder
        // This handles type dispatch and uses OnlineIndexer internally
        do {
            try await EntityIndexBuilder.buildIndex(
                forPersistableType: entity.persistableType,
                container: container,
                storeSubspace: subspace,
                index: index,
                indexStateManager: indexStateManager,
                batchSize: 100,
                configurations: configs
            )
        } catch EntityIndexBuilderError.entityNotRegistered {
            throw AdminError.operationFailed(
                "Cannot rebuild index '\(indexName)' for entity '\(entity.name)': " +
                "Entity not registered in IndexBuilderRegistry. " +
                "Ensure FDBContainer is created with Schema([YourType.self, ...])"
            )
        } catch EntityIndexBuilderError.typeNotBuildable(_, let reason) {
            throw AdminError.operationFailed("Cannot rebuild index '\(indexName)': \(reason)")
        }

        progress?(1.0)
    }

    /// Build Index from IndexDescriptor
    ///
    /// Creates an Index object from an IndexDescriptor for use with IndexMaintainers.
    private func buildIndex(from descriptor: IndexDescriptor, persistableType: String) -> Index {
        let rootExpression: KeyExpression
        if descriptor.keyPaths.isEmpty {
            rootExpression = EmptyKeyExpression()
        } else {
            let firstKeyPathString = String(describing: descriptor.keyPaths.first!)
            let fieldName = extractFieldName(from: firstKeyPathString)
            rootExpression = FieldKeyExpression(fieldName: fieldName)
        }

        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: rootExpression,
            keyPaths: descriptor.keyPaths,
            subspaceKey: descriptor.name,
            itemTypes: Set([persistableType]),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
        )
    }

    /// Extract field name from keyPath string representation
    private func extractFieldName(from keyPathString: String) -> String {
        if let dotIndex = keyPathString.lastIndex(of: ".") {
            let afterDot = keyPathString[keyPathString.index(after: dotIndex)...]
            if let parenIndex = afterDot.firstIndex(of: "(") {
                return String(afterDot[..<parenIndex])
            }
            return String(afterDot)
        }
        return keyPathString
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
        let manager = StatisticsManager(
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
                try await manager.collectIndexStatistics(
                    index: indexDescriptor,
                    indexSubspace: indexDataSubspace
                )
            }
        }
    }

    /// Update statistics for a specific type
    ///
    /// Implements PostgreSQL ANALYZE-style statistics collection:
    /// 1. Sample records using reservoir sampling
    /// 2. Build MCV (Most Common Values) list
    /// 3. Build histogram excluding MCV values
    /// 4. Estimate cardinality using HyperLogLog++
    ///
    /// - Parameter type: The Persistable type to analyze
    public func updateStatistics<T: Persistable>(for type: T.Type) async throws {
        // Get statistics subspace from metadata
        let statisticsSubspace = try await getStatisticsSubspace()
        let manager = StatisticsManager(
            container: container,
            subspace: statisticsSubspace,
            configuration: .default
        )

        // Get data store for this type
        let dataStore = try await container.store(for: type)

        // Collect statistics using StatisticsManager
        try await manager.collectStatistics(
            for: type,
            using: dataStore,
            sampleRate: nil,
            fields: nil
        )
    }

    /// Get statistics subspace from DirectoryLayer
    private func getStatisticsSubspace() async throws -> Subspace {
        let directoryLayer = DirectoryLayer(database: container.database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: ["_metadata", "statistics"])
        return dirSubspace.subspace
    }

    // MARK: - FDB-Specific Features

    public func watch<T: Persistable>(_ type: T.Type, id: T.ID) -> AsyncStream<WatchEvent<T>> {
        watchManager.watch(type, id: id)
    }

    public func currentReadVersion() async throws -> UInt64 {
        let version: Int64 = try await container.database.withTransaction(configuration: .batch) { transaction in
            try await transaction.getReadVersion()
        }
        return UInt64(version)
    }

    public func estimatedStorageSize<T: Persistable>(for type: T.Type) async throws -> Int64 {
        let subspace = try await container.resolveDirectory(for: type)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(T.persistableType)
        let (begin, end) = itemSubspace.range()

        let sizeBytes = try await container.database.withTransaction(configuration: .batch) { transaction in
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
        // Use container's resolveDirectory to respect #Directory definitions
        return try await container.resolveDirectory(for: entity.persistableType)
    }

    private func convertToPublicPlan<T: Persistable>(_ plan: DatabaseEngine.QueryPlan<T>) -> QueryPlanPublic {
        let planType = determinePlanType(plan.rootOperator)
        let selectedIndex = plan.usedIndexes.first?.name

        return QueryPlanPublic(
            planType: planType,
            selectedIndex: selectedIndex,
            estimatedCost: plan.estimatedCost.totalCost,
            estimatedRows: Int64(plan.estimatedCost.recordFetches),
            indexConditions: extractIndexConditions(plan.rootOperator),
            filterConditions: extractFilterConditions(plan.rootOperator),
            sortRequired: plan.estimatedCost.requiresSort,
            alternatives: nil
        )
    }

    private func determinePlanType<T: Persistable>(_ op: PlanOperator<T>) -> PublicPlanType {
        switch op {
        case .tableScan:
            return .tableScan
        case .indexScan:
            return .indexScan
        case .indexSeek:
            return .indexSeek
        case .indexOnlyScan:
            return .indexOnly
        case .union, .intersection:
            return .multiIndexMerge
        case .filter(let filterOp):
            return determinePlanType(filterOp.input)
        case .sort(let sortOp):
            return determinePlanType(sortOp.input)
        case .limit(let limitOp):
            return determinePlanType(limitOp.input)
        case .project(let projectOp):
            return determinePlanType(projectOp.input)
        default:
            return .tableScan
        }
    }

    private func extractIndexConditions<T: Persistable>(_ op: PlanOperator<T>) -> [String] {
        var conditions: [String] = []

        switch op {
        case .indexScan(let scanOp):
            conditions = scanOp.satisfiedConditions.map { condition in
                let constraintType = condition.isEquality ? "=" : (condition.isRange ? "range" : (condition.isIn ? "IN" : "other"))
                return "\(condition.fieldName) (\(constraintType))"
            }
        case .indexSeek(let seekOp):
            conditions = seekOp.satisfiedConditions.map { condition in
                let constraintType = condition.isEquality ? "=" : (condition.isRange ? "range" : (condition.isIn ? "IN" : "other"))
                return "\(condition.fieldName) (\(constraintType))"
            }
        case .filter(let filterOp):
            conditions = extractIndexConditions(filterOp.input)
        case .sort(let sortOp):
            conditions = extractIndexConditions(sortOp.input)
        case .limit(let limitOp):
            conditions = extractIndexConditions(limitOp.input)
        default:
            break
        }

        return conditions
    }

    private func extractFilterConditions<T: Persistable>(_ op: PlanOperator<T>) -> [String] {
        var conditions: [String] = []

        switch op {
        case .filter(let filterOp):
            conditions.append(describeCondition(filterOp.predicate))
            conditions.append(contentsOf: extractFilterConditions(filterOp.input))
        case .tableScan(let scanOp):
            if let predicate = scanOp.filterPredicate {
                conditions.append(describeCondition(predicate))
            }
        case .sort(let sortOp):
            conditions = extractFilterConditions(sortOp.input)
        case .limit(let limitOp):
            conditions = extractFilterConditions(limitOp.input)
        default:
            break
        }

        return conditions
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
