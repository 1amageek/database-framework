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

        // Determine index state
        let state: PublicIndexBuildState = .ready // TODO: Check actual state from metadata

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

    public func rebuildIndex(_ indexName: String, progress: (@Sendable (Double) -> Void)?) async throws {
        // Find the index and its owning entity
        guard let (entity, _) = findEntityAndIndex(name: indexName) else {
            throw AdminError.indexNotFound(indexName)
        }

        // Resolve directory for the entity
        let subspace = try await resolveDirectoryForEntity(entity)
        let indexSubspace = subspace.subspace(SubspaceKey.indexes).subspace(indexName)
        let itemSubspace = subspace.subspace(SubspaceKey.items).subspace(entity.name)

        // Simple rebuild: clear and re-scan
        // Note: For production use, this should use OnlineIndexer for non-blocking rebuilds
        try await container.database.withTransaction(configuration: .batch) { transaction in
            // Clear existing index entries
            let (indexBegin, indexEnd) = indexSubspace.range()
            transaction.clearRange(beginKey: indexBegin, endKey: indexEnd)
        }

        progress?(0.1)

        // Count items for progress tracking
        let (itemBegin, itemEnd) = itemSubspace.range()

        let itemCount: Int64 = try await container.database.withTransaction(configuration: .batch) { transaction in
            var count: Int64 = 0
            for try await _ in transaction.getRange(begin: itemBegin, end: itemEnd, snapshot: true) {
                count += 1
            }
            return count
        }

        progress?(0.2)

        // Re-index items in batches
        // Note: This is a simplified implementation. Full implementation would use OnlineIndexer
        // with proper IndexMaintainer integration
        let totalItems = itemCount
        var processedCount: Int64 = 0

        try await container.database.withTransaction(configuration: .batch) { [totalItems] transaction in
            for try await (key, value) in transaction.getRange(begin: itemBegin, end: itemEnd, snapshot: true) {
                // Note: Actual indexing would require IndexMaintainer
                // This is a placeholder showing the structure
                _ = key
                _ = value
            }
        }

        // Report final progress
        _ = processedCount
        _ = totalItems
        progress?(1.0)
    }

    public func updateStatistics() async throws {
        // Update statistics for all types - placeholder implementation
        // Would ideally call StatisticsManager.collectStatistics for each type
    }

    public func updateStatistics<T: Persistable>(for type: T.Type) async throws {
        // This would ideally use StatisticsManager.collectStatistics
        // Placeholder implementation
        _ = try await container.store(for: type)
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
        // Use DirectoryLayer to resolve entity's directory
        let directoryLayer = DirectoryLayer(database: container.database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: [entity.name])
        return dirSubspace.subspace
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
