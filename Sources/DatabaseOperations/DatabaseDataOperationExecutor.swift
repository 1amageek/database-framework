import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import GraphIndex
#endif

/// Executes database semantics against one already selected data root.
///
/// The executor has no API for resolving another root. Base lifecycle remains
/// in `BaseOperationExecutor` when the `MultipleBases` trait is enabled.
package final class DatabaseDataOperationExecutor: Sendable {
    package let resource: Security.Resource
    package let authorization: AuthorizationContext
    package let container: DBContainer
    private let dataContext: DatabaseContext

    package init(
        resource: Security.Resource,
        container: DBContainer,
        authorization: AuthorizationContext,
        dataContext: DatabaseContext
    ) {
        self.resource = resource
        self.container = container
        self.authorization = authorization
        self.dataContext = dataContext
    }

    package var monotonicClock: any StorageMonotonicClock {
        container.monotonicClock
    }

    package var wallClock: any WallClock { container.wallClock }
    package var schema: Schema { container.schema }
    package var schemaGeneration: UInt64 { container.schemaGeneration }
    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        container.runtimeConfiguration
    }
    package var containerIdentity: ObjectIdentifier {
        ObjectIdentifier(container)
    }

    package func requireDataContext() -> DatabaseContext { dataContext }

    package func withStorageTransaction<Result: Sendable>(
        requiredAccess: Security.Access = .read,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await dataContext.withTransaction(
            requiredAccess: requiredAccess,
            configuration: configuration,
            executionDeadline: executionDeadline
        ) { transaction in
            try await operation(transaction.storageAccess)
        }
    }

    package func withDataTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await dataContext.withTransaction(
            requiredAccess: requiredAccess,
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    /// Executes framework-owned job cleanup against the already bound data
    /// root. The persistent job runner is responsible for validating the exact
    /// job lease before exposing this package-only capability.
    package func withOperationOwnedStorageTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        let lease = try container.requireActiveDataRoot()
        guard lease.resource == resource else {
            throw DatabaseOperationError.dataRootLeaseMismatch
        }
        return try await lease.transactionExecutor.withTransaction(
            configuration: configuration,
            clock: container.monotonicClock,
            operation
        )
    }

    package func makeIndexMaintenanceRuntime()
        -> DatabaseIndexMaintenanceRuntime {
        DatabaseIndexMaintenanceRuntime(container: container)
    }

    package func pendingSchemaIndexBuilds(
        in schema: Schema,
        transaction: any TransactionAccess
    ) async throws -> [String: Set<String>] {
        try await container.pendingSchemaIndexBuilds(
            in: schema,
            transaction: transaction
        )
    }

    package func partitionCatalogPage(
        entity: String,
        continuation: ByteString?,
        limit: Int,
        transaction: any TransactionAccess
    ) async throws -> DatabasePartitionCatalogPage {
        try await container.partitionCatalogPage(
            entity: entity,
            continuation: continuation,
            limit: limit,
            transaction: transaction
        )
    }

    package func completeSchemaIndexBuild(
        entity: String,
        index: String,
        transaction: any TransactionAccess
    ) throws {
        try container.completeSchemaIndexBuild(
            entity: entity,
            index: index,
            transaction: transaction
        )
    }

    package func installSchemaSnapshot(
        _ schema: Schema,
        transaction: any TransactionAccess
    ) throws {
        try container.installDataRootSchemaSnapshot(
            schema,
            transaction: transaction
        )
    }

    func indexStatusPage(
        entity: String?,
        index: String?,
        partitions: FieldObject,
        continuation: ByteString?,
        budget: ExecutionBudget,
        wireLimits: DatabaseWireLimits,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexStatusTargetPage {
        try await DatabaseIndexStatusPager(
            container: container,
            wireLimits: wireLimits
        ).page(
            entity: entity,
            index: index,
            partitions: partitions,
            continuation: continuation,
            budget: budget,
            transaction: transaction
        )
    }

    package func migrationStatus(
        targetVersion: Schema.Version? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseMigrationStatus {
        try await dataContext.withDataOperation {
            try await self.container.migrationStatus(
                targetVersion: targetVersion,
                transaction: transaction
            )
        }
    }

    package func migrationStatus(
        targetVersion: Schema.Version? = nil
    ) async throws -> DatabaseMigrationStatus {
        try await withStorageTransaction(
            requiredAccess: .administer,
            configuration: .readOnly
        ) { transaction in
            try await self.container.migrationStatus(
                targetVersion: targetVersion,
                transaction: transaction
            )
        }
    }

    package func runMigrations(
        targetVersion: Schema.Version? = nil,
        maximumStageCount: UInt64
    ) async throws -> DatabaseMigrationExecutionResult {
        try await dataContext.withDataOperation {
            try await self.container.runMigrations(
                targetVersion: targetVersion,
                maximumStageCount: maximumStageCount
            )
        }
    }

    package func authorize(_ access: Security.Access) async throws {
        try await withDataTransaction(
            requiredAccess: access,
            configuration: .readOnly
        ) { _ in () }
    }

    package func grantStore() throws -> DatabaseGrantStore {
        let root = try container.requireActiveDataRoot()
        guard root.resource == resource else {
            throw DatabaseAdministrationError.targetMismatch(target)
        }
        return DatabaseGrantStore(resource: resource, root: root.root)
    }

    package func maintenanceCheckpoint(
        for jobID: DatabaseTypes.UUID,
        transaction: any TransactionAccess
    ) async throws -> ByteString? {
        try await transaction.getValue(
            for: try maintenanceCheckpointKey(for: jobID),
            snapshot: false
        )
    }

    package func storeMaintenanceCheckpoint(
        _ value: ByteString,
        for jobID: DatabaseTypes.UUID,
        transaction: any TransactionAccess
    ) throws {
        try transaction.setValue(
            value,
            for: try maintenanceCheckpointKey(for: jobID)
        )
    }

    package func clearMaintenanceCheckpoint(
        for jobID: DatabaseTypes.UUID,
        transaction: any TransactionAccess
    ) throws {
        try transaction.clear(key: maintenanceCheckpointKey(for: jobID))
    }

    private var target: DatabaseOperationTarget {
        switch resource {
        case .database: .database
        case .base(let id): .base(id)
        }
    }

    private func maintenanceCheckpointKey(
        for jobID: DatabaseTypes.UUID
    ) throws -> ByteString {
        let root = try container.requireActiveDataRoot()
        guard root.resource == resource else {
            throw DatabaseAdministrationError.targetMismatch(target)
        }
        return root.root
            .subspace("_metadata")
            .subspace("maintenance-job-checkpoints")
            .pack(Tuple(jobID))
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    package func makeSPARQLQueryExecutor(
        datasetScanner: any RDFDatasetScanner,
        readMode: RDFDatasetReadMode,
        dataset: SPARQLExecutionDataset,
        functionRegistry: SPARQLFunctionRegistry
    ) -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            database: container.engine,
            monotonicClock: container.monotonicClock,
            wallClock: container.wallClock,
            datasetScanner: datasetScanner,
            readMode: readMode,
            dataset: dataset,
            functionRegistry: functionRegistry
        )
    }
    #endif
}
