import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

/// Executes database-scoped control metadata operations. Data-domain access
/// is intentionally absent from this boundary.
package final class DatabaseControlExecutor: Sendable {
    private let container: DBContainer
    package let authorization: AuthorizationContext

    package init(
        container: DBContainer,
        authorization: AuthorizationContext
    ) {
        self.container = container
        self.authorization = authorization
    }

    package var monotonicClock: any StorageMonotonicClock {
        container.monotonicClock
    }

    package var wallClock: any WallClock { container.wallClock }
    package var schema: Schema { container.schema }
    package var schemaFingerprint: SchemaFingerprint {
        container.schemaFingerprint
    }
    package var schemaGeneration: UInt64 { container.schemaGeneration }
    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        container.runtimeConfiguration
    }

    package func withTransaction<Result: Sendable>(
        requiredAccess: Security.Access,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await container.withControlTransaction(
            requiredAccess: requiredAccess,
            authorization: authorization,
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    package func withMetadataTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await container.withControlMetadataTransaction(
            configuration: configuration,
            executionDeadline: executionDeadline,
            operation
        )
    }

    package func validateMutationStateStore(
        _ stateStore: DatabaseMutationStateStore
    ) throws {
        try stateStore.validate(container: container)
    }

    package var defaultPlacementID: Base.Placement.ID {
        container.storageTopology.defaultPlacementID
    }

    package func placementIDs() -> [Base.Placement.ID] {
        container.storageTopology.placements.keys.sorted()
    }

    package func requirePlacement(_ id: Base.Placement.ID) throws {
        guard container.storageTopology.placement(identifiedBy: id) != nil else {
            throw DatabaseBaseCatalogError.placementNotFound(id)
        }
    }

    package var layoutStatus: DatabaseLayoutStatus { container.layoutStatus }
    package var grantStore: DatabaseGrantStore {
        container.databaseGrantStore
    }

    package func loadBases() async throws -> [DatabaseBaseRecord] {
        try await withTransaction(
            requiredAccess: .read,
            configuration: .readOnly
        ) {
            transaction in
            try await self.container.baseCatalog.loadAll(
                transaction: transaction.storageAccess
            )
        }
    }

    package func loadBases(
        transaction: DatabaseTransaction
    ) async throws -> [DatabaseBaseRecord] {
        try await container.baseCatalog.loadAll(
            transaction: transaction.storageAccess
        )
    }

    package func schemaApplication(
        idempotencyKey: String,
        transaction: DatabaseTransaction
    ) async throws -> DatabaseSchemaApplicationRecord? {
        try await DatabaseSchemaApplicationStore(
            metadataSubspace: container.metadataSubspace
        ).load(
            idempotencyKey: idempotencyKey,
            transaction: transaction.storageAccess
        )
    }

    package func insertSchemaApplication(
        _ record: DatabaseSchemaApplicationRecord,
        transaction: DatabaseTransaction
    ) async throws {
        try await DatabaseSchemaApplicationStore(
            metadataSubspace: container.metadataSubspace
        ).insert(record, transaction: transaction.storageAccess)
    }

    package func finishSchemaApplication(
        job: JobIdentity,
        transaction: DatabaseTransaction
    ) async throws {
        try await DatabaseSchemaApplicationStore(
            metadataSubspace: container.metadataSubspace
        ).finish(job: job, transaction: transaction.storageAccess)
    }

    package func makeSchemaTransitionExecutor(
        runtimeFactory: AnyDatabaseSchemaRuntimeFactory
    ) -> DatabaseSchemaTransitionExecutor {
        DatabaseSchemaTransitionExecutor(
            container: container,
            authorization: authorization,
            runtimeFactory: runtimeFactory
        )
    }

    package func loadBase(_ id: Base.ID) async throws -> DatabaseBaseRecord? {
        try await withMetadataTransaction(configuration: .readOnly) {
            transaction in
            try await self.container.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            )
        }
    }

    package func provisionBase(
        _ id: Base.ID,
        placementID: Base.Placement.ID,
        initialGrants: [Security.Grant],
        expectedRevision: UInt64
    ) async throws -> DatabaseBaseRecord {
        try await container.provisionBase(
            id,
            placementID: placementID,
            initialGrants: initialGrants,
            expectedRevision: expectedRevision
        )
    }

    package func legacyLayoutInventory(
        allowCurrentLayout: Bool = false
    ) async throws -> DatabaseLegacyLayoutInventory {
        try await container.legacyLayoutInventory(
            allowCurrentLayout: allowCurrentLayout
        )
    }

    package func legacyLayoutFingerprint(
        inventory: DatabaseLegacyLayoutInventory
    ) async throws -> DatabaseTypes.ByteString {
        try await container.legacyLayoutFingerprint(inventory: inventory)
    }

    package func prepareLegacyMigrationBase(
        _ id: Base.ID,
        placementID: Base.Placement.ID,
        initialGrants: [Security.Grant],
        expectedRevision: UInt64
    ) async throws -> (record: DatabaseBaseRecord, root: Subspace) {
        try await container.prepareLegacyMigrationBase(
            id,
            placementID: placementID,
            initialGrants: initialGrants,
            expectedRevision: expectedRevision
        )
    }

    package func validateLegacyMigrationDestination(
        _ root: Subspace,
        inventory: DatabaseLegacyLayoutInventory
    ) async throws {
        try await container.validateLegacyMigrationDestination(
            root,
            inventory: inventory
        )
    }

    package func legacyMigrationBase(
        _ id: Base.ID
    ) async throws -> (record: DatabaseBaseRecord, root: Subspace) {
        try await container.legacyMigrationBase(id)
    }

    package func rebuildAndCutOverLegacyMigration(
        record: DatabaseBaseRecord,
        root: Subspace
    ) async throws -> DatabaseBaseRecord {
        try await container.rebuildAndCutOverLegacyMigration(
            record: record,
            root: root
        )
    }

    package func cleanupLegacyLayout(
        inventory: DatabaseLegacyLayoutInventory
    ) async throws {
        try await container.cleanupLegacyLayout(inventory: inventory)
    }

    package func abortLegacyMigrationBase(_ id: Base.ID) async throws {
        try await container.abortLegacyMigrationBase(id)
    }

    package func scanLegacyLayoutBatch(
        inventory: DatabaseLegacyLayoutInventory,
        destinationBaseRoot: Subspace?,
        destinationDomainID: DatabaseStorageDomain.ID?,
        mode: DBContainer.LegacyLayoutTransferMode,
        progress: DatabaseLegacyLayoutTransferProgress
    ) async throws -> DatabaseLegacyLayoutTransferProgress {
        try await container.scanLegacyLayoutBatch(
            inventory: inventory,
            destinationBaseRoot: destinationBaseRoot,
            destinationDomainID: destinationDomainID,
            mode: mode,
            progress: progress
        )
    }

    package func visibleCompositions() async throws
        -> [DatabaseCompositionRecord] {
        let records = try await withTransaction(
            requiredAccess: .read,
            configuration: .readOnly
        ) { transaction in
            try await self.container.compositionCatalog.loadAll(
                transaction: transaction.storageAccess
            )
        }
        var visible: [DatabaseCompositionRecord] = []
        visible.reserveCapacity(records.count)
        for record in records {
            do {
                _ = try await container.session(
                    authorization: authorization
                ).composition(record.composition.id).resolve()
                visible.append(record)
            } catch is DatabaseCompositionAccessError {
                continue
            }
        }
        return visible
    }

    package func createComposition(
        _ composition: Base.Composition,
        expectedRevision: UInt64,
        transaction: DatabaseTransaction
    ) async throws -> DatabaseCompositionRecord {
        try await requireActiveMembers(
            composition.bases,
            transaction: transaction
        )
        return try await container.compositionCatalog.create(
            composition,
            expectedRevision: expectedRevision,
            transaction: transaction.storageAccess
        )
    }

    package func replaceComposition(
        id: Base.Composition.ID,
        bases: [Base.ID],
        expectedRevision: UInt64,
        transaction: DatabaseTransaction
    ) async throws -> DatabaseCompositionRecord {
        try await requireActiveMembers(bases, transaction: transaction)
        return try await container.compositionCatalog.replace(
            id: id,
            bases: bases,
            expectedRevision: expectedRevision,
            transaction: transaction.storageAccess
        )
    }

    package func deleteComposition(
        _ id: Base.Composition.ID,
        expectedRevision: UInt64,
        transaction: DatabaseTransaction
    ) async throws -> (revision: UInt64, generation: UInt64) {
        try await container.compositionCatalog.delete(
            id,
            expectedRevision: expectedRevision,
            transaction: transaction.storageAccess
        )
    }

    private func requireActiveMembers(
        _ bases: [Base.ID],
        transaction: DatabaseTransaction
    ) async throws {
        for baseID in bases {
            guard let record = try await container.baseCatalog.load(
                baseID,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseCompositionCatalogError.memberBaseNotFound(baseID)
            }
            guard record.lifecycle == .active else {
                throw DatabaseCompositionCatalogError.memberBaseNotActive(baseID)
            }
        }
    }
}
