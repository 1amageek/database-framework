#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes
import StorageKit

// This SPI is the execution-facing boundary for the optional MultiBase
// runtime. Hosts may orchestrate operations and durable jobs, while catalog
// ownership, placement state, leases, and lifecycle transitions remain inside
// DatabaseEngine.
extension DBContainer {
    @_spi(DatabaseExecution)
    public func executionAcquireBaseLease(
        _ id: Base.ID,
        permitsInactiveMaintenance: Bool,
    ) throws -> DatabaseBaseLease {
        permitsInactiveMaintenance
            ? try acquireBaseAdministrationLease(id)
            : try acquireBaseLease(id)
    }

    @_spi(DatabaseExecution)
    public func executionAcquireBaseSchemaMaintenanceLease(
        _ id: Base.ID
    ) throws -> DatabaseBaseLease {
        try acquireBaseSchemaMaintenanceLease(id)
    }

    @_spi(DatabaseExecution)
    public func executionWithBaseLease<Result: Sendable>(
        _ lease: DatabaseBaseLease,
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await withBaseLease(lease, operation)
    }

    @_spi(DatabaseExecution)
    public var executionDatabaseGrantStore: DatabaseGrantStore {
        databaseGrantStore
    }

    @_spi(DatabaseExecution)
    public var executionDefaultBasePlacementID: Base.Placement.ID {
        storageTopology.defaultPlacementID
    }

    @_spi(DatabaseExecution)
    public func executionBasePlacementIDs() -> [Base.Placement.ID] {
        storageTopology.placements.keys.sorted()
    }

    @_spi(DatabaseExecution)
    public func executionRequireBasePlacement(
        _ id: Base.Placement.ID
    ) throws {
        guard storageTopology.placement(identifiedBy: id) != nil else {
            throw DatabaseBaseCatalogError.placementNotFound(id)
        }
    }

    @_spi(DatabaseExecution)
    public func executionLoadBaseRecords(
        transaction: any TransactionAccess
    ) async throws -> [DatabaseBaseRecord] {
        try await baseCatalog.loadAll(transaction: transaction)
    }

    @_spi(DatabaseExecution)
    public func executionLoadBaseRecord(
        _ id: Base.ID,
        transaction: any TransactionAccess
    ) async throws -> DatabaseBaseRecord? {
        try await baseCatalog.load(id, transaction: transaction)
    }

    @_spi(DatabaseExecution)
    public func executionProvisionBaseRecord(
        _ id: Base.ID,
        placementID: Base.Placement.ID,
        initialGrants: [Security.Grant],
        expectedRevision: UInt64
    ) async throws -> DatabaseBaseRecord {
        try await provisionBase(
            id,
            placementID: placementID,
            initialGrants: initialGrants,
            expectedRevision: expectedRevision
        )
    }

    @_spi(DatabaseExecution)
    public func executionLoadCompositionRecords(
        transaction: any TransactionAccess
    ) async throws -> [DatabaseCompositionRecord] {
        try await compositionCatalog.loadAll(transaction: transaction)
    }

    @_spi(DatabaseExecution)
    public func executionLoadCompositionRecord(
        _ id: Base.Composition.ID,
        transaction: any TransactionAccess
    ) async throws -> DatabaseCompositionRecord? {
        try await compositionCatalog.load(id, transaction: transaction)
    }

    @_spi(DatabaseExecution)
    public func executionCreateCompositionRecord(
        _ composition: Base.Composition,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> DatabaseCompositionRecord {
        try await requireActiveCompositionMembers(
            composition.bases,
            transaction: transaction
        )
        return try await compositionCatalog.create(
            composition,
            expectedRevision: expectedRevision,
            transaction: transaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionReplaceCompositionRecord(
        id: Base.Composition.ID,
        bases: [Base.ID],
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> DatabaseCompositionRecord {
        try await requireActiveCompositionMembers(
            bases,
            transaction: transaction
        )
        return try await compositionCatalog.replace(
            id: id,
            bases: bases,
            expectedRevision: expectedRevision,
            transaction: transaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionDeleteCompositionRecord(
        _ id: Base.Composition.ID,
        expectedRevision: UInt64,
        transaction: any TransactionAccess
    ) async throws -> (revision: UInt64, generation: UInt64) {
        try await compositionCatalog.delete(
            id,
            expectedRevision: expectedRevision,
            transaction: transaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionWithBaseAdministrationTransaction<
        Result: Sendable
    >(
        baseID: Base.ID,
        requiredAccess: Security.Access,
        authorization: AuthorizationContext,
        configuration: TransactionConfiguration = .default,
        executionDeadline: TransactionExecutionDeadline? = nil,
        _ operation: @Sendable @escaping (
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        let lease = try acquireBaseAdministrationLease(baseID)
        return try await withBaseLease(lease) {
            try await self.withBaseAdministrationTransaction(
                requiredAccess: requiredAccess,
                authorization: authorization,
                configuration: configuration,
                executionDeadline: executionDeadline,
                operation
            )
        }
    }

    @_spi(DatabaseExecution)
    public func executionBoundBaseGrantStore(
        expectedBaseID: Base.ID
    ) throws -> DatabaseGrantStore {
        let lease = try requireBoundBaseLease()
        guard lease.baseID == expectedBaseID else {
            throw DatabaseBaseCatalogError.baseNotFound(expectedBaseID)
        }
        return DatabaseGrantStore(
            resource: .base(expectedBaseID),
            root: lease.root
        )
    }

    @_spi(DatabaseExecution)
    public func executionBoundBaseMetadataSubspace(
        expectedBaseID: Base.ID,
        component: String
    ) throws -> Subspace {
        let lease = try requireBoundBaseLease()
        guard lease.baseID == expectedBaseID else {
            throw DatabaseBaseCatalogError.baseNotFound(expectedBaseID)
        }
        return lease.root.subspace("_metadata").subspace(component)
    }

    @_spi(DatabaseExecution)
    public func executionBoundBaseMatchesControlDomain(
        _ id: Base.ID
    ) throws -> Bool {
        let lease = try requireBoundBaseLease()
        return lease.baseID == id
            && lease.domainID == controlDomainID.value
    }

    @_spi(DatabaseExecution)
    public func executionLoadBaseRecord(_ id: Base.ID) async throws
        -> DatabaseBaseRecord {
        try await withControlMetadataTransaction(configuration: .readOnly) {
            transaction in
            guard let record = try await self.baseCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseBaseCatalogError.baseNotFound(id)
            }
            return record
        }
    }

    @_spi(DatabaseExecution)
    public func executionRetireBaseRecord(
        _ id: Base.ID,
        expectedRevision: UInt64
    ) async throws -> DatabaseBaseRecord {
        try await retireBase(id, expectedRevision: expectedRevision)
    }

    @_spi(DatabaseExecution)
    public func executionActivateBaseRecord(
        _ id: Base.ID,
        expectedRevision: UInt64,
        authorization: AuthorizationContext
    ) async throws -> DatabaseBaseRecord {
        try await activateBase(
            id,
            expectedRevision: expectedRevision,
            authorization: authorization
        )
    }

    @_spi(DatabaseExecution)
    public func executionPrepareBaseDeletion(
        _ id: Base.ID,
        expectedRevision: UInt64,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        try await prepareBaseDeletion(
            id,
            expectedRevision: expectedRevision,
            owner: owner
        )
    }

    @_spi(DatabaseExecution)
    public func executionClearBaseForDeletion(
        _ id: Base.ID,
        owner: ByteString,
        authorization: AuthorizationContext
    ) async throws -> DatabaseBaseRecord {
        try await clearBaseForDeletion(
            id,
            owner: owner,
            authorization: authorization
        )
    }

    @_spi(DatabaseExecution)
    public func executionFinishBaseDeletion(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        try await finishBaseDeletion(id, owner: owner)
    }

    @_spi(DatabaseExecution)
    public func executionPrepareUnsuccessfulBaseDeletionRecovery(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        try await prepareUnsuccessfulBaseDeletionRecovery(id, owner: owner)
    }

    @_spi(DatabaseExecution)
    public func executionFinalizeSuccessfulBaseDeletion(
        _ id: Base.ID,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        try await finalizeSuccessfulBaseDeletion(
            id,
            owner: owner,
            controlTransaction: controlTransaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionFinalizeUnsuccessfulBaseDeletion(
        _ id: Base.ID,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        try await finalizeUnsuccessfulBaseDeletion(
            id,
            owner: owner,
            controlTransaction: controlTransaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionPermitsBaseDeletionFinalization(
        _ id: Base.ID,
        owner: ByteString
    ) async throws -> Bool {
        try await permitsBaseDeletionFinalization(id, owner: owner)
    }

    @_spi(DatabaseExecution)
    public func executionPrepareBasePlacementMove(
        _ id: Base.ID,
        destinationPlacementID: Base.Placement.ID,
        expectedRevision: UInt64,
        owner: ByteString
    ) async throws -> DatabaseBasePlacementMoveDescriptor {
        try await prepareBasePlacementMove(
            id,
            destinationPlacementID: destinationPlacementID,
            expectedRevision: expectedRevision,
            owner: owner
        )
    }

    @_spi(DatabaseExecution)
    public func executionCopyBasePlacementBatch(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        continuation: ByteString?,
        digest: ByteString?,
        keyCount: UInt64,
        byteCount: UInt64
    ) async throws -> DatabaseBasePlacementTransferProgress {
        try await copyBasePlacementBatch(
            descriptor,
            continuation: continuation,
            digest: digest,
            keyCount: keyCount,
            byteCount: byteCount
        )
    }

    @_spi(DatabaseExecution)
    public func executionVerifyBasePlacementBatch(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        destination: Bool,
        continuation: ByteString?,
        digest: ByteString?,
        keyCount: UInt64,
        byteCount: UInt64
    ) async throws -> DatabaseBasePlacementTransferProgress {
        try await verifyBasePlacementBatch(
            descriptor,
            destination: destination,
            continuation: continuation,
            digest: digest,
            keyCount: keyCount,
            byteCount: byteCount
        )
    }

    @_spi(DatabaseExecution)
    public func executionCutOverBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor
    ) async throws -> DatabaseBaseRecord {
        try await cutOverBasePlacementMove(descriptor)
    }

    @_spi(DatabaseExecution)
    public func executionFinishBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        try await finishBasePlacementMove(descriptor, owner: owner)
    }

    @_spi(DatabaseExecution)
    public func executionPrepareUnsuccessfulBasePlacementMoveRecovery(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString
    ) async throws -> DatabaseBaseRecord {
        try await prepareUnsuccessfulBasePlacementMoveRecovery(
            descriptor,
            owner: owner
        )
    }

    @_spi(DatabaseExecution)
    public func executionFinalizeSuccessfulBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        try await finalizeSuccessfulBasePlacementMove(
            descriptor,
            owner: owner,
            controlTransaction: controlTransaction
        )
    }

    @_spi(DatabaseExecution)
    public func executionFinalizeUnsuccessfulBasePlacementMove(
        _ descriptor: DatabaseBasePlacementMoveDescriptor,
        owner: ByteString,
        controlTransaction: any TransactionAccess
    ) async throws {
        try await finalizeUnsuccessfulBasePlacementMove(
            descriptor,
            owner: owner,
            controlTransaction: controlTransaction
        )
    }

    private func requireActiveCompositionMembers(
        _ bases: [Base.ID],
        transaction: any TransactionAccess
    ) async throws {
        for baseID in bases {
            guard let record = try await baseCatalog.load(
                baseID,
                transaction: transaction
            ) else {
                throw DatabaseCompositionCatalogError.memberBaseNotFound(baseID)
            }
            guard record.lifecycle == .active else {
                throw DatabaseCompositionCatalogError.memberBaseNotActive(baseID)
            }
        }
    }
}
#endif
