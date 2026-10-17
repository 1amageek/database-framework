import DatabaseKit
import DatabaseTypes
import StorageKit

/// Owns database semantics performed within one storage transaction attempt.
///
/// Public operations reject overlapping entry. Derived mutation maintainers
/// receive an operation-scoped capability that permits recursive persistence
/// while preserving this owner, shared storage access, and final validation.
/// `TransactionRunner` alone owns commit, cancellation, and retry lifecycle.
public final actor DatabaseTransaction: DatabaseTransactionWriting, DatabaseTransactionCheckpointing {
    private enum State: Sendable, Equatable {
        case open
        case executing(UInt64)
        case preparingCommit(UInt64)
        case failed
        case closed
    }

    private enum MutationSource: Sendable, Equatable {
        case requested
        case derived
        case sqlAction
    }

    nonisolated package let storageAccess: any TransactionAccess
    nonisolated package let transactionInstant: Timestamp

    @_spi(DatabaseExecution)
    public nonisolated var executionStorageAccess: any TransactionAccess {
        storageAccess
    }

    private let container: DBContainer
    private var readPolicy: DatabaseReadPolicy
    private let readAuthorization: DatabaseReadAuthorization?
    private let mutationMaintenanceService: PersistableMutationMaintenanceService
    private let operationGate = TransactionOperationGate()
    private var validationGate: TransactionOperationGate?
    private var state: State = .open
    private var nextOperationID: UInt64 = 1
    private var subspaceCache = DatabaseStoreCache<ResolvedSubspaces>()
    private var scheduledDeletions = EntityReferenceSet()
    private var scheduledWrites = EntityReferenceSet()
    private var activeMutationIdentities = EntityReferenceSet()
    private var mutationJournal = TransactionMutationJournal()
    private var sqlConstraintModes: [SQLCatalogName: Bool] = [:]
    private var sqlConstraintModeReservation: DatabaseIntermediateReservation?
    private var sqlConstraintsPending = false
    private var sqlConstraintMeter: DatabaseWorkMeter?


    private struct CheckpointRecord: Sendable {
        let checkpoint: DatabaseTransactionCheckpoint
        let undoPosition: Int
        let schema: DatabaseSchemaStagingState?
        let sqlModes: [SQLCatalogName: Bool]
        let sqlModeReservation: DatabaseIntermediateReservation?
        let sqlPending: Bool
    }
    private struct ReadScopeRecord: Sendable {
        let scope: DatabaseTransactionCheckpointScope
        let close: @Sendable () async throws -> Void
        let reservation: DatabaseIntermediateReservation
    }
    private let checkpointGate = TransactionOperationGate(permitsConcurrentOperations: true)
    private var checkpointWorkMeter: DatabaseWorkMeter?
    private var physicalCheckpoints: (any TransactionCheckpointAccess)?
    private var checkpointInitialLease: DatabaseSchemaLease?
    private var checkpointRecords: [CheckpointRecord] = []
    private var checkpointRecordCapacity = 0
    private var checkpointReservation: DatabaseIntermediateReservation?
    private var checkpointTransitionPending = false
    private var nextCheckpointScopeIdentifier: UInt64 = 1
    private var checkpointReadScopes: [ReadScopeRecord] = []
    private var schemaStagingState: DatabaseSchemaStagingState?
    private var checkpointCleanupFailure: (any Error)?
    private var checkpointFailure: (any Error)?
    private var checkpointStatementIsActive = false

    private struct ResolvedSubspaces: Sendable {
        let items: Subspace
        let blobs: Subspace
        let partitionPath: [String]
    }

    @_spi(DatabaseExecution)
    public init(
        storageAccess: any TransactionAccess,
        container: DBContainer,
        authorization: AuthorizationContext,
        transactionInstant: Timestamp? = nil
    ) {
        self.storageAccess = storageAccess
        self.container = container
        self.transactionInstant = transactionInstant ?? container.wallClock.now
        self.readPolicy = DatabaseReadPolicy(
            schemaLease: container.acquireActiveSchemaLease(),
            authorization: authorization
        )
        self.readAuthorization = nil
        self.mutationMaintenanceService =
            PersistableMutationMaintenanceService(
                maintainers: container.runtimeConfiguration
                    .persistableMutationMaintainers
            )
    }

    package init(
        storageAccess: any TransactionAccess,
        container: DBContainer,
        readPolicy: DatabaseReadPolicy,
        readAuthorization: DatabaseReadAuthorization? = nil,
        transactionInstant: Timestamp? = nil
    ) {
        self.storageAccess = storageAccess
        self.container = container
        self.transactionInstant = transactionInstant ?? container.wallClock.now
        self.readPolicy = readPolicy
        self.readAuthorization = readAuthorization
        self.mutationMaintenanceService =
            PersistableMutationMaintenanceService(
                maintainers: container.runtimeConfiguration
                    .persistableMutationMaintainers
            )
    }

    package nonisolated func makeSQLTemporalEnvironment() -> SQLTemporalEnvironment {
        if let binding = ActiveSQLTemporalStatement.binding, binding.container === container {
            return binding.environment
        }
        return SQLTemporalEnvironment(
            transactionInstant: transactionInstant,
            statementInstant: container.wallClock.now,
            wallClock: container.wallClock,
            configuration: container.configuration.sqlTemporal
        )
    }

    // MARK: - Typed reads

    public func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        consistency: TransactionReadConsistency
    ) async throws -> Model? {
        try await performOperation { _ in
            guard let subspaces = try await openSubspaces(for: type) else {
                return nil
            }
            return try await fetchModel(
                of: type,
                identifiedBy: id,
                from: subspaces,
                consistency: consistency
            )
        }
    }

    public func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        in partition: DirectoryPath<Model>,
        consistency: TransactionReadConsistency
    ) async throws -> Model? {
        try await performOperation { _ in
            guard let subspaces = try await openSubspaces(
                for: type,
                in: partition
            ) else {
                return nil
            }
            return try await fetchModel(
                of: type,
                identifiedBy: id,
                from: subspaces,
                consistency: consistency
            )
        }
    }

    public func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy ids: [Model.ID],
        consistency: TransactionReadConsistency = .serializable
    ) async throws -> [Model] {
        try await performOperation { _ in
            guard let subspaces = try await openSubspaces(for: type) else {
                return []
            }
            var models: [Model] = []
            models.reserveCapacity(ids.count)
            for id in ids {
                if let model = try await fetchModel(
                    of: type,
                    identifiedBy: id,
                    from: subspaces,
                    consistency: consistency
                ) {
                    models.append(model)
                }
            }
            return models
        }
    }

    public func scan<Model: Persistable>(
        _ type: Model.Type,
        in partition: DirectoryPath<Model>,
        after continuation: DatabaseScanContinuation?,
        limit: Int,
        consistency: TransactionReadConsistency
    ) async throws -> sending DatabaseScanPage<Model> {
        return try await performOperation { _ in
            guard limit > 0, limit < Int.max else {
                throw DatabaseTransactionError.invalidLimit(limit)
            }
            try readPolicy.authorizeList(
                entityName: Model.persistableType,
                limit: limit,
                offset: nil,
                orderBy: nil
            )
            let runtime = try entityRuntime(named: Model.persistableType)
            guard let subspaces = try await openSubspaces(
                for: type,
                in: partition
            ) else {
                return DatabaseScanPage<Model>(
                    items: [],
                    continuation: nil
                )
            }
            try validate(
                continuation,
                entity: Model.persistableType,
                partitionPath: subspaces.partitionPath
            )

            let typeSubspace = subspaces.items.subspace(
                Model.persistableType
            )
            let (begin, end) = typeSubspace.range()
            let beginSelector: KeySelector
            if let continuation {
                guard continuation.storageKey >= begin,
                      continuation.storageKey < end else {
                    throw DatabaseScanContinuationError.keyOutsideEntityRange
                }
                beginSelector = .firstGreaterThan(continuation.storageKey)
            } else {
                beginSelector = .firstGreaterOrEqual(begin)
            }

            var entries = try await TransactionRangeCollection.collect(
                using: storageAccess,
                from: beginSelector,
                to: .firstGreaterOrEqual(end),
                limit: limit + 1,
                reverse: false,
                snapshot: consistency == .snapshot,
                streamingMode: .iterator
            )
            let hasMore = entries.count > limit
            if hasMore {
                entries.removeLast(entries.count - limit)
            }

            let storage = container.itemStorageFactory.make(
                transaction: storageAccess,
                blobsSubspace: subspaces.blobs
            )
            var models: [Model] = []
            models.reserveCapacity(entries.count)
            for entry in entries {
                guard let bytes = try await storage.read(
                    for: entry.0,
                    snapshot: consistency == .snapshot
                ) else {
                    throw DatabaseTransactionError.itemDisappearedDuringScan
                }
                let persistedModel = try DataAccess.deserializePersistedModel(
                    bytes,
                    expectedEntity: Model.persistableType
                )
                let canonicalModel = try runtime.canonicalized(persistedModel)
                let model = try canonicalModel.decode(as: Model.self)
                try readPolicy.authorizeGet(canonicalModel)
                models.append(model)
            }

            let next = try hasMore ? entries.last.map {
                try DatabaseScanContinuation(
                    entity: Model.persistableType,
                    partitionPath: subspaces.partitionPath,
                    storageKey: $0.0
                )
            } : nil
            return DatabaseScanPage<Model>(
                items: models,
                continuation: next
            )
        }
    }

    // MARK: - Typed mutations

    public func save<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition
    ) async throws {
        try await performOperation { operationID in
            try await persist(
                identity: try EntityReferenceEncoder.encode(model),
                PersistedModel(model),
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    public func delete<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition
    ) async throws {
        try await performOperation { operationID in
            try await remove(
                identity: try EntityReferenceEncoder.encode(model),
                PersistedModel(model),
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    public func delete<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID
    ) async throws {
        try await performOperation { operationID in
            let identity = try EntityReference(
                entity: Model.persistableType,
                id: id.persistableIdentifierValue
            )
            guard let subspaces = try await openSubspaces(for: type),
                  let model = try await fetchModel(
                    of: type,
                    identifiedBy: id,
                    from: subspaces,
                    consistency: .serializable
                  ) else {
                throw DatabaseTransactionError.persistedModelNotFound(
                    identity
                )
            }
            try await remove(
                identity: identity,
                PersistedModel(model),
                precondition: .exists,
                operationID: operationID,
                source: .requested
            )
        }
    }

    public func delete<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        in partition: DirectoryPath<Model>
    ) async throws {
        try await performOperation { operationID in
            let identity = try EntityReference(
                entity: Model.persistableType,
                id: id.persistableIdentifierValue,
                partitions: try AnyDirectoryPath(partition)
                    .canonicalPartitions()
            )
            guard let subspaces = try await openSubspaces(
                for: type,
                in: partition
            ), let model = try await fetchModel(
                of: type,
                identifiedBy: id,
                from: subspaces,
                consistency: .serializable
            ) else {
                throw DatabaseTransactionError.persistedModelNotFound(
                    identity
                )
            }
            try await remove(
                identity: identity,
                PersistedModel(model),
                precondition: .exists,
                operationID: operationID,
                source: .requested
            )
        }
    }

    // MARK: - Package persistence operations

    @_spi(DatabaseExecution)
    public func loadPersistedModel(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?,
        fields: Set<String>? = nil
    ) async throws -> PersistedModel? {
        try await performOperation { _ in
            try await loadPersistedModelUnchecked(
                entity: entity,
                id: id,
                partition: partition,
                fields: fields
            )
        }
    }

    func loadRetainedPersistedModel(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        fields: Set<String>? = nil
    ) async throws -> DatabaseRetainedPersistedModels.Entry? {
        try await performOperation { _ in
            try await loadRetainedPersistedModelUnchecked(
                entity: entity,
                id: id,
                partition: partition,
                snapshot: snapshot,
                workMeter: workMeter,
                fields: fields
            )
        }
    }

    package func fetchPersistedModel(
        identifiedBy identity: EntityReference
    ) async throws -> PersistedModel? {
        try await performOperation { _ in
            let resolved = try resolve(identity)
            return try await loadPersistedModelUnchecked(
                entity: identity.entity,
                id: resolved.id,
                partition: resolved.partition
            )
        }
    }

    package func scanPersistedModels(
        entity: String,
        partition: AnyDirectoryPath?,
        limit: Int,
        offset: Int = 0,
        startingAfterIdentifier: ByteString? = nil,
        workMeter: DatabaseWorkMeter,
        authorizationRequirement: DatabaseListReadAuthorizationRequirement? = nil
    ) async throws -> DatabaseRetainedPersistedModels {
        try await performOperation { _ in
            try await scanPersistedModelsUnchecked(
                entity: entity,
                partition: partition,
                limit: limit,
                offset: offset,
                startingAfterIdentifier: startingAfterIdentifier,
                workMeter: workMeter,
                authorizationRequirement: authorizationRequirement
            )
        }
    }

    package func scanPersistedModelChangesForMutation(
        entity: String,
        partition: AnyDirectoryPath?,
        maximumRows: Int,
        maximumChanges: Int,
        workMeter: DatabaseWorkMeter,
        transform: @Sendable (
            borrowing PersistedModel
        ) async throws -> DatabasePreparedStatementMutationChange?
    ) async throws -> sending DatabaseRetainedEntityMutationScan {
        try await performOperation { _ in
            try await scanPersistedModelChangesForMutationUnchecked(
                entity: entity,
                partition: partition,
                maximumRows: maximumRows,
                maximumChanges: maximumChanges,
                workMeter: workMeter,
                transform: transform
            )
        }
    }

    @_spi(DatabaseExecution)
    public func savePersistedModel(
        _ model: PersistedModel,
        precondition: WritePrecondition
    ) async throws {
        try await performOperation { operationID in
            try await persist(
                identity: try entityRuntime(named: model.entity).identity(
                    for: model
                ),
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    package func deletePersistedModel(
        _ model: PersistedModel,
        precondition: WritePrecondition
    ) async throws {
        try await performOperation { operationID in
            try await remove(
                identity: try entityRuntime(named: model.entity).identity(
                    for: model
                ),
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    @_spi(DatabaseExecution)
    public func apply(
        _ mutations: [PersistableMutation]
    ) async throws {
        try await performOperation { operationID in
            try await apply(
                mutations,
                operationID: operationID
            )
        }
    }

    @_spi(DatabaseExecution)
    public func apply(
        _ mutations: [PersistableMutation],
        workMeter: DatabaseWorkMeter
    ) async throws {
        try await performOperation { operationID in
            try mutationJournal.bind(to: workMeter)
            try await apply(
                mutations,
                operationID: operationID
            )
        }
    }

    package func withEntityMutationOperation<Result: Sendable>(
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable @escaping (UInt64) async throws -> Result
    ) async throws -> Result {
        try await performOperation { operationID in
            try mutationJournal.bind(to: workMeter)
            return try await operation(operationID)
        }
    }

    package func withEntityValidationOperation<Result: Sendable>(
        _ operation: @Sendable @escaping (UInt64) async throws -> Result
    ) async throws -> Result {
        try await performOperation { operationID in
            try await operation(operationID)
        }
    }

    @_spi(DatabaseExecution)
    public func persistedMutationEffectCount() throws -> Int {
        guard state == .open else {
            throw lifecycleError(for: state)
        }
        return mutationJournal.persistedEffectCount
    }

    @_spi(DatabaseExecution)
    public func persistedMutationEffects() throws
        -> [PersistableMutationEffect] {
        guard state == .open else {
            throw lifecycleError(for: state)
        }
        return mutationJournal.persistedEffects()
    }

    package func mapPersistedMutationEffects<Output: Sendable>(
        _ transform: @Sendable (
            PersistableMutationEffect
        ) throws -> Output
    ) throws -> [Output] {
        guard state == .open else {
            throw lifecycleError(for: state)
        }
        return try mutationJournal.mapPersistedEffects(transform)
    }

    package func currentMutationVersion(for identity: EntityReference, within operationID: UInt64) throws -> ByteString? {
        do {
            try ensureActive(operationID, permitsMutation: false)
            return try mutationJournal.currentVersion(for: identity)
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func fetchPersistedModel(
        identifiedBy identity: EntityReference,
        within operationID: UInt64
    ) async throws -> PersistedModel? {
        do {
            try ensureActive(operationID, permitsMutation: false)
            let resolved = try resolve(identity)
            return try await loadPersistedModelUnchecked(
                entity: identity.entity,
                id: resolved.id,
                partition: resolved.partition
            )
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func apply(
        _ mutations: [PersistableMutation],
        within operationID: UInt64
    ) async throws {
        do {
            try ensureActive(operationID, permitsMutation: true)
            try await apply(mutations, operationID: operationID)
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func loadPersistedModel(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?,
        within operationID: UInt64
    ) async throws -> PersistedModel? {
        do {
            try ensureActive(operationID, permitsMutation: false)
            return try await loadPersistedModelUnchecked(
                entity: entity,
                id: id,
                partition: partition
            )
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func persistedMutationEffectCount(
        within operationID: UInt64
    ) throws -> Int {
        do {
            try ensureActive(operationID, permitsMutation: false)
            return mutationJournal.persistedEffectCount
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func mapPersistedMutationEffects<Output: Sendable>(
        within operationID: UInt64,
        _ transform: @Sendable (
            PersistableMutationEffect
        ) throws -> Output
    ) throws -> [Output] {
        do {
            try ensureActive(operationID, permitsMutation: false)
            return try mutationJournal.mapPersistedEffects(transform)
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func savePersistedModel(
        _ model: PersistedModel,
        precondition: WritePrecondition,
        within operationID: UInt64
    ) async throws {
        do {
            try ensureActive(operationID, permitsMutation: true)
            try await persist(
                identity: try entityRuntime(named: model.entity).identity(
                    for: model
                ),
                model,
                precondition: precondition,
                operationID: operationID,
                source: .derived
            )
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func deletePersistedModel(
        _ model: PersistedModel,
        precondition: WritePrecondition,
        within operationID: UInt64
    ) async throws {
        do {
            try ensureActive(operationID, permitsMutation: true)
            try await remove(
                identity: try entityRuntime(named: model.entity).identity(
                    for: model
                ),
                model,
                precondition: precondition,
                operationID: operationID,
                source: .derived
            )
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func isDeletionScheduled(
        for identity: EntityReference,
        within operationID: UInt64
    ) throws -> Bool {
        try ensureActive(operationID, permitsMutation: false)
        return scheduledDeletions.contains(identity)
    }

    package func prepareForCommit() async throws {
        try await drainCheckpointScopes(permanently: true)
        await operationGate.closeAndWait()
        guard state == .open else {
            throw lifecycleError(for: state)
        }
        let operationID = try issueOperationID()
        state = .preparingCommit(operationID)
        let validationGate = TransactionOperationGate()
        self.validationGate = validationGate
        let context = PersistableValidationContext(
            schema: readPolicy.schemaLease.schema,
            transaction: self,
            operationID: operationID,
            operationGate: validationGate
        )
        do {
            if mutationJournal.persistedEffectCount > 0 {
                try await container.requireSQLWriterGeneration(readPolicy.schemaLease.generation, transaction: storageAccess)
            }
            try await validateSQLConstraints(forceDeferred: true)
            if let retainedModels = try mutationJournal.retainedCurrentModels() {
                try await retainedModels.withElements { models in
                    try await mutationMaintenanceService.validateFinalState(
                        of: models,
                        context: context
                    )
                }
            } else {
                try await mutationMaintenanceService.validateFinalState(
                    of: mutationJournal.currentModels(),
                    context: context
                )
            }
            await context.closeAndWait()
            guard state == .preparingCommit(operationID) else {
                throw lifecycleError(for: state)
            }
            self.validationGate = nil
            mutationJournal.removeAll()
            clearCheckpointRecords()
            sqlConstraintModes.removeAll()
            sqlConstraintModeReservation = nil
            sqlConstraintMeter = nil
            state = .closed
        } catch {
            await context.closeAndWait()
            self.validationGate = nil
            mutationJournal.removeAll()
            clearCheckpointRecords()
            sqlConstraintModes.removeAll()
            sqlConstraintModeReservation = nil
            sqlConstraintMeter = nil
            state = .closed
            throw error
        }
    }

    package func invalidate() async {
        state = .closed
        do { try await drainCheckpointScopes(permanently: true) }
        catch { checkpointCleanupFailure = error }
        await operationGate.closeAndWait()
        await validationGate?.closeAndWait()
        mutationJournal.removeAll()
        clearCheckpointRecords()
        schemaStagingState = nil
        sqlConstraintModes.removeAll()
        sqlConstraintModeReservation = nil
        sqlConstraintMeter = nil
    }

    // MARK: - Persistence pipeline

    private func apply(
        _ mutations: [PersistableMutation],
        operationID: UInt64
    ) async throws {
        var identities = EntityReferenceSet(
            minimumCapacity: mutations.count
        )

        for mutation in mutations {
            let identity = mutation.identity
            guard identities.insert(identity) else {
                throw DatabaseTransactionError.duplicateMutation(identity)
            }
            switch mutation {
            case .save:
                scheduledWrites.insert(identity)
            case .delete:
                scheduledDeletions.insert(identity)
            }
        }

        // Apply every proposed final value before delete rules inspect the
        // inverse-reference catalog. This gives delete enforcement the complete
        // batch overlay without a second relationship planner.
        for mutation in mutations {
            guard case .save(let identity, let model, let precondition) = mutation else {
                continue
            }
            try await persist(
                identity: identity,
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
        for mutation in mutations {
            guard case .delete(let identity, let model, let precondition) = mutation else {
                continue
            }
            try await remove(
                identity: identity,
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    private func persist(
        identity: EntityReference,
        _ model: PersistedModel,
        precondition: WritePrecondition,
        operationID: UInt64,
        source: MutationSource,
        dispatchSQLActions: Bool = true
    ) async throws {
        if source == .derived,
           scheduledDeletions.contains(identity) {
            throw DatabaseTransactionError.conflictingDerivedMutation(identity)
        }
        if source == .requested {
            scheduledWrites.insert(identity)
        }
        guard activeMutationIdentities.insert(identity) else {
            throw DatabaseTransactionError.conflictingDerivedMutation(identity)
        }
        defer {
            activeMutationIdentities.remove(identity)
        }
        let resolved = try resolve(identity)
        let store = try await container.store(
            for: resolved.entity,
            path: resolved.partition,
            readPolicy: readPolicy,
            transaction: storageAccess
        )
        let integrityAuthorization = source == .sqlAction ? SQLReferentialActionAuthorization(owner: self,
            operationID: operationID, identity: identity, schemaLease: readPolicy.schemaLease) : nil
        let preparedWrite = try await store.prepareSave(
            model,
            identity: identity,
            precondition: precondition,
            transaction: storageAccess,
            workMeter: mutationJournal.workMeter ?? (source == .sqlAction ? sqlMeter() : nil),
            integrityAuthorization: integrityAuthorization
        )
        defer { preparedWrite.transientReservation?.release() }
        if let relation = readPolicy.sqlCatalog.catalog.relations.first(where: {
            SQLConstraintCompiler.same($0.entityName, preparedWrite.result.canonicalModel.entity)
        }) {
            try await validateSQLRowPolicies(preparedWrite.result.canonicalModel, relation: relation, meter: sqlMeter())
        }
        try updateMutationJournal(
            identity: identity,
            previousModel: preparedWrite.result.previousCanonicalModel,
            currentModel: preparedWrite.result.canonicalModel
        )
        try await store.commitPreparedWrite(
            preparedWrite,
            transaction: storageAccess
        )
        try await PolymorphicProjectionMaintainer(
            container: container
        ).update(
            preparedWrite.result,
            transaction: storageAccess
        )
        let mutationContext = try makeMutationContext(operationID: operationID)
        do {
            try await mutationMaintenanceService.update(
                identity: identity,
                oldModel: preparedWrite.result.previousCanonicalModel,
                newModel: preparedWrite.result.canonicalModel,
                context: mutationContext
            )
            await mutationContext.closeAndWait()
        } catch {
            await mutationContext.closeAndWait()
            throw error
        }
        try ensureActive(operationID, permitsMutation: true)
        try await validateSQLConstraints(forceDeferred: false, rowBoundary: true)
        activeMutationIdentities.remove(identity)
        if dispatchSQLActions {
            try await applySQLReferentialActions(oldModel: preparedWrite.result.previousCanonicalModel,
                newModel: preparedWrite.result.canonicalModel, operationID: operationID)
        }
    }

    private func remove(
        identity: EntityReference,
        _ model: PersistedModel,
        precondition: WritePrecondition,
        operationID: UInt64,
        source: MutationSource,
        dispatchSQLActions: Bool = true
    ) async throws {
        if source == .derived,
           scheduledWrites.contains(identity),
           !scheduledDeletions.contains(identity) {
            throw DatabaseTransactionError.conflictingDerivedMutation(identity)
        }
        if source == .requested {
            scheduledDeletions.insert(identity)
        }
        guard activeMutationIdentities.insert(identity) else {
            throw DatabaseTransactionError.conflictingDerivedMutation(identity)
        }
        defer {
            activeMutationIdentities.remove(identity)
        }
        let resolved = try resolve(identity)
        let store = try await container.store(
            for: resolved.entity,
            path: resolved.partition,
            readPolicy: readPolicy,
            transaction: storageAccess
        )
        let integrityAuthorization = source == .sqlAction ? SQLReferentialActionAuthorization(owner: self,
            operationID: operationID, identity: identity, schemaLease: readPolicy.schemaLease) : nil
        guard let preparedDelete = try await store.prepareDelete(
            model,
            identity: identity,
            precondition: precondition,
            transaction: storageAccess,
            workMeter: mutationJournal.workMeter ?? (source == .sqlAction ? sqlMeter() : nil),
            integrityAuthorization: integrityAuthorization
        ) else {
            return
        }
        defer { preparedDelete.transientReservation?.release() }
        try updateMutationJournal(
            identity: identity,
            previousModel: preparedDelete.persistedModel,
            currentModel: nil
        )
        try await store.commitPreparedDelete(
            preparedDelete,
            transaction: storageAccess
        )
        try await PolymorphicProjectionMaintainer(
            container: container
        ).remove(
            preparedDelete.persistedModel,
            identifier: preparedDelete.identifier,
            transaction: storageAccess
        )
        let mutationContext = try makeMutationContext(operationID: operationID)
        do {
            try await mutationMaintenanceService.update(
                identity: identity,
                oldModel: preparedDelete.persistedModel,
                newModel: nil,
                context: mutationContext
            )
            await mutationContext.closeAndWait()
        } catch {
            await mutationContext.closeAndWait()
            throw error
        }
        try ensureActive(operationID, permitsMutation: true)
        activeMutationIdentities.remove(identity)
        if dispatchSQLActions {
            try await applySQLReferentialActions(oldModel: preparedDelete.persistedModel,
                newModel: nil, operationID: operationID)
        }
    }

    private func updateMutationJournal(
        identity: EntityReference,
        previousModel: PersistedModel?,
        currentModel: PersistedModel?
    ) throws {
        sqlConstraintsPending = true
        try mutationJournal.record(
            identity: identity,
            previousModel: previousModel,
            currentModel: currentModel
        )
    }

    private func loadPersistedModelUnchecked(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?,
        fields: Set<String>? = nil
    ) async throws -> PersistedModel? {
        let runtime = try entityRuntime(named: entity)
        if runtime.entity.hasDynamicDirectory, partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: entity,
                fields: runtime.entity.dynamicFieldNames
            )
        }
        guard let subspaces = try await openSubspaces(
            for: runtime.entity,
            partition: partition
        ) else {
            return nil
        }
        let key = subspaces.items.subspace(entity).pack(id)
        let storage = container.itemStorageFactory.make(
            transaction: storageAccess,
            blobsSubspace: subspaces.blobs
        )
        guard let data = try await storage.read(for: key) else {
            return nil
        }
        let persistedModel = try DataAccess.deserializePersistedModel(
            data,
            expectedEntity: entity
        )
        let model = try runtime.canonicalized(persistedModel)
        try readPolicy.authorizeGet(model, fields: fields)
        return model
    }

    private func loadRetainedPersistedModelUnchecked(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        fields: Set<String>?
    ) async throws -> DatabaseRetainedPersistedModels.Entry? {
        let runtime = try entityRuntime(named: entity)
        if runtime.entity.hasDynamicDirectory, partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: entity,
                fields: runtime.entity.dynamicFieldNames
            )
        }
        guard let subspaces = try await openSubspaces(
            for: runtime.entity,
            partition: partition
        ) else {
            return nil
        }
        let key = subspaces.items.subspace(entity).pack(id)
        let storage = container.itemStorageFactory.make(
            transaction: storageAccess,
            blobsSubspace: subspaces.blobs
        )
        guard let data = try await storage.readRetained(
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            stage: .storageRow
        ) else {
            return nil
        }
        let retained = try DatabaseRetainedStoredModel.decode(
            data,
            entity: entity,
            runtime: runtime,
            workMeter: workMeter,
            stage: .storageRow
        )
        try retained.withModel { model in
            try readPolicy.authorizeGet(model, fields: fields)
        }
        try workMeter.checkpoint(at: .storageRow)
        return retained.makeEntry()
    }

    private func scanPersistedModelsUnchecked(
        entity: String,
        partition: AnyDirectoryPath?,
        limit: Int,
        offset: Int,
        startingAfterIdentifier: ByteString?,
        workMeter: DatabaseWorkMeter,
        authorizationRequirement: DatabaseListReadAuthorizationRequirement?
    ) async throws -> DatabaseRetainedPersistedModels {
        guard limit > 0 else {
            throw DatabaseTransactionError.invalidLimit(limit)
        }
        guard offset >= 0 else {
            throw DatabaseTransactionError.invalidLimit(offset)
        }
        let runtime = try entityRuntime(named: entity)
        if runtime.entity.hasDynamicDirectory, partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: entity,
                fields: runtime.entity.dynamicFieldNames
            )
        }
        let authorizedFields: Set<String>?
        if let readAuthorization {
            try readPolicy.validate(readAuthorization)
            guard let authorizationRequirement,
                  authorizationRequirement.entityName == runtime.entity.name,
                  readAuthorization.covers(
                    listRequirement: authorizationRequirement
                  ),
                  let fields = readAuthorization.fields.fieldsByEntity[
                    runtime.entity.name
                  ] else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            authorizedFields = fields
        } else {
            try readPolicy.authorizeList(
                entityName: runtime.entity.name,
                limit: limit,
                offset: nil,
                orderBy: nil
            )
            authorizedFields = nil
        }
        guard let subspaces = try await openSubspaces(
            for: runtime.entity,
            partition: partition
        ) else {
            let empty = try DatabaseRetainedArrayBuilder<
                DatabaseRetainedPersistedModels.Entry?
            >(
                workMeter: workMeter,
                stage: .storageRow,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    DatabaseRetainedPersistedModels.Entry?.self
                )
            )
            return try DatabaseRetainedPersistedModels(buffer: empty.finish())
        }
        let (begin, end) = subspaces.items.subspace(entity).range()
        let entitySubspace = subspaces.items.subspace(entity)
        let startingAfter = try startingAfterIdentifier.map {
            entitySubspace.pack(try Tuple(packed: $0))
        }
        let storage = container.itemStorageFactory.make(
            transaction: storageAccess,
            blobsSubspace: subspaces.blobs
        )
        let (requestedScanLimit, scanLimitOverflow) = limit
            .addingReportingOverflow(offset)
        let scanLimit = scanLimitOverflow ? Int.max : requestedScanLimit
        return try await Self.consumePersistedModels(
            storage: storage,
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            scanLimit: scanLimit,
            expectedCount: limit,
            offset: offset,
            entity: entity,
            runtime: runtime,
            readPolicy: readPolicy,
            authorizedFields: authorizedFields,
            workMeter: workMeter
        )
    }

    /// The actor has already resolved and sealed every transaction-bound
    /// dependency before this helper runs. Keeping cursor consumption in a
    /// nonisolated operation scope prevents the callback from carrying actor
    /// isolation across the backend cursor's asynchronous cleanup boundary.
    private nonisolated static func consumePersistedModels(
        storage: ItemStorage,
        begin: ByteString,
        end: ByteString,
        startingAfter: ByteString?,
        scanLimit: Int,
        expectedCount: Int,
        offset: Int,
        entity: String,
        runtime: EntityRuntimeRegistration,
        readPolicy: DatabaseReadPolicy?,
        authorizedFields: Set<String>?,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedPersistedModels {
        var models = try DatabaseRetainedArrayBuilder<
            DatabaseRetainedPersistedModels.Entry?
        >(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseRetainedPersistedModels.Entry?.self
            ),
            expectedCount: expectedCount
        )
        var skipped = 0
        try await storage.consumeRetainedScan(
            begin: begin,
            end: end,
            startingAfter: startingAfter,
            snapshot: false,
            limit: scanLimit,
            workMeter: workMeter,
            stage: .storageRow
        ) { _, data in
            try workMeter.consume(at: .storageRow)
            if skipped < offset {
                skipped += 1
                return
            }
            let retained = try DatabaseRetainedStoredModel.decode(
                data,
                entity: entity,
                runtime: runtime,
                workMeter: workMeter,
                stage: .storageRow
            )
            try retained.withModel { model in
                try readPolicy?.authorizeGet(model, fields: authorizedFields)
            }
            try workMeter.checkpoint(at: .storageRow)
            let admission = try models.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1),
                at: .storageRow
            )
            models.append(retained.makeEntry(), using: admission)
        }
        return try DatabaseRetainedPersistedModels(buffer: models.finish())
    }

    private func scanPersistedModelChangesForMutationUnchecked(
        entity: String,
        partition: AnyDirectoryPath?,
        maximumRows: Int,
        maximumChanges: Int,
        workMeter: DatabaseWorkMeter,
        transform: @Sendable (
            borrowing PersistedModel
        ) async throws -> DatabasePreparedStatementMutationChange?
    ) async throws -> sending DatabaseRetainedEntityMutationScan {
        guard maximumRows >= 0, maximumRows < Int.max else {
            throw DatabaseTransactionError.invalidLimit(maximumRows)
        }
        guard maximumChanges > 0 else {
            throw DatabaseEntityMutationError.changeLimitExceeded(
                actual: 0,
                maximum: maximumChanges
            )
        }
        let runtime = try entityRuntime(named: entity)
        if runtime.entity.hasDynamicDirectory, partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: entity,
                fields: runtime.entity.dynamicFieldNames
            )
        }
        let storageLimit = maximumRows + 1
        try readPolicy.authorizeList(
            entityName: runtime.entity.name,
            limit: storageLimit,
            offset: nil,
            orderBy: nil
        )
        var changes = try DatabaseRetainedArrayBuilder<EntityMutationChange>(
            workMeter: workMeter,
            stage: .mutationPlanning,
            layout: try DatabaseRetainedArrayLayout.forElement(EntityMutationChange.self)
        )
        guard let subspaces = try await openSubspaces(
            for: runtime.entity,
            partition: partition
        ) else {
            return DatabaseRetainedEntityMutationScan(
                changes: changes.finish(),
                hasMoreSourceRows: false
            )
        }
        let (begin, end) = subspaces.items.subspace(entity).range()
        let storage = container.itemStorageFactory.make(
            transaction: storageAccess,
            blobsSubspace: subspaces.blobs
        )
        var iterator = storage.scan(
            begin: begin,
            end: end,
            startingAfter: nil,
            snapshot: false,
            limit: storageLimit,
            workMeter: workMeter,
            stage: .storageRow
        ).makeAsyncIterator()
        var sourceRowCount = 0
        while let (_, data) = try await iterator.next() {
            try workMeter.consume(at: .storageRow)
            try DatabaseByteProcessingMeter.consume(
                byteCount: data.count,
                passes: 2,
                workMeter: workMeter,
                stage: .storageRow
            )
            let persistedModel = try DataAccess.deserializePersistedModel(
                data,
                expectedEntity: entity
            )
            let model = try runtime.canonicalized(persistedModel)
            try workMeter.checkpoint(at: .storageRow)
            try readPolicy.authorizeGet(model)
            guard sourceRowCount < maximumRows else {
                return DatabaseRetainedEntityMutationScan(
                    changes: changes.finish(),
                    hasMoreSourceRows: true
                )
            }
            sourceRowCount += 1
            guard let prepared = try await transform(model) else { continue }
            guard prepared.workMeter === workMeter else {
                throw DatabaseIntermediateReservationError.workMeterMismatch
            }
            let change = prepared.change
            guard changes.count < maximumChanges else {
                throw DatabaseEntityMutationError.changeLimitExceeded(
                    actual: maximumChanges == Int.max
                        ? Int.max
                        : maximumChanges + 1,
                    maximum: maximumChanges
                )
            }
            try withExtendedLifetime(prepared) {
                try changes.append(
                    footprint: try DatabaseEntityMutationFootprintMeter.footprint(
                        of: change,
                        workMeter: workMeter
                    ),
                    make: { change }
                )
            }
        }
        return DatabaseRetainedEntityMutationScan(
            changes: changes.finish(),
            hasMoreSourceRows: false
        )
    }

    // MARK: - Checkpoint lifecycle

    package func configureCheckpointExecution(workMeter: DatabaseWorkMeter,
        checkpoints: any TransactionCheckpointAccess
    ) throws {
        guard checkpointWorkMeter == nil, state == .open else {
            throw DatabaseTransactionCheckpointError.unavailable
        }
        try mutationJournal.bind(to: workMeter)
        let layout = try DatabaseRetainedArrayLayout.forElement(CheckpointRecord.self)
        checkpointReservation = try workMeter.reserveIntermediate(
            bytes: layout.containerByteCount, at: .mutationPlanning)
        checkpointInitialLease = readPolicy.schemaLease
        checkpointWorkMeter = workMeter
        physicalCheckpoints = checkpoints
    }

    private func hasAdmittedCheckpointScope() -> Bool {
        guard let scope = DatabaseTransactionCheckpointScope.current else { return false }
        return scope.owner == ObjectIdentifier(self) && scope.isActive
    }

    private func beginCheckpointScope(independent: Bool = false) throws -> DatabaseTransactionCheckpointScope? {
        guard checkpointWorkMeter != nil else { return nil }
        if let inherited = DatabaseTransactionCheckpointScope.current,
           inherited.owner == ObjectIdentifier(self), !inherited.isActive {
            throw DatabaseTransactionError.closed
        }
        let descendant = hasAdmittedCheckpointScope()
        if checkpointTransitionPending && !descendant { throw DatabaseTransactionError.concurrentOperation }
        switch state {
        case .failed: throw DatabaseTransactionCheckpointError.failed
        case .closed, .preparingCommit: throw DatabaseTransactionError.closed
        case .executing where !descendant: throw DatabaseTransactionError.concurrentOperation
        case .open, .executing: break
        }
        if descendant && !independent { return nil }
        guard nextCheckpointScopeIdentifier < UInt64.max else {
            throw DatabaseTransactionCheckpointError.scopeIdentifierExhausted
        }
        try checkpointGate.enter(admittedDescendant: descendant)
        let scope = DatabaseTransactionCheckpointScope(owner: ObjectIdentifier(self),
            identifier: nextCheckpointScopeIdentifier, gate: checkpointGate)
        nextCheckpointScopeIdentifier += 1
        return scope
    }

    package func registerCheckpointReadScope(
        close: @escaping @Sendable () async throws -> Void
    ) throws -> DatabaseTransactionCheckpointScope? {
        guard let scope = try beginCheckpointScope(independent: true),
              let meter = checkpointWorkMeter else { return nil }
        do {
            let layout = try DatabaseRetainedArrayLayout.forElement(ReadScopeRecord.self)
            let reservation = try meter.reserveIntermediate(
                bytes: try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                    .adding(DatabaseIntermediateFootprint(bytes: layout.sharedOwnerByteCount)).bytes,
                at: .resultMaterialization)
            checkpointReadScopes.append(ReadScopeRecord(scope: scope, close: close, reservation: reservation))
            return scope
        } catch {
            scope.end()
            recordCheckpointFailure(error)
            throw error
        }
    }

    package func endCheckpointReadScope(_ scope: DatabaseTransactionCheckpointScope?) {
        guard let scope else { return }
        checkpointReadScopes.removeAll { $0.scope.identifier == scope.identifier }
        scope.end()
    }

    package func recordCheckpointFailure(_ error: any Error) {
        guard state != .closed else { return }
        if checkpointWorkMeter != nil, checkpointFailure == nil { checkpointFailure = error }
        if checkpointWorkMeter != nil, !(error is CancellationError),
           !(error is TransactionExecutionDeadlineExceeded),
           !(error is DatabaseWorkLimitError),
           !((error as? StorageError).map { $0.isRetryable || $0.code == .commitUnknownResult } ?? false) {
            state = .failed
        } else {
            state = .closed
        }
    }

    private func drainCheckpointScopes(permanently: Bool) async throws {
        guard checkpointWorkMeter != nil else { return }
        if hasAdmittedCheckpointScope() { throw DatabaseTransactionCheckpointError.barrierInsideOperation }
        checkpointTransitionPending = true
        checkpointGate.pause()
        let scopes = checkpointReadScopes
        checkpointReadScopes.removeAll()
        var cleanupFailure: (any Error)?
        for record in scopes {
            do { try await record.close() } catch {
                if let previous = cleanupFailure {
                    cleanupFailure = DatabaseReadSessionCleanupError(operationError: previous, cleanupError: error)
                } else {
                    cleanupFailure = error
                }
            }
            record.scope.end()
        }
        await checkpointGate.closeAndWait()
        if let cleanupFailure { throw cleanupFailure }
        if !permanently { try ensureDatabaseTaskIsActive() }
    }

    private func withCheckpointBarrier<Result: Sendable>(
        recovering: Bool = false,
        _ operation: () async throws -> Result
    ) async throws -> Result {
        guard checkpointWorkMeter != nil else { throw DatabaseTransactionCheckpointError.unavailable }
        guard state != .closed else { throw DatabaseTransactionError.closed }
        guard !checkpointTransitionPending else { throw DatabaseTransactionError.concurrentOperation }
        if hasAdmittedCheckpointScope() { throw DatabaseTransactionCheckpointError.barrierInsideOperation }
        do {
            try await drainCheckpointScopes(permanently: false)
            switch state {
            case .open: break
            case .failed where recovering: break
            case .failed: throw DatabaseTransactionCheckpointError.failed
            case .executing, .preparingCommit, .closed: throw DatabaseTransactionError.closed
            }
            let result = try await operation()
            checkpointTransitionPending = false
            checkpointGate.resume()
            return result
        } catch {
            state = .closed
            if recovering, let primary = checkpointFailure {
                throw DatabaseReadSessionCleanupError(operationError: primary, cleanupError: error)
            }
            throw error
        }
    }

    public func createCheckpoint() async throws -> DatabaseTransactionCheckpoint {
        try await withCheckpointBarrier {
            guard let checkpoints = physicalCheckpoints,
                  let reservation = checkpointReservation,
                  let meter = checkpointWorkMeter else { throw DatabaseTransactionCheckpointError.unavailable }
            try meter.consume(at: .mutationPlanning)
            let layout = try DatabaseRetainedArrayLayout.forElement(CheckpointRecord.self)
            let growth = try layout.growth(from: checkpointRecordCapacity, toFit: checkpointRecords.count + 1)
            try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .mutationPlanning)
            checkpointRecords.reserveCapacity(growth.capacity)
            checkpointRecordCapacity = growth.capacity
            let checkpoint = DatabaseTransactionCheckpoint(physical: try await checkpoints.createCheckpoint())
            checkpointRecords.append(CheckpointRecord(checkpoint: checkpoint,
                undoPosition: mutationJournal.checkpointPosition(), schema: schemaStagingState,
                sqlModes: sqlConstraintModes, sqlModeReservation: sqlConstraintModeReservation, sqlPending: sqlConstraintsPending))
            return checkpoint
        }
    }

    public func rollback(to checkpoint: DatabaseTransactionCheckpoint) async throws {
        try await withCheckpointBarrier(recovering: true) {
            guard let index = checkpointRecords.firstIndex(where: { $0.checkpoint == checkpoint }),
                  let checkpoints = physicalCheckpoints,
                  let meter = checkpointWorkMeter else { throw DatabaseTransactionCheckpointError.invalidToken }
            try meter.consume(at: .mutationPlanning)
            let record = checkpointRecords[index]
            for _ in record.undoPosition..<mutationJournal.undoCount {
                try meter.consume(at: .mutationPlanning)
            }
            try await checkpoints.rollback(to: checkpoint.physical)
            mutationJournal.rollback(to: record.undoPosition)
            schemaStagingState = record.schema
            sqlConstraintModes = record.sqlModes
            sqlConstraintModeReservation = record.sqlModeReservation
            sqlConstraintsPending = record.sqlPending
            if let lease = record.schema?.lease ?? checkpointInitialLease {
                readPolicy = DatabaseReadPolicy(schemaLease: lease, authorization: readPolicy.authorization)
            }
            checkpointRecords.removeSubrange((index + 1)..<checkpointRecords.count)
            subspaceCache = DatabaseStoreCache()
            scheduledDeletions.removeAll()
            scheduledWrites.removeAll()
            activeMutationIdentities.removeAll()
            checkpointFailure = nil
            state = .open
        }
    }

    public func release(_ checkpoint: DatabaseTransactionCheckpoint) async throws {
        try await withCheckpointBarrier {
            guard let index = checkpointRecords.firstIndex(where: { $0.checkpoint == checkpoint }),
                  let checkpoints = physicalCheckpoints,
                  let meter = checkpointWorkMeter else { throw DatabaseTransactionCheckpointError.invalidToken }
            try meter.consume(at: .mutationPlanning)
            try await checkpoints.release(checkpoint.physical)
            checkpointRecords.removeSubrange(index..<checkpointRecords.count)
            if checkpointRecords.isEmpty { mutationJournal.releaseCheckpointHistory() }
        }
    }

    private func clearCheckpointRecords() {
        checkpointRecords.removeAll()
        checkpointReservation = nil
        checkpointRecordCapacity = 0
        physicalCheckpoints = nil
        checkpointFailure = nil
        if let lease = checkpointInitialLease {
            readPolicy = DatabaseReadPolicy(schemaLease: lease, authorization: readPolicy.authorization)
        }
        checkpointInitialLease = nil
    }

    package func withStatementScope<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        guard !checkpointStatementIsActive else { throw DatabaseTransactionError.concurrentOperation }
        let scope = try beginCheckpointScope()
        checkpointStatementIsActive = true
        defer {
            checkpointStatementIsActive = false
            scope?.end()
        }
        let lease = readPolicy.schemaLease
        guard let binding = ActiveDatabaseTransactionContext.binding,
              binding.container === container else { throw DatabaseTransactionError.invalidOperationContext }
        do {
            try checkpointWorkMeter?.consume(at: .mutationPlanning)
            return try await DatabaseTransactionCheckpointScope.$current.withValue(
                scope ?? DatabaseTransactionCheckpointScope.current
            ) {
                try await DatabaseSchemaExecutionScope.$container.withValue(container) {
                    try await DatabaseSchemaExecutionScope.$lease.withValue(lease) {
                        try await ActiveDatabaseTransactionContext.$binding.withValue(binding.rebindingSchemaLease(lease)) {
                            try await operation()
                        }
                    }
                }
            }
        } catch {
            recordCheckpointFailure(error)
            throw error
        }
    }

    @_spi(DatabaseExecution)
    public func stageSQLCatalog(_ catalog: SQLCatalog, expectedGeneration: UInt64,
        idempotencyKey: String) async throws -> DatabaseSchemaPublicationResult {
        try await performOperation { _ in
            guard let workMeter = checkpointWorkMeter else { throw DatabaseTransactionCheckpointError.unavailable }
            let originalLease = checkpointInitialLease ?? readPolicy.schemaLease.publicationSource ?? readPolicy.schemaLease
            try await container.requireSQLWriterGeneration(schemaStagingState?.lease.generation ?? originalLease.generation,
                transaction: storageAccess)
            let outcome = try await container.stageSQLCatalog(catalog,
                expectedGeneration: expectedGeneration, idempotencyKey: idempotencyKey,
                authorization: readPolicy.authorization, sourceLease: readPolicy.schemaLease,
                transaction: storageAccess, workMeter: workMeter)
            if let candidate = outcome.state {
                schemaStagingState = candidate
                readPolicy = DatabaseReadPolicy(schemaLease: candidate.lease, authorization: readPolicy.authorization)
                sqlConstraintsPending = true
            }
            return outcome.publication
        }
    }

    package func takePreparedSchemaPublication() -> DatabasePreparedSchemaPublication? {
        precondition(state == .closed)
        let publication = schemaStagingState?.publication
        schemaStagingState = nil
        return publication
    }

    package func takeCheckpointCleanupFailure() -> (any Error)? {
        let failure = checkpointCleanupFailure
        checkpointCleanupFailure = nil
        return failure
    }

    package var hasPendingCheckpointBarrier: Bool { checkpointTransitionPending }

    // MARK: - Operation lifecycle

    private func performOperation<Value: ~Copyable & Sendable>(
        _ operation: (UInt64) async throws -> sending Value
    ) async throws -> sending Value {
        guard checkpointWorkMeter != nil else {
            return try await performAdmittedOperation(operation)
        }
        let scope = try beginCheckpointScope()
        defer { scope?.end() }
        let box = try await DatabaseTransactionCheckpointScope.$current.withValue(
            scope ?? DatabaseTransactionCheckpointScope.current
        ) {
            DatabaseReadResultBox(try await performAdmittedOperation(operation))
        }
        return box.take()
    }

    private func performAdmittedOperation<Value: ~Copyable & Sendable>(
        _ operation: (UInt64) async throws -> sending Value
    ) async throws -> sending Value {
        try operationGate.enter()
        var operationID: UInt64?
        do {
            try ensureDatabaseTaskIsActive()
            guard state == .open else {
                throw lifecycleError(for: state)
            }
            let issuedOperationID = try issueOperationID()
            operationID = issuedOperationID
            state = .executing(issuedOperationID)
            defer {
                scheduledDeletions.removeAll(keepingCapacity: true)
                scheduledWrites.removeAll(keepingCapacity: true)
            }
            let value = try await operation(issuedOperationID)
            try await validateSQLConstraints(forceDeferred: false)
            guard state == .executing(issuedOperationID) else {
                throw lifecycleError(for: state)
            }
            state = .open
            operationGate.leave()
            return value
        } catch {
            if let operationID, state == .executing(operationID) {
                recordCheckpointFailure(error)
            }
            operationGate.leave()
            throw error
        }
    }

    private func issueOperationID() throws -> UInt64 {
        guard nextOperationID < UInt64.max else {
            state = .closed
            throw DatabaseTransactionError.operationIdentifierExhausted
        }
        let operationID = nextOperationID
        nextOperationID += 1
        return operationID
    }

    private func ensureActive(
        _ operationID: UInt64,
        permitsMutation: Bool
    ) throws {
        switch state {
        case .executing(operationID):
            return
        case .preparingCommit(operationID) where !permitsMutation:
            return
        case .executing, .preparingCommit:
            throw DatabaseTransactionError.invalidOperationContext
        case .failed:
            throw DatabaseTransactionCheckpointError.failed
        case .open, .closed:
            throw DatabaseTransactionError.closed
        }
    }

    private func lifecycleError(
        for state: State
    ) -> DatabaseTransactionError {
        switch state {
        case .executing:
            return .concurrentOperation
        case .open:
            preconditionFailure("An open transaction has no lifecycle error")
        case .failed, .preparingCommit, .closed:
            return .closed
        }
    }

    private func makeMutationContext(
        operationID: UInt64
    ) throws -> PersistableMutationContext {
        #if DATABASE_MULTI_BASE
        let dataRoot = try container.requireActiveDataRoot().dataDirectory.root
        #else
        let dataRoot = container.defaultTenant.data.root
        #endif
        return PersistableMutationContext(
            schema: container.schema,
            transaction: self,
            operationID: operationID,
            dataRoot: dataRoot,
            storageAccess: storageAccess
        )
    }

    // MARK: - Storage mapping

    private func openSubspaces<Model: Persistable>(
        for type: Model.Type
    ) async throws -> ResolvedSubspaces? {
        guard !Model.hasDynamicDirectory else {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: Model.persistableType,
                fields: Model.directoryFieldNames
            )
        }
        return try await openSubspaces(
            for: type,
            in: DirectoryPath<Model>()
        )
    }

    private func openSubspaces<Model: Persistable>(
        for type: Model.Type,
        in partition: DirectoryPath<Model>
    ) async throws -> ResolvedSubspaces? {
        guard let entity = container.schema.entity(named: Model.persistableType) else {
            throw DatabaseTransactionError.unknownEntity(Model.persistableType)
        }
        return try await openSubspaces(
            for: entity,
            partition: try AnyDirectoryPath(partition)
        )
    }

    private func openSubspaces(
        for entity: Schema.Entity,
        partition: AnyDirectoryPath?
    ) async throws -> ResolvedSubspaces? {
        let path = try partition ?? AnyDirectoryPath(for: entity)
        try path.validate()
        let partitionPath = path.resolve()
        #if DATABASE_MULTI_BASE
        let cacheKey = DatabaseStoreCacheKey(
            basePlacementGeneration: try container.requireActiveDataRoot().generation,
            entity: entity.name,
            components: partitionPath
        )
        #else
        let cacheKey = DatabaseStoreCacheKey(
            entity: entity.name,
            components: partitionPath
        )
        #endif
        if let cached = subspaceCache.value(for: cacheKey) {
            return cached
        }
        // Every caller of this walk reads or deletes, so it opens without
        // creating: a directory no write ever reached is an absent keyspace,
        // and reporting it as such is what keeps a read from publishing a
        // directory. The absent result is deliberately not cached, because the
        // next write in this same transaction may create it.
        guard let root = try await container.openDirectory(
            for: entity,
            path: path,
            transaction: storageAccess
        ) else {
            return nil
        }
        let resolved = ResolvedSubspaces(
            items: root.subspace(SubspaceKey.items),
            blobs: root.subspace(SubspaceKey.blobs),
            partitionPath: partitionPath
        )
        subspaceCache.insert(resolved, for: cacheKey)
        return resolved
    }

    private func fetchModel<Model: Persistable>(
        of type: Model.Type,
        identifiedBy id: Model.ID,
        from subspaces: ResolvedSubspaces,
        consistency: TransactionReadConsistency
    ) async throws -> Model? {
        let key = subspaces.items
            .subspace(Model.persistableType)
            .pack(try PersistableIdentifierKeyCodec.tuple(for: id))
        let storage = container.itemStorageFactory.make(
            transaction: storageAccess,
            blobsSubspace: subspaces.blobs
        )
        guard let bytes = try await storage.read(
            for: key,
            snapshot: consistency == .snapshot
        ) else {
            return nil
        }
        let persistedModel = try DataAccess.deserializePersistedModel(
            bytes,
            expectedEntity: Model.persistableType
        )
        let runtime = try entityRuntime(named: Model.persistableType)
        let canonicalModel = try runtime.canonicalized(persistedModel)
        let model = try canonicalModel.decode(as: Model.self)
        try readPolicy.authorizeGet(canonicalModel)
        return model
    }

    private func entityRuntime(
        named entity: String
    ) throws -> EntityRuntimeRegistration {
        guard readPolicy.schema.entity(named: entity) != nil else {
            throw DatabaseTransactionError.unknownEntity(entity)
        }
        guard let runtime = readPolicy.entityRuntime(named: entity) else {
            throw DatabaseTransactionError.entityHasNoPersistableType(entity)
        }
        return runtime
    }

    private func resolve(
        _ identity: EntityReference
    ) throws -> (
        entity: Schema.Entity,
        id: Tuple,
        partition: AnyDirectoryPath?
    ) {
        let runtime = try entityRuntime(named: identity.entity)
        guard let entity = readPolicy.schema.entity(named: identity.entity) else {
            throw DatabaseTransactionError.invalidIdentity(
                entity: identity.entity,
                reason: "entity is not present in the schema"
            )
        }
        do {
            let id = try PersistableIdentifierKeyCodec.tuple(
                for: identity,
                expectedType: runtime.entity.identifierType
            )
            let partition = try CanonicalPartitionBinding.makeAnyBinding(
                for: entity,
                partitions: identity.partitions
            )
            return (entity, id, partition)
        } catch {
            throw DatabaseTransactionError.invalidIdentity(
                entity: identity.entity,
                reason: "identity resolution failed"
            )
        }
    }

    private func validate(
        _ continuation: DatabaseScanContinuation?,
        entity: String,
        partitionPath: [String]
    ) throws {
        guard let continuation else {
            return
        }
        guard continuation.entity == entity else {
            throw DatabaseScanContinuationError.mismatchedEntity(
                expected: entity,
                actual: continuation.entity
            )
        }
        guard continuation.partitionPath == partitionPath else {
            throw DatabaseScanContinuationError.mismatchedPartition
        }
    }
}

public enum DatabaseTransactionError: Error, Sendable, Equatable {
    case concurrentOperation
    case closed
    case invalidOperationContext
    case operationIdentifierExhausted
    case invalidLimit(Int)
    case itemDisappearedDuringScan
    case unknownEntity(String)
    case entityHasNoPersistableType(String)
    case invalidIdentity(entity: String, reason: String)
    case persistedModelNotFound(EntityReference)
    case duplicateMutation(EntityReference)
    case conflictingDerivedMutation(EntityReference)
}


extension DatabaseTransaction {
    private func sqlMeter() -> DatabaseWorkMeter {
        if let meter = mutationJournal.workMeter ?? checkpointWorkMeter ?? sqlConstraintMeter { return meter }
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        sqlConstraintMeter = meter
        return meter
    }

    package func sqlPolicyCatalog() throws -> (SQLCatalog, Schema) {
        guard state != .closed && state != .failed else { throw lifecycleError(for: state) }
        return (readPolicy.schemaLease.sqlCatalog.catalog, readPolicy.schemaLease.schema)
    }

    package func setSQLConstraintsDeferred(_ deferred: Bool, names: [String]?) async throws {
        try await performOperation { _ in
            let catalog = readPolicy.schemaLease.sqlCatalog.catalog
            let meter = sqlMeter()
            var metadataBytes: UInt64 = 256
            for relation in catalog.relations {
                for attribute in relation.constraints.map({ $0.attributes }) + (relation.primaryKeyConstraint.map { [$0] } ?? []) {
                    metadataBytes = try DatabaseIntermediateFootprint(bytes: metadataBytes).adding(DatabaseIntermediateFootprint(bytes:
                        256 + UInt64(relation.id.utf8.count + attribute.id.utf8.count + attribute.name.utf8.count))).bytes
                }
            }
            let scratch = try meter.reserveIntermediate(bytes: metadataBytes, at: .mutationPlanning)
            defer { scratch.release() }
            var selected: [(SQLRelation, SQLConstraintAttributes)] = []
            for relation in catalog.relations {
                let attributes = relation.constraints.map({ $0.attributes }) + (relation.primaryKeyConstraint.map { [$0] } ?? [])
                for attribute in attributes where names == nil || names!.contains(where: { SQLConstraintCompiler.same($0, attribute.name) }) {
                    if names == nil, case .immediate = attribute.timing { continue }
                    selected.append((relation, attribute))
                }
            }
            for name in names ?? [] where !selected.contains(where: { SQLConstraintCompiler.same($0.1.name, name) }) {
                throw SQLConstraintError.unknownConstraint(name)
            }
            for (_, attribute) in selected {
                if case .immediate = attribute.timing { throw SQLConstraintError.notDeferrable(attribute.name) }
            }
            if !deferred {
                try await validateSQLConstraints(forceDeferred: false, forced: selected.map { constraintKey(relation: $0.0.id, constraint: $0.1.id) })
            }
            // Reserve the replacement dictionary before COW can allocate. A
            // checkpoint retains the prior dictionary and its original claim.
            let claim = try meter.reserveIntermediate(bytes: metadataBytes, at: .mutationPlanning)
            var replacement = sqlConstraintModes
            for (relation, attribute) in selected {
                replacement[constraintKey(relation: relation.id, constraint: attribute.id)] = deferred
            }
            sqlConstraintModes = replacement
            sqlConstraintModeReservation = claim
        }
    }

    private func constraintKey(relation: String, constraint: String) -> SQLCatalogName {
        SQLCatalogName("\(relation.utf8.count):\(relation)\(constraint)")
    }

    private func sqlIsDeferred(_ attributes: SQLConstraintAttributes, relation: SQLRelation) -> Bool {
        if let mode = sqlConstraintModes[constraintKey(relation: relation.id, constraint: attributes.id)] { return mode }
        if case .deferrable(let initiallyDeferred) = attributes.timing { return initiallyDeferred }
        return false
    }

    private func requiresSQLValidation(_ relation: SQLRelation, schema: Schema) -> Bool {
        if !relation.constraints.isEmpty { return true }
        if relation.columns.contains(where: { if case .input(nil) = $0.policy { return false }; return true }) { return true }
        guard let ids = relation.primaryKeyColumnIDs else { return false }
        guard ids.count == 1,
              let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.id, ids[0]) }),
              case .field(let number) = column.backing,
              let entity = schema.entities.first(where: { SQLConstraintCompiler.same($0.name, relation.entityName) }),
              let field = entity.fields.first(where: { $0.fieldNumber == number }),
              SQLConstraintCompiler.same(field.name, "id"), !field.isOptional,
              relation.primaryKeyConstraint?.timing == .immediate else { return true }
        return false
    }

    /// Integrity scans borrow only the admitted attempt's root and schema.
    /// They expose no rows or read capability to a caller and never apply user
    /// row filtering, which could hide a conflicting key or referenced row.
    private func sqlIntegrityRows(_ relation: SQLRelation, meter: DatabaseWorkMeter) async throws -> DatabaseRetainedPersistedModels {
        let runtime = try entityRuntime(named: relation.entityName)
        var output = try DatabaseRetainedArrayBuilder<DatabaseRetainedPersistedModels.Entry?>(
            workMeter: meter, stage: .storageRow,
            layout: DatabaseRetainedArrayLayout.forElement(DatabaseRetainedPersistedModels.Entry?.self))
        var continuation: ByteString?
        repeat {
            try meter.consume(at: .storageRow)
            let paths: [AnyDirectoryPath?]
            if runtime.entity.hasDynamicDirectory {
                let page = try await container.partitionCatalogPage(entity: runtime.entity,
                    directoryLayers: readPolicy.schemaLease.directoryLayers,
                    continuation: continuation, limit: 1, transaction: storageAccess)
                paths = try page.entries.map { try AnyDirectoryPath(entity: runtime.entity, partitions: $0.partitions) }
                continuation = page.continuation
            } else {
                paths = [nil]
                continuation = nil
            }
            for path in paths {
                guard let subspaces = try await openSubspaces(for: runtime.entity, partition: path) else { continue }
                let (begin, end) = subspaces.items.subspace(relation.entityName).range()
                let storage = container.itemStorageFactory.make(transaction: storageAccess, blobsSubspace: subspaces.blobs)
                let rows = try await Self.consumePersistedModels(storage: storage, begin: begin, end: end,
                    startingAfter: nil, scanLimit: Int.max, expectedCount: 0, offset: 0,
                    entity: relation.entityName, runtime: runtime, readPolicy: nil, authorizedFields: nil, workMeter: meter)
                for entry in rows {
                    try output.append(footprint: DatabaseIntermediateFootprint(rows: 1), make: { entry })
                }
            }
        } while continuation != nil
        return try DatabaseRetainedPersistedModels(buffer: output.finish())
    }

    private func sqlRow(_ model: borrowing PersistedModel, relation: SQLRelation, meter: DatabaseWorkMeter,
        recomputingStored: Bool = false) async throws -> CanonicalSourceRow {
        try await SQLConstraintRows.row(fields: DatabaseEntityProjection.fieldObject(for: model), relation: relation,
            physicalSchema: readPolicy.schemaLease.schema, environment: makeSQLTemporalEnvironment(),
            workMeter: meter, recomputingStored: recomputingStored)
    }

    private func validateSQLConstraints(forceDeferred: Bool, forced: [SQLCatalogName] = [], rowBoundary: Bool = false,
        catalogOverride: SQLCatalogSnapshot? = nil, requireValidation: Bool = false,
        admissionAgainst previousCatalog: SQLCatalog? = nil) async throws {
        guard sqlConstraintsPending || requireValidation else { return }
        let lease = readPolicy.schemaLease
        let catalog = (catalogOverride ?? lease.sqlCatalog).catalog
        let meter = sqlMeter()
        let environment = makeSQLTemporalEnvironment()
        for relation in catalog.relations where requiresSQLValidation(relation, schema: lease.schema) {
            let previousRelation = previousCatalog?.relations.first { SQLConstraintCompiler.same($0.id, relation.id) }
            let rows = try await sqlIntegrityRows(relation, meter: meter)
            var constraints = relation.constraints
            if let columns = relation.primaryKeyColumnIDs, let attributes = relation.primaryKeyConstraint {
                constraints.insert(try SQLConstraint(attributes: attributes, kind: .unique(columnIDs: columns, nulls: .notDistinct)), at: 0)
            }
            for constraint in constraints {
                let primary = relation.primaryKeyConstraint.map { SQLConstraintCompiler.same($0.id, constraint.id) } ?? false
                if let previousRelation {
                    if primary {
                        if previousRelation.primaryKeyColumnIDs?.map(SQLCatalogName.init) == relation.primaryKeyColumnIDs?.map(SQLCatalogName.init) && previousRelation.primaryKeyConstraint == relation.primaryKeyConstraint { continue }
                    } else if previousRelation.constraints.contains(constraint) { continue }
                }
                if !requireValidation {
                    switch constraint.kind { case .check, .notNull: continue; default: break }
                }
                let deferred = sqlIsDeferred(constraint.attributes, relation: relation)
                let onlyPrimaryNulls: Bool
                if rowBoundary {
                    switch constraint.kind {
                    case .foreignKey: continue
                    case .unique:
                        if case .deferrable = constraint.timing {
                            if !primary { continue }
                            onlyPrimaryNulls = true
                        } else { onlyPrimaryNulls = false }
                    default: onlyPrimaryNulls = false
                    }
                } else { onlyPrimaryNulls = false }
                if !rowBoundary && deferred && !forceDeferred && !forced.contains(constraintKey(relation: relation.id, constraint: constraint.id)) { continue }
                let check: CanonicalBoundExpression?
                if case .check(let expression) = constraint.kind {
                    check = try SQLConstraintCompiler.binder(relation: relation, physicalSchema: lease.schema, workMeter: meter)
                        .bindPredicate(expression)
                } else { check = nil }
                for index in rows.indices {
                    guard let entry = rows[index] else { continue }
                    try await entry.withModel { model in
                        let row = try await sqlRow(model, relation: relation, meter: meter)
                        switch constraint.kind {
                        case .notNull(let id):
                            if SQLConstraintRows.nullCount(row, ids: [id], relation: relation) != 0 { throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name) }
                        case .check:
                            guard let check else {
                                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "CHECK binding is missing")
                            }
                            let value = try await DatabaseContext.evaluateCanonicalScalar(check, on: row,
                                temporalEnvironment: environment, session: try sqlPolicySession(meter: meter), workMeter: meter)
                            if case .primitive(.bool(false)) = value.value { throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name) }
                        case .unique(let ids, let nulls):
                            let count = SQLConstraintRows.nullCount(row, ids: ids, relation: relation)
                            if primary && count != 0 { throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name) }
                            if onlyPrimaryNulls { return }
                            if count != 0 && nulls == .distinct { return }
                            let key = try SQLConstraintRows.key(row, ids: ids, relation: relation, environment: environment, workMeter: meter)
                            for otherIndex in rows.indices where otherIndex < index {
                                guard let other = rows[otherIndex] else { continue }
                                try await other.withModel { otherModel in
                                    let otherRow = try await sqlRow(otherModel, relation: relation, meter: meter)
                                    if nulls == .distinct && SQLConstraintRows.nullCount(otherRow, ids: ids, relation: relation) != 0 { return }
                                    let otherKey = try SQLConstraintRows.key(otherRow, ids: ids, relation: relation, environment: environment, workMeter: meter)
                                    if try SQLConstraintRows.equal(key, otherKey, workMeter: meter) {
                                        throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name)
                                    }
                                }
                            }
                        case .foreignKey(let foreign):
                            let nullCount = SQLConstraintRows.nullCount(row, ids: foreign.columnIDs, relation: relation)
                            if nullCount > 0 {
                                if foreign.match == .simple || nullCount == foreign.columnIDs.count { return }
                                throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name)
                            }
                            guard let target = catalog.relations.first(where: { SQLConstraintCompiler.same($0.id, foreign.referencedRelationID) }),
                                  let targetIDs = foreign.referencedColumnIDs ?? target.primaryKeyColumnIDs else {
                                throw SQLConstraintError.invalidDeclaration(relation: relation.id, reason: "foreign key target is missing")
                            }
                            let key = try SQLConstraintRows.key(row, ids: foreign.columnIDs, relation: relation, environment: environment, workMeter: meter)
                            let targetRows = try await sqlIntegrityRows(target, meter: meter)
                            var found = false
                            for targetEntry in targetRows {
                                guard let targetEntry else { continue }
                                try await targetEntry.withModel { targetModel in
                                    let targetRow = try await sqlRow(targetModel, relation: target, meter: meter)
                                    let targetKey = try SQLConstraintRows.key(targetRow, ids: targetIDs, relation: target, environment: environment, workMeter: meter)
                                    if try SQLConstraintRows.equal(key, targetKey, workMeter: meter) { found = true }
                                }
                                if found { break }
                            }
                            if !found { throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name) }
                        }
                    }
                }
            }
            if requireValidation {
                for entry in rows {
                    guard let entry else { continue }
                    try await entry.withModel { model in
                        try await validateSQLRowPolicies(model, relation: relation, meter: meter,
                            previousRelation: previousRelation, checks: false)
                    }
                }
            }
        }
    }
}


extension DatabaseTransaction {
    private func applySQLReferentialActions(oldModel: PersistedModel?, newModel: PersistedModel?, operationID: UInt64) async throws {
        guard let oldModel else { return }
        let lease = readPolicy.schemaLease
        let catalog = lease.sqlCatalog.catalog
        guard let target = catalog.relations.first(where: { SQLConstraintCompiler.same($0.entityName, oldModel.entity) }) else { return }
        let meter = sqlMeter()
        let environment = makeSQLTemporalEnvironment()
        for source in catalog.relations {
            for constraint in source.constraints {
                guard case .foreignKey(let foreign) = constraint.kind,
                      SQLConstraintCompiler.same(foreign.referencedRelationID, target.id),
                      let targetIDs = foreign.referencedColumnIDs ?? target.primaryKeyColumnIDs else { continue }
                try meter.consume(at: .mutationPlanning)
                let oldRow = try await sqlRow(oldModel, relation: target, meter: meter)
                let oldKey = try SQLConstraintRows.key(oldRow, ids: targetIDs, relation: target, environment: environment, workMeter: meter)
                var newRow: CanonicalSourceRow?
                if let newModel {
                    let row = try await sqlRow(newModel, relation: target, meter: meter)
                    let key = try SQLConstraintRows.key(row, ids: targetIDs, relation: target, environment: environment, workMeter: meter)
                    if try SQLConstraintRows.equal(oldKey, key, workMeter: meter) { continue }
                    newRow = row
                }
                let action = newModel == nil ? foreign.onDelete : foreign.onUpdate
                if action == .noAction { continue }
                let rows = try await sqlIntegrityRows(source, meter: meter)
                for entry in rows {
                    guard let entry else { continue }
                    try await entry.withModel { model in
                        let row = try await sqlRow(model, relation: source, meter: meter)
                        if SQLConstraintRows.nullCount(row, ids: foreign.columnIDs, relation: source) > 0 { return }
                        let key = try SQLConstraintRows.key(row, ids: foreign.columnIDs, relation: source, environment: environment, workMeter: meter)
                        guard try SQLConstraintRows.equal(key, oldKey, workMeter: meter) else { return }
                        if action == .restrict { throw SQLConstraintError.violation(relation: source.id, constraint: constraint.name) }
                        let runtime = try entityRuntime(named: source.entityName)
                        let identity = try runtime.identity(for: copy model)
                        if action == .cascade, newModel == nil {
                            try await remove(identity: identity, copy model, precondition: .none, operationID: operationID, source: .sqlAction)
                            return
                        }
                        var actionPayloads = try DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>(workMeter: meter,
                            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self))
                        let fieldMetadata = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<(String, FieldValue)>.stride))
                            .multiplied(by: UInt64(source.columns.count + runtime.entity.fields.count))
                        let fieldClaim = try meter.reserveIntermediate(bytes: fieldMetadata.bytes, at: .mutationPlanning)
                        try actionPayloads.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: fieldClaim) })
                        let existingFields = try DatabaseEntityProjection.fieldObject(for: model)
                        var fields = existingFields.fields
                        let selected = newModel == nil ? (foreign.onDeleteColumnIDs ?? foreign.columnIDs) : foreign.columnIDs
                        for id in selected {
                            guard let slot = source.columns.firstIndex(where: { SQLConstraintCompiler.same($0.id, id) }),
                                  case .field(let number) = source.columns[slot].backing,
                                  let field = runtime.entity.fields.first(where: { $0.fieldNumber == number }) else {
                                throw SQLConstraintError.invalidDeclaration(relation: source.id, reason: "referential action requires a physical column")
                            }
                            let value: QueryValue
                            if action == .cascade, let newRow,
                               let position = foreign.columnIDs.firstIndex(where: { SQLConstraintCompiler.same($0, id) }),
                               let targetSlot = target.columns.firstIndex(where: { SQLConstraintCompiler.same($0.id, targetIDs[position]) }) {
                                value = newRow.values[targetSlot]
                            } else if action == .setDefault, case .input(let expression) = source.columns[slot].policy, let expression {
                                let binder = try SQLConstraintCompiler.binder(relation: source, physicalSchema: lease.schema, workMeter: meter)
                                let bound = try binder.bindScalarAssignment(expression, to: SQLConstraintCompiler.columnType(source.columns[slot], relation: source, physicalSchema: lease.schema))
                                let bindings = lease.sqlCatalog.sequenceDefaultBindings(relationID: source.id,
                                    columnID: source.columns[slot].id)
                                let result = try await SQLSequenceRuntime.$defaultBindings.withValue(bindings) {
                                    try await DatabaseContext.evaluateCanonicalScalar(bound, on: row,
                                        temporalEnvironment: environment, session: try sqlPolicySession(meter: meter), workMeter: meter)
                                }
                                for lease in result.leases { try actionPayloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease }) }
                                value = result.value
                            } else { value = .primitive(.null) }
                            let stored: FieldValue
                            if let type = field.queryType {
                                let maximum = try DatabaseEntityMutationFootprintMeter.encodedValueFootprint(value, type: type, workMeter: meter)
                                let encoded = try DatabaseQueryScopedFieldValue.producing(maximumFootprint: maximum, workMeter: meter, stage: .mutationPlanning) {
                                    try QueryFieldStorageAdapter.encode(value, type: type, limits: SQLConstraintRows.limits(meter))
                                }
                                if let reservation = encoded.retainedReservation {
                                    try actionPayloads.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: reservation) })
                                }
                                stored = encoded.withValue { copy $0 }
                            } else if case .primitive(let scalar) = value { stored = scalar }
                            else { throw QueryValueError.incompatibleType }
                            fields.removeAll { SQLConstraintCompiler.same($0.key, field.name) }
                            fields.append((key: field.name, value: stored))
                        }
                        let actionOwner = try actionPayloads.finish().moveToSharedOwnership(at: .mutationPlanning)
                        defer { withExtendedLifetime(actionOwner) {} }
                        let changed = try FieldObject(consume fields)
                        let generated = try await SQLConstraintRows.row(fields: changed, relation: source, physicalSchema: lease.schema,
                            environment: environment, workMeter: meter, recomputingStored: true)
                        defer { withExtendedLifetime(generated) {} }
                        let finalFields = try SQLConstraintRows.fields(generated, relation: source, physicalSchema: lease.schema,
                            preserving: changed, workMeter: meter)
                        defer { withExtendedLifetime(finalFields) {} }
                        let updated = try runtime.persistedModel(from: finalFields.fields)
                        let updatedIdentity = try runtime.identity(for: updated)
                        if updatedIdentity != identity {
                            try await remove(identity: identity, copy model, precondition: .none, operationID: operationID,
                                source: .sqlAction, dispatchSQLActions: false)
                            try await persist(identity: updatedIdentity, updated, precondition: .notExists, operationID: operationID,
                                source: .sqlAction, dispatchSQLActions: false)
                            try await applySQLReferentialActions(oldModel: copy model, newModel: updated, operationID: operationID)
                        } else {
                            try await persist(identity: updatedIdentity, updated, precondition: .none, operationID: operationID, source: .sqlAction)
                        }
                    }
                }
            }
        }
    }
}


extension DatabaseTransaction {
    package func makeSQLSessionBinding(searchPath: SQLSearchPath, workMeter: DatabaseWorkMeter) throws -> SQLSessionBinding {
        guard state != .closed && state != .failed else { throw lifecycleError(for: state) }
        return SQLSessionBinding(resolution: try SQLReadResolution.make(policy: readPolicy, searchPath: searchPath, workMeter: workMeter),
            databaseName: container.configuration.sqlDatabaseName)
    }
}


extension DatabaseTransaction {
    /// Validates an immutable catalog candidate against the same admitted raw
    /// storage transaction before its metadata can be published.
    func validateSQLCatalog(_ snapshot: SQLCatalogSnapshot, workMeter: DatabaseWorkMeter) async throws {
        try await performOperation { _ in
            let previousMeter = sqlConstraintMeter
            sqlConstraintMeter = workMeter
            defer { sqlConstraintMeter = previousMeter }
            try await validateSQLConstraints(forceDeferred: true, catalogOverride: snapshot, requireValidation: true,
                admissionAgainst: readPolicy.sqlCatalog.catalog)
        }
    }
}


/// An unforgeable-in-public-code capability for one matched referential action.
struct SQLReferentialActionAuthorization: Sendable {
    private let owner: DatabaseTransaction
    private let operationID: UInt64
    private let identity: EntityReference
    private let schemaLease: DatabaseSchemaLease

    fileprivate init(owner: DatabaseTransaction, operationID: UInt64,
        identity: EntityReference, schemaLease: DatabaseSchemaLease) {
        self.owner = owner
        self.operationID = operationID
        self.identity = identity
        self.schemaLease = schemaLease
    }

    func validate(identity: EntityReference, schemaLease: DatabaseSchemaLease) async throws {
        guard self.identity == identity, self.schemaLease === schemaLease else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        try await owner.validateSQLReferentialAction(operationID: operationID, schemaLease: schemaLease)
    }
}

extension DatabaseTransaction {
    fileprivate func validateSQLReferentialAction(operationID: UInt64, schemaLease: DatabaseSchemaLease) throws {
        try ensureActive(operationID, permitsMutation: true)
        guard readPolicy.schemaLease === schemaLease else { throw DatabaseTransactionError.invalidOperationContext }
    }
}


extension DatabaseTransaction {
    private func sqlPolicySession(meter: DatabaseWorkMeter) throws -> SQLSessionBinding {
        let searchPath: SQLSearchPath
        if let inherited = ActiveSQLSession.binding,
           inherited.resolution.policy.authorization == readPolicy.authorization,
           let active = ActiveDatabaseTransactionContext.binding, active.container === container,
           active.databaseTransaction === self {
            searchPath = inherited.resolution.searchPath
        } else { searchPath = .default }
        return try makeSQLSessionBinding(searchPath: searchPath, workMeter: meter)
    }

    private func validateSQLRowPolicies(_ model: borrowing PersistedModel, relation: SQLRelation,
        meter: DatabaseWorkMeter, previousRelation: SQLRelation? = nil, checks: Bool = true) async throws {
        let hasRowChecks = checks && relation.constraints.contains {
            switch $0.kind { case .notNull, .check: return true; default: return false }
        }
        let hasColumnPolicies = relation.columns.contains { column in
            if previousRelation?.columns.contains(column) == true { return false }
            switch column.policy { case .generated, .identity: return true; case .input: return false }
        }
        guard hasRowChecks || hasColumnPolicies else { return }
        let environment = makeSQLTemporalEnvironment()
        let row = try await sqlRow(model, relation: relation, meter: meter)
        if hasRowChecks {
            let binder = try SQLConstraintCompiler.binder(relation: relation, physicalSchema: readPolicy.schema, workMeter: meter)
            for constraint in relation.constraints {
                switch constraint.kind {
                case .notNull(let id):
                    if SQLConstraintRows.nullCount(row, ids: [id], relation: relation) != 0 {
                        throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name)
                    }
                case .check(let expression):
                    let result = try await DatabaseContext.evaluateCanonicalScalar(binder.bindPredicate(expression), on: row,
                        temporalEnvironment: environment, session: try sqlPolicySession(meter: meter), workMeter: meter)
                    if case .primitive(.bool(false)) = result.value {
                        throw SQLConstraintError.violation(relation: relation.id, constraint: constraint.name)
                    }
                default: break
                }
            }
        }
        guard hasColumnPolicies else { return }
        let expected = try await sqlRow(model, relation: relation, meter: meter, recomputingStored: true)
        for (slot, column) in relation.columns.enumerated() {
            if previousRelation?.columns.contains(column) == true { continue }
            if case .identity = column.policy, row.values[slot].isNull {
                throw SQLConstraintError.violation(relation: relation.id, constraint: column.name)
            }
            guard case .generated = column.policy, case .field = column.backing,
                  let type = row.layout.types[slot] else { continue }
            let matches: Bool
            if row.values[slot].isNull || expected.values[slot].isNull {
                matches = row.values[slot].isNull && expected.values[slot].isNull
            } else {
                matches = try CanonicalSQLValueSemantics.compareValues(row.values[slot], expected.values[slot],
                    leftType: type, rightType: type, environment: environment, workMeter: meter) == .equal
            }
            if !matches { throw SQLConstraintError.violation(relation: relation.id, constraint: column.name) }
        }
    }
}
