import DatabaseKit
import DatabaseTypes
import StorageKit

/// Owns database semantics performed within one storage transaction attempt.
///
/// Public operations reject overlapping entry. Derived mutation maintainers
/// receive an operation-scoped capability that permits recursive persistence
/// while preserving this owner, shared storage access, and final validation.
/// `TransactionRunner` alone owns commit, cancellation, and retry lifecycle.
public final actor DatabaseTransaction: DatabaseTransactionWriting {
    private enum State: Sendable, Equatable {
        case open
        case executing(UInt64)
        case preparingCommit(UInt64)
        case closed
    }

    private enum MutationSource: Sendable, Equatable {
        case requested
        case derived
    }

    nonisolated package let storageAccess: any TransactionAccess

    private let container: DBContainer
    private let mutationMaintenanceService: PersistableMutationMaintenanceService
    private let transactionScope = DatabaseTransactionScope()
    private var validationScope: DatabaseTransactionScope?
    private var state: State = .open
    private var nextOperationID: UInt64 = 1
    private var subspaceCache = DatabaseStoreCache<ResolvedSubspaces>()
    private var scheduledDeletions = EntityReferenceSet()
    private var scheduledWrites = EntityReferenceSet()
    private var activeMutationIdentities = EntityReferenceSet()
    private var mutationJournal = TransactionMutationJournal()

    private struct ResolvedSubspaces: Sendable {
        let items: Subspace
        let blobs: Subspace
        let partitionPath: [String]
    }

    package init(
        storageAccess: any TransactionAccess,
        container: DBContainer
    ) {
        self.storageAccess = storageAccess
        self.container = container
        self.mutationMaintenanceService =
            PersistableMutationMaintenanceService(
                maintainers: container.runtimeConfiguration
                    .persistableMutationMaintainers
            )
    }

    // MARK: - Typed reads

    public func fetch<Model: Persistable>(
        _ type: Model.Type,
        identifiedBy id: Model.ID,
        consistency: DatabaseReadConsistency
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
        consistency: DatabaseReadConsistency
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
        consistency: DatabaseReadConsistency = .serializable
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
        consistency: DatabaseReadConsistency
    ) async throws -> sending DatabaseScanPage<Model> {
        return try await performOperation { _ in
            guard limit > 0, limit < Int.max else {
                throw DatabaseTransactionError.invalidLimit(limit)
            }
            try container.securityDelegate?.evaluateList(
                entity: Model.persistableType,
                limit: limit,
                offset: nil,
                orderBy: nil
            )
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

            var entries = try await storageAccess.collectRange(
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
                let model = try persistedModel.decode(as: Model.self)
                try container.securityDelegate?.evaluateGet(persistedModel)
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
                model,
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
                model,
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
                model,
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
                model,
                precondition: .exists,
                operationID: operationID,
                source: .requested
            )
        }
    }

    // MARK: - Package persistence operations

    package func loadPersistedModel(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?
    ) async throws -> (any Persistable)? {
        try await performOperation { _ in
            try await loadPersistedModelUnchecked(
                entity: entity,
                id: id,
                partition: partition
            )
        }
    }

    package func fetchPersistedModel(
        identifiedBy identity: EntityReference
    ) async throws -> (any Persistable)? {
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
        limit: Int
    ) async throws -> [any Persistable] {
        try await performOperation { _ in
            try await scanPersistedModelsUnchecked(
                entity: entity,
                partition: partition,
                limit: limit
            )
        }
    }

    package func savePersistedModel(
        _ model: any Persistable,
        precondition: WritePrecondition
    ) async throws {
        try await performOperation { operationID in
            try await persist(
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    package func deletePersistedModel(
        _ model: any Persistable,
        precondition: WritePrecondition
    ) async throws {
        try await performOperation { operationID in
            try await remove(
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    package func apply(
        _ mutations: [PersistableMutation]
    ) async throws {
        try await performOperation { operationID in
            try await apply(
                mutations,
                operationID: operationID
            )
        }
    }

    package func persistedMutationEffects() throws
        -> [PersistableMutationEffect] {
        guard state == .open else {
            throw lifecycleError(for: state)
        }
        return mutationJournal.persistedEffects()
    }

    package func fetchPersistedModel(
        identifiedBy identity: EntityReference,
        within operationID: UInt64
    ) async throws -> (any Persistable)? {
        do {
            try ensureActive(operationID, permitsMutation: false)
            let resolved = try resolve(identity)
            return try await loadPersistedModelUnchecked(
                entity: identity.entity,
                id: resolved.id,
                partition: resolved.partition
            )
        } catch {
            state = .closed
            throw error
        }
    }

    package func savePersistedModel(
        _ model: any Persistable,
        precondition: WritePrecondition,
        within operationID: UInt64
    ) async throws {
        do {
            try ensureActive(operationID, permitsMutation: true)
            try await persist(
                model,
                precondition: precondition,
                operationID: operationID,
                source: .derived
            )
        } catch {
            state = .closed
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
            let runtime = try entityRuntime(named: model.entity)
            let decoded = try runtime.decode(model)
            try await persist(
                decoded,
                precondition: precondition,
                operationID: operationID,
                source: .derived
            )
        } catch {
            state = .closed
            throw error
        }
    }

    package func deletePersistedModel(
        _ model: any Persistable,
        precondition: WritePrecondition,
        within operationID: UInt64
    ) async throws {
        do {
            try ensureActive(operationID, permitsMutation: true)
            try await remove(
                model,
                precondition: precondition,
                operationID: operationID,
                source: .derived
            )
        } catch {
            state = .closed
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
        await transactionScope.closeAndWait()
        guard state == .open else {
            throw lifecycleError(for: state)
        }
        let operationID = try issueOperationID()
        state = .preparingCommit(operationID)
        let validationScope = DatabaseTransactionScope()
        self.validationScope = validationScope
        let context = PersistableValidationContext(
            schema: container.schema,
            transaction: self,
            operationID: operationID,
            scope: validationScope
        )
        do {
            try await mutationMaintenanceService.validateFinalState(
                of: mutationJournal.currentModels(),
                context: context
            )
            await context.closeAndWait()
            guard state == .preparingCommit(operationID) else {
                throw lifecycleError(for: state)
            }
            self.validationScope = nil
            state = .closed
        } catch {
            await context.closeAndWait()
            self.validationScope = nil
            state = .closed
            throw error
        }
    }

    package func invalidate() async {
        state = .closed
        await transactionScope.closeAndWait()
        await validationScope?.closeAndWait()
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
            let identity = try EntityReferenceEncoder.encode(
                mutation.model
            )
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
            guard case .save(let model, let precondition) = mutation else {
                continue
            }
            try await persist(
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
        for mutation in mutations {
            guard case .delete(let model, let precondition) = mutation else {
                continue
            }
            try await remove(
                model,
                precondition: precondition,
                operationID: operationID,
                source: .requested
            )
        }
    }

    private func persist(
        _ model: any Persistable,
        precondition: WritePrecondition,
        operationID: UInt64,
        source: MutationSource
    ) async throws {
        let identity = try EntityReferenceEncoder.encode(model)
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
        let modelType = type(of: model)
        let partition = try partition(for: model)
        let store = try await container.store(
            for: modelType,
            path: partition,
            transaction: storageAccess
        )
        let write = try await store.save(
            model,
            precondition: precondition,
            transaction: storageAccess
        )
        try await PolymorphicProjectionMaintainer(
            container: container
        ).update(
            write,
            transaction: storageAccess
        )
        let mutationContext = makeMutationContext(operationID: operationID)
        do {
            try await mutationMaintenanceService.update(
                oldModel: write.previousModel,
                newModel: model,
                context: mutationContext
            )
            await mutationContext.closeAndWait()
        } catch {
            await mutationContext.closeAndWait()
            throw error
        }
        try ensureActive(operationID, permitsMutation: true)
        updateMutationJournal(
            identity: identity,
            previousModel: write.previousModel,
            currentModel: model
        )
    }

    private func remove(
        _ model: any Persistable,
        precondition: WritePrecondition,
        operationID: UInt64,
        source: MutationSource
    ) async throws {
        let identity = try EntityReferenceEncoder.encode(model)
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
        let modelType = type(of: model)
        let partition = try partition(for: model)
        let store = try await container.store(
            for: modelType,
            path: partition,
            transaction: storageAccess
        )
        guard let persistedModel = try await store.delete(
            model,
            precondition: precondition,
            transaction: storageAccess
        ) else {
            return
        }
        try await PolymorphicProjectionMaintainer(
            container: container
        ).remove(
            persistedModel,
            transaction: storageAccess
        )
        let mutationContext = makeMutationContext(operationID: operationID)
        do {
            try await mutationMaintenanceService.update(
                oldModel: persistedModel,
                newModel: nil,
                context: mutationContext
            )
            await mutationContext.closeAndWait()
        } catch {
            await mutationContext.closeAndWait()
            throw error
        }
        try ensureActive(operationID, permitsMutation: true)
        updateMutationJournal(
            identity: identity,
            previousModel: persistedModel,
            currentModel: nil
        )
    }

    private func updateMutationJournal(
        identity: EntityReference,
        previousModel: (any Persistable)?,
        currentModel: (any Persistable)?
    ) {
        mutationJournal.record(
            identity: identity,
            previousModel: previousModel,
            currentModel: currentModel
        )
    }

    private func loadPersistedModelUnchecked(
        entity: String,
        id: Tuple,
        partition: AnyDirectoryPath?
    ) async throws -> (any Persistable)? {
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
        let model = try runtime.decode(persistedModel)
        try container.securityDelegate?.evaluateGet(persistedModel)
        return model
    }

    private func scanPersistedModelsUnchecked(
        entity: String,
        partition: AnyDirectoryPath?,
        limit: Int
    ) async throws -> [any Persistable] {
        guard limit > 0 else {
            throw DatabaseTransactionError.invalidLimit(limit)
        }
        let runtime = try entityRuntime(named: entity)
        if runtime.entity.hasDynamicDirectory, partition == nil {
            throw DirectoryPathError.dynamicFieldsRequired(
                typeName: entity,
                fields: runtime.entity.dynamicFieldNames
            )
        }
        try container.securityDelegate?.evaluateList(
            entity: runtime.entity.name,
            limit: limit,
            offset: nil,
            orderBy: nil
        )
        guard let subspaces = try await openSubspaces(
            for: runtime.entity,
            partition: partition
        ) else {
            return []
        }
        let (begin, end) = subspaces.items.subspace(entity).range()
        let storage = container.itemStorageFactory.make(
            transaction: storageAccess,
            blobsSubspace: subspaces.blobs
        )
        var models: [any Persistable] = []
        models.reserveCapacity(limit)
        var iterator = storage.scan(
            begin: begin,
            end: end,
            snapshot: false,
            limit: limit
        ).makeAsyncIterator()
        while let (_, data) = try await iterator.next() {
            let persistedModel = try DataAccess.deserializePersistedModel(
                data,
                expectedEntity: entity
            )
            let model = try runtime.decode(persistedModel)
            try container.securityDelegate?.evaluateGet(persistedModel)
            models.append(model)
        }
        return models
    }

    // MARK: - Operation lifecycle

    private func performOperation<Value: ~Copyable & Sendable>(
        _ operation: (UInt64) async throws -> sending Value
    ) async throws -> sending Value {
        try transactionScope.enter()
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
            guard state == .executing(issuedOperationID) else {
                throw lifecycleError(for: state)
            }
            state = .open
            transactionScope.leave()
            return value
        } catch {
            if let operationID, state == .executing(operationID) {
                state = .closed
            }
            transactionScope.leave()
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
        case .preparingCommit, .closed:
            return .closed
        }
    }

    private func makeMutationContext(
        operationID: UInt64
    ) -> PersistableMutationContext {
        PersistableMutationContext(
            schema: container.schema,
            transaction: self,
            operationID: operationID,
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
        let cacheKey = DatabaseStoreCacheKey(
            entity: entity.name,
            components: partitionPath
        )
        if let cached = subspaceCache.value(for: cacheKey) {
            return cached
        }
        guard try await container.engine.namespaceResolver.namespaceExists(
            path: partitionPath,
            transaction: storageAccess
        ) else {
            return nil
        }
        let root = try await container.openDirectory(
            for: entity,
            path: path,
            transaction: storageAccess
        )
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
        consistency: DatabaseReadConsistency
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
        let model = try persistedModel.decode(as: Model.self)
        try container.securityDelegate?.evaluateGet(persistedModel)
        return model
    }

    private func entityRuntime(
        named entity: String
    ) throws -> EntityRuntimeRegistration {
        guard container.schema.entity(named: entity) != nil else {
            throw DatabaseTransactionError.unknownEntity(entity)
        }
        guard let runtime = container.runtimeConfiguration.entityRuntimes.registration(
            named: entity
        ) else {
            throw DatabaseTransactionError.entityHasNoPersistableType(entity)
        }
        return runtime
    }

    private func resolve(
        _ identity: EntityReference
    ) throws -> (id: Tuple, partition: AnyDirectoryPath?) {
        let runtime = try entityRuntime(named: identity.entity)
        guard let entity = container.schema.entity(named: identity.entity) else {
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
            return (id, partition)
        } catch {
            throw DatabaseTransactionError.invalidIdentity(
                entity: identity.entity,
                reason: "identity resolution failed"
            )
        }
    }

    private func partition(
        for model: any Persistable
    ) throws -> AnyDirectoryPath? {
        let entityName = type(of: model).persistableType
        guard let entity = container.schema.entity(named: entityName) else {
            throw DatabaseTransactionError.unknownEntity(entityName)
        }
        guard entity.hasDynamicDirectory else {
            return nil
        }
        let persisted = try model.persistedFields()
        var partitions: [(key: String, value: FieldValue)] = []
        partitions.reserveCapacity(entity.dynamicFieldNames.count)
        for name in entity.dynamicFieldNames {
            guard let value = persisted.first(where: {
                $0.name == name
            })?.value else {
                throw DirectoryPathError.missingFields([name])
            }
            partitions.append((key: name, value: value))
        }
        return try AnyDirectoryPath(
            entity: entity,
            partitions: FieldObject(partitions)
        )
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
