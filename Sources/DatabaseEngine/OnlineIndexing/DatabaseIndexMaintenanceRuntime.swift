import DatabaseKit
import DatabaseTypes
import StorageKit

public struct DatabaseIndexMaintenanceRuntime: Sendable {
    public static let maximumSliceWorkUnits: UInt64 = 10_000

    private let container: DBContainer
    private let storageLimits: StorageFrameLimits

    public init(
        container: DBContainer,
        storageLimits: StorageFrameLimits = .default
    ) {
        self.container = container
        self.storageLimits = storageLimits
    }

    public func prepareResources(
        entity: String,
        index: String,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> FieldObject {
        let target = try await resolveTarget(
            entity: entity,
            index: index,
            partitions: partitions,
            directoryAccess: .create(transaction)
        )
        return target.partitions
    }

    /// Validates and canonicalizes an index target without creating or opening
    /// storage. Persistent job compilation uses this pure boundary so a job
    /// cannot leave Base-local resources behind before its control record is
    /// committed.
    public func canonicalPartitions(
        entity entityName: String,
        index indexName: String,
        partitions: FieldObject
    ) throws -> FieldObject {
        let target = try resolveDefinition(
            entity: entityName,
            index: indexName,
            partitions: partitions
        )
        return target.binding?.canonicalPartitions() ?? FieldObject()
    }

    public func status(
        entity: String,
        index: String,
        partitions: FieldObject,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexMaintenanceStatus {
        let target = try await resolveTarget(
            entity: entity,
            index: index,
            partitions: partitions,
            directoryAccess: .open(transaction),
            expectedIdentity: expectedIdentity
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: target.subspace
        )
        let persistedState = try await lifecycleStore.persistedState(
            of: index,
            transaction: transaction
        )
        let effectiveState: IndexState
        if let persistedState {
            effectiveState = persistedState
        } else {
            let pending = try await container.pendingSchemaIndexBuilds(
                entity: entity,
                indexes: [index],
                transaction: transaction
            )
            effectiveState = pending.contains(index) ? .writeOnly : .disabled
        }
        let rebuildState = try await loadRebuildState(
            key: rebuildStateKey(target: target),
            entity: entity,
            index: index,
            transaction: transaction
        )
        let rebuildPhase: DatabaseIndexRebuildPhase?
        switch rebuildState?.phase {
        case .building:
            rebuildPhase = .building
        case .complete:
            rebuildPhase = .complete
        case .failed:
            rebuildPhase = .failed
        case nil:
            rebuildPhase = nil
        }
        return DatabaseIndexMaintenanceStatus(
            entity: entity,
            index: index,
            partitions: target.partitions,
            indexState: effectiveState,
            rebuildPhase: rebuildPhase,
            indexedEntityCount: rebuildState?.indexedEntityCount ?? 0,
            detail: rebuildState?.detail
        )
    }

    public func runRebuildSlice(
        entity: String,
        index indexName: String,
        partitions: FieldObject,
        generation: DatabaseTypes.UUID,
        mode: DatabaseIndexRebuildSliceMode,
        maximumWorkUnits: UInt64,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexRebuildSlice {
        guard maximumWorkUnits > 0,
              maximumWorkUnits <= Self.maximumSliceWorkUnits,
              let workLimit = Int(exactly: maximumWorkUnits) else {
            throw DatabaseIndexRebuildError.invalidWorkLimit(maximumWorkUnits)
        }
        let target = try await resolveTarget(
            entity: entity,
            index: indexName,
            partitions: partitions,
            directoryAccess: mode == .start
                ? .create(transaction)
                : .open(transaction),
            expectedIdentity: expectedIdentity
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: target.subspace
        )
        let key = rebuildStateKey(target: target)
        let existingState = try await loadRebuildState(
            key: key,
            entity: entity,
            index: indexName,
            transaction: transaction
        )

        let current: DatabaseIndexRebuildState
        switch mode {
        case .start:
            if let existing = existingState,
               existing.generation == generation,
               existing.phase == .complete {
                return DatabaseIndexRebuildSlice(
                    completedWorkUnits: 0,
                    indexedEntityCount: existing.indexedEntityCount,
                    isComplete: true
                )
            }
            if let existing = existingState,
               existing.generation == generation,
               existing.phase == .building {
                current = existing
                break
            }
            if let existing = existingState, existing.phase == .building {
                throw DatabaseIndexRebuildError.buildAlreadyActive(
                    index: indexName,
                    generation: existing.generation
                )
            }
            try await prepare(
                target: target,
                generation: generation,
                lifecycleStore: lifecycleStore,
                transaction: transaction
            )
            current = DatabaseIndexRebuildState(
                entity: entity,
                index: indexName,
                generation: generation,
                phase: .building
            )
        case .resume:
            guard let existing = existingState,
                  existing.generation == generation,
                  existing.phase == .building else {
                throw DatabaseIndexRebuildError.corruptedRebuildState
            }
            current = existing
        }

        let index = makeIndex(
            descriptor: target.descriptor,
            entity: entity
        )
        let slice = try await DatabaseEntityIndexSliceExecutor.run(
            for: target.entityRuntime,
            container: container,
            storeSubspace: target.subspace,
            index: index,
            lastProcessedKey: current.lastProcessedKey,
            maximumWorkUnits: workLimit,
            transaction: transaction
        )
        let (count, overflow) = current.indexedEntityCount
            .addingReportingOverflow(slice.processed)
        guard !overflow else {
            throw DatabaseIndexRebuildError.entityCountOverflow
        }

        let updated: DatabaseIndexRebuildState
        if slice.hasMore {
            guard let lastKey = slice.lastProcessedKey else {
                throw DatabaseIndexRebuildError.corruptedRebuildState
            }
            updated = DatabaseIndexRebuildState(
                entity: entity,
                index: indexName,
                generation: generation,
                phase: .building,
                lastProcessedKey: lastKey,
                indexedEntityCount: count
            )
        } else {
            try await target.entityRuntime.finalizeIndex(
                container: container,
                storeSubspace: target.subspace,
                index: index,
                configurations: container.runtimeConfiguration
                    .indexConfigurations(named: indexName),
                transaction: transaction
            )
            if index.isUnique {
                let tracker = UniquenessViolationTracker(
                    container: container,
                    metadataSubspace: target.subspace.subspace(SubspaceKey.metadata)
                )
                guard try await tracker.hasViolations(
                    indexName: indexName,
                    transaction: transaction
                ) == false else {
                    throw DatabaseIndexRebuildError.uniquenessViolation(
                        index: indexName
                    )
                }
            }
            try await lifecycleStore.makeReadable(
                indexName,
                transaction: transaction
            )
            updated = DatabaseIndexRebuildState(
                entity: entity,
                index: indexName,
                generation: generation,
                phase: .complete,
                lastProcessedKey: slice.lastProcessedKey,
                indexedEntityCount: count
            )
        }
        try transaction.setValue(
            try StorageFrameCodec.encode(
                updated,
                limits: storageLimits
            ),
            for: key
        )
        return DatabaseIndexRebuildSlice(
            completedWorkUnits: slice.processed,
            indexedEntityCount: count,
            isComplete: !slice.hasMore
        )
    }

    public func markFailed(
        entity: String,
        index: String,
        partitions: FieldObject,
        generation: DatabaseTypes.UUID,
        detail: String,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws {
        let target = try await resolveTarget(
            entity: entity,
            index: index,
            partitions: partitions,
            directoryAccess: .open(transaction),
            expectedIdentity: expectedIdentity
        )
        let key = rebuildStateKey(target: target)
        guard let state = try await loadRebuildState(
            key: key,
            entity: entity,
            index: index,
            transaction: transaction
        ) else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        guard state.generation == generation else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        if state.phase == .failed {
            return
        }
        guard state.phase == .building else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        let failed = DatabaseIndexRebuildState(
            entity: entity,
            index: index,
            generation: generation,
            phase: .failed,
            lastProcessedKey: state.lastProcessedKey,
            indexedEntityCount: state.indexedEntityCount,
            detail: detail
        )
        try transaction.setValue(
            try StorageFrameCodec.encode(
                failed,
                limits: storageLimits
            ),
            for: key
        )
    }

    private func prepare(
        target: Target,
        generation: DatabaseTypes.UUID,
        lifecycleStore: IndexLifecycleStore,
        transaction: any TransactionAccess
    ) async throws {
        try await lifecycleStore.disable(
            target.descriptor.name,
            transaction: transaction
        )
        let physicalIndexSubspace = try lifecycleStore.indexSubspace(
            for: target.descriptor.name
        )
        let indexRange = physicalIndexSubspace.range()
        try transaction.clearRange(
            beginKey: indexRange.begin,
            endKey: indexRange.end
        )
        try transaction.clear(
            key:
                physicalIndexSubspace
                .subspace("_progress")
                .pack(Tuple(target.descriptor.name))
        )
        let tracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: target.subspace.subspace(SubspaceKey.metadata)
        )
        try await tracker.clearAllViolations(
            indexName: target.descriptor.name,
            transaction: transaction
        )
        try await lifecycleStore.enable(
            target.descriptor.name,
            transaction: transaction
        )
        let state = DatabaseIndexRebuildState(
            entity: target.entity,
            index: target.descriptor.name,
            generation: generation,
            phase: .building
        )
        try transaction.setValue(
            try StorageFrameCodec.encode(
                state,
                limits: storageLimits
            ),
            for: rebuildStateKey(target: target)
        )
    }

    private func loadRebuildState(
        key: ByteString,
        entity: String,
        index: String,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexRebuildState? {
        guard let bytes = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return nil
        }
        let state: DatabaseIndexRebuildState
        do {
            state = try StorageFrameCodec.decode(
                DatabaseIndexRebuildState.self,
                from: bytes,
                limits: storageLimits
            )
        } catch {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        guard state.entity == entity, state.index == index else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        return state
    }

    private func resolveTarget(
        entity entityName: String,
        index indexName: String,
        partitions: FieldObject,
        directoryAccess: DirectoryAccess,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil
    ) async throws -> Target {
        let definition = try resolveDefinition(
            entity: entityName,
            index: indexName,
            partitions: partitions
        )
        let subspace: Subspace
        switch directoryAccess {
        case .create(let transaction):
            subspace = try await container.resolveDirectory(
                for: definition.entity,
                path: definition.binding,
                transaction: transaction
            )
        case .open(let transaction):
            subspace = try await container.openDirectory(
                for: definition.entity,
                path: definition.binding,
                transaction: transaction
            )
        }
        let target = Target(
            entity: entityName,
            entityRuntime: definition.entityRuntime,
            descriptor: definition.descriptor,
            storageIdentity: try DatabaseIndexStorageIdentity.resolve(
                named: indexName,
                in: container.schema,
                physicalLayout: try physicalLayout(for: indexName)
            ),
            partitions: definition.binding?.canonicalPartitions()
                ?? FieldObject(),
            subspace: subspace
        )
        if let expectedIdentity,
            target.storageIdentity != expectedIdentity
        {
            throw DatabaseIndexRebuildError.indexGenerationMismatch(
                indexName
            )
        }
        return target
    }

    private func resolveDefinition(
        entity entityName: String,
        index indexName: String,
        partitions: FieldObject
    ) throws -> TargetDefinition {
        guard let entity = container.schema.entities.first(where: {
            $0.name == entityName
        }) else {
            throw DatabaseIndexRebuildError.entityNotFound(entityName)
        }
        guard let entityRuntime = container.runtimeConfiguration
            .entityRuntimes.registration(named: entityName) else {
            throw DatabaseIndexRebuildError.compiledTypeMissing(entityName)
        }
        guard let descriptor = entity.indexDescriptors.first(where: {
            $0.name == indexName
        }) else {
            throw DatabaseIndexRebuildError.indexNotFound(
                entity: entityName,
                index: indexName
            )
        }
        let binding = try CanonicalPartitionBinding.makeAnyBinding(
            for: entity,
            partitions: partitions
        )
        return TargetDefinition(
            entity: entity,
            entityRuntime: entityRuntime,
            descriptor: descriptor,
            binding: binding
        )
    }

    private enum DirectoryAccess: Sendable {
        case create(any TransactionAccess)
        case open(any TransactionAccess)
    }

    private func rebuildStateKey(target: Target) -> ByteString {
        target.subspace
            .subspace(SubspaceKey.metadata)
            .subspace("index-rebuild")
            .pack(Tuple(
                    target.storageIdentity.name,
                    target.storageIdentity.definitionFingerprint.bytes,
                    target.storageIdentity.layoutFingerprint
                )
            )
    }

    private func makeIndex(
        descriptor: IndexDescriptor,
        entity: String
    ) -> ResolvedIndex {
        let expression = KeyExpressionFactory.from(
            keyPaths: descriptor.fieldNames
        )
        return ResolvedIndex(
            descriptor: descriptor,
            rootExpression: expression,
            itemTypes: Set([entity]),
        )
    }

    private func physicalLayout(
        for indexName: String
    ) throws -> IndexPhysicalLayout {
        guard let layout = container.indexPhysicalLayouts[indexName] else {
            throw DatabaseIndexStorageIdentityError.physicalLayoutNotResolved(
                indexName
            )
        }
        return layout
    }

    private struct Target {
        let entity: String
        let entityRuntime: EntityRuntimeRegistration
        let descriptor: IndexDescriptor
        let storageIdentity: DatabaseIndexStorageIdentity
        let partitions: FieldObject
        let subspace: Subspace
    }

    private struct TargetDefinition {
        let entity: Schema.Entity
        let entityRuntime: EntityRuntimeRegistration
        let descriptor: IndexDescriptor
        let binding: AnyDirectoryPath?
    }
}
