import DatabaseKit
import DatabaseTypes
import StorageKit

package struct DatabaseIndexMaintenanceRuntime: Sendable {
    package static let maximumSliceWorkUnits: UInt64 = 10_000

    private let container: DBContainer
    private let storageLimits: StorageFrameLimits

    package init(
        container: DBContainer,
        storageLimits: StorageFrameLimits = .default
    ) {
        self.container = container
        self.storageLimits = storageLimits
    }

    package func prepareResources(
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
    package func canonicalPartitions(
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

    package func status(
        entity: String,
        index: String,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexMaintenanceStatus {
        let target = try await resolveTarget(
            entity: entity,
            index: index,
            partitions: partitions,
            directoryAccess: .open(transaction)
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
        return DatabaseIndexMaintenanceStatus(
            entity: entity,
            index: index,
            partitions: target.partitions,
            indexState: effectiveState,
            rebuildState: try await loadRebuildState(
                key: rebuildStateKey(target: target),
                entity: entity,
                index: index,
                transaction: transaction
            )
        )
    }

    package func runRebuildSlice(
        entity: String,
        index indexName: String,
        partitions: FieldObject,
        generation: DatabaseTypes.UUID,
        mode: DatabaseIndexRebuildSliceMode,
        maximumWorkUnits: UInt64,
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
                : .open(transaction)
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
                configurations: container.indexConfigurations[indexName] ?? [],
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

    package func markFailed(
        entity: String,
        index: String,
        partitions: FieldObject,
        generation: DatabaseTypes.UUID,
        detail: String,
        transaction: any TransactionAccess
    ) async throws {
        let target = try await resolveTarget(
            entity: entity,
            index: index,
            partitions: partitions,
            directoryAccess: .open(transaction)
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
        let indexRange = target.subspace
            .subspace(SubspaceKey.indexes)
            .subspace(target.descriptor.name)
            .range()
        try transaction.clearRange(
            beginKey: indexRange.begin,
            endKey: indexRange.end
        )
        try transaction.clear(
            key: target.subspace
                .subspace(SubspaceKey.indexes)
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
        directoryAccess: DirectoryAccess
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
        return Target(
            entity: entityName,
            entityRuntime: definition.entityRuntime,
            descriptor: definition.descriptor,
            partitions: definition.binding?.canonicalPartitions()
                ?? FieldObject(),
            subspace: subspace
        )
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
            .pack(Tuple(target.descriptor.name))
    }

    private func makeIndex(
        descriptor: IndexDescriptor,
        entity: String
    ) -> Index {
        let expression = KeyExpressionFactory.from(
            keyPaths: descriptor.fieldNames
        )
        return Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: expression,
            subspaceKey: descriptor.name,
            itemTypes: Set([entity]),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
        )
    }

    private struct Target {
        let entity: String
        let entityRuntime: EntityRuntimeRegistration
        let descriptor: IndexDescriptor
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
