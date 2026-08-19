import DatabaseKit
import DatabaseTypes
import StorageKit

/// Resumable maintenance for one index owned by a polymorphic group.
public struct DatabasePolymorphicIndexMaintenanceRuntime: Sendable {
    private let container: DBContainer
    private let storageLimits: StorageFrameLimits

    public init(
        container: DBContainer,
        storageLimits: StorageFrameLimits = .default
    ) {
        self.container = container
        self.storageLimits = storageLimits
    }

    public func status(
        group groupIdentifier: String,
        index indexName: String,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexMaintenanceStatus {
        let target = try await resolveTarget(
            group: groupIdentifier,
            index: indexName,
            expectedIdentity: expectedIdentity,
            transaction: transaction
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: target.subspace
        )
        let persistedState = try await lifecycleStore.persistedState(
            of: indexName,
            transaction: transaction
        )
        let effectiveState: IndexState
        if let persistedState {
            effectiveState = persistedState
        } else {
            let pending = try await container
                .pendingSchemaPolymorphicIndexBuilds(
                    group: groupIdentifier,
                    indexes: [indexName],
                    transaction: transaction
                )
            effectiveState = pending.contains(indexName)
                ? .writeOnly
                : .disabled
        }
        let rebuildState = try await loadRebuildState(
            key: rebuildStateKey(target),
            scope: groupIdentifier,
            index: indexName,
            transaction: transaction
        )
        let rebuildPhase: DatabaseIndexRebuildPhase?
        switch rebuildState?.phase {
        case .building: rebuildPhase = .building
        case .complete: rebuildPhase = .complete
        case .failed: rebuildPhase = .failed
        case nil: rebuildPhase = nil
        }
        return DatabaseIndexMaintenanceStatus(
            entity: groupIdentifier,
            index: indexName,
            partitions: FieldObject(),
            indexState: effectiveState,
            rebuildPhase: rebuildPhase,
            indexedEntityCount: rebuildState?.indexedEntityCount ?? 0,
            detail: rebuildState?.detail
        )
    }

    public func runRebuildSlice(
        group groupIdentifier: String,
        index indexName: String,
        generation: DatabaseTypes.UUID,
        mode: DatabaseIndexRebuildSliceMode,
        maximumWorkUnits: UInt64,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws -> DatabaseIndexRebuildSlice {
        guard maximumWorkUnits > 0,
              maximumWorkUnits
                <= DatabaseIndexMaintenanceRuntime.maximumSliceWorkUnits,
              let workLimit = Int(exactly: maximumWorkUnits) else {
            throw DatabaseIndexRebuildError.invalidWorkLimit(maximumWorkUnits)
        }
        let target = try await resolveTarget(
            group: groupIdentifier,
            index: indexName,
            expectedIdentity: expectedIdentity,
            transaction: transaction
        )
        let lifecycleStore = IndexLifecycleStore(
            container: container,
            subspace: target.subspace
        )
        let stateKey = rebuildStateKey(target)
        let existingState = try await loadRebuildState(
            key: stateKey,
            scope: groupIdentifier,
            index: indexName,
            transaction: transaction
        )

        let current: DatabaseIndexRebuildState
        switch mode {
        case .start:
            if let existingState,
               existingState.generation == generation,
               existingState.phase == .complete {
                return DatabaseIndexRebuildSlice(
                    completedWorkUnits: 0,
                    indexedEntityCount: existingState.indexedEntityCount,
                    isComplete: true
                )
            }
            if let existingState,
               existingState.generation == generation,
               existingState.phase == .building {
                current = existingState
            } else {
                if let existingState, existingState.phase == .building {
                    throw DatabaseIndexRebuildError.buildAlreadyActive(
                        index: indexName,
                        generation: existingState.generation
                    )
                }
                try await prepare(
                    target,
                    generation: generation,
                    lifecycleStore: lifecycleStore,
                    transaction: transaction
                )
                current = DatabaseIndexRebuildState(
                    entity: groupIdentifier,
                    index: indexName,
                    generation: generation,
                    phase: .building
                )
            }
        case .resume:
            guard let existingState,
                  existingState.generation == generation,
                  existingState.phase == .building else {
                throw DatabaseIndexRebuildError.corruptedRebuildState
            }
            current = existingState
        }

        let slice = try await buildSlice(
            target,
            lastProcessedKey: current.lastProcessedKey,
            maximumWorkUnits: workLimit,
            lifecycleStore: lifecycleStore,
            transaction: transaction
        )
        let (count, overflow) = current.indexedEntityCount
            .addingReportingOverflow(slice.processed)
        guard !overflow else {
            throw DatabaseIndexRebuildError.entityCountOverflow
        }

        let updated: DatabaseIndexRebuildState
        if slice.hasMore {
            guard let lastProcessedKey = slice.lastProcessedKey else {
                throw DatabaseIndexRebuildError.corruptedRebuildState
            }
            updated = DatabaseIndexRebuildState(
                entity: groupIdentifier,
                index: indexName,
                generation: generation,
                phase: .building,
                lastProcessedKey: lastProcessedKey,
                indexedEntityCount: count
            )
        } else {
            try await target.finalization.runtime.finalizeIndex(
                container: container,
                storeSubspace: target.subspace,
                index: target.finalization.index,
                configurations: container.runtimeConfiguration
                    .indexConfigurations(named: indexName),
                transaction: transaction
            )
            if target.finalization.index.isUnique {
                let tracker = UniquenessViolationTracker(
                    container: container,
                    metadataSubspace: target.subspace.subspace(
                        SubspaceKey.metadata
                    )
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
                entity: groupIdentifier,
                index: indexName,
                generation: generation,
                phase: .complete,
                lastProcessedKey: slice.lastProcessedKey,
                indexedEntityCount: count
            )
        }
        try transaction.setValue(
            try StorageFrameCodec.encode(updated, limits: storageLimits),
            for: stateKey
        )
        return DatabaseIndexRebuildSlice(
            completedWorkUnits: slice.processed,
            indexedEntityCount: count,
            isComplete: !slice.hasMore
        )
    }

    public func markFailed(
        group groupIdentifier: String,
        index indexName: String,
        generation: DatabaseTypes.UUID,
        detail: String,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws {
        let target = try await resolveTarget(
            group: groupIdentifier,
            index: indexName,
            expectedIdentity: expectedIdentity,
            transaction: transaction
        )
        let key = rebuildStateKey(target)
        guard let state = try await loadRebuildState(
            key: key,
            scope: groupIdentifier,
            index: indexName,
            transaction: transaction
        ), state.generation == generation else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        if state.phase == .failed { return }
        guard state.phase == .building else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        try transaction.setValue(
            try StorageFrameCodec.encode(
                DatabaseIndexRebuildState(
                    entity: groupIdentifier,
                    index: indexName,
                    generation: generation,
                    phase: .failed,
                    lastProcessedKey: state.lastProcessedKey,
                    indexedEntityCount: state.indexedEntityCount,
                    detail: detail
                ),
                limits: storageLimits
            ),
            for: key
        )
    }

    private func prepare(
        _ target: Target,
        generation: DatabaseTypes.UUID,
        lifecycleStore: IndexLifecycleStore,
        transaction: any TransactionAccess
    ) async throws {
        try await lifecycleStore.disable(
            target.declaration.name,
            transaction: transaction
        )
        let physicalSubspace = try lifecycleStore.indexSubspace(
            for: target.declaration.name
        )
        let range = physicalSubspace.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)
        let tracker = UniquenessViolationTracker(
            container: container,
            metadataSubspace: target.subspace.subspace(SubspaceKey.metadata)
        )
        try await tracker.clearAllViolations(
            indexName: target.declaration.name,
            transaction: transaction
        )
        try await lifecycleStore.enable(
            target.declaration.name,
            transaction: transaction
        )
        try transaction.setValue(
            try StorageFrameCodec.encode(
                DatabaseIndexRebuildState(
                    entity: target.group.identifier,
                    index: target.declaration.name,
                    generation: generation,
                    phase: .building
                ),
                limits: storageLimits
            ),
            for: rebuildStateKey(target)
        )
    }

    private func buildSlice(
        _ target: Target,
        lastProcessedKey: ByteString?,
        maximumWorkUnits: Int,
        lifecycleStore: IndexLifecycleStore,
        transaction: any TransactionAccess
    ) async throws -> SliceResult {
        let itemSubspace = target.subspace.subspace(SubspaceKey.items)
        let range = itemSubspace.range()
        let begin = lastProcessedKey.map { $0.appending(0) } ?? range.begin
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: target.subspace.subspace(SubspaceKey.blobs)
        )
        let sequence = storage.scan(
            begin: begin,
            end: range.end,
            snapshot: false,
            limit: maximumWorkUnits + 1
        )
        var batch: [(key: ByteString, value: ByteString)] = []
        batch.reserveCapacity(maximumWorkUnits)
        var iterator = sequence.makeAsyncIterator()
        while let (key, value) = try await iterator.next() {
            batch.append((key: key, value: value))
        }
        let hasMore = batch.count > maximumWorkUnits
        if hasMore {
            batch.removeLast()
        }

        let maintenance = IndexMaintenanceService(
            indexLifecycleStore: lifecycleStore,
            violationTracker: UniquenessViolationTracker(
                container: container,
                metadataSubspace: target.subspace.subspace(
                    SubspaceKey.metadata
                )
            ),
            configurations: container.runtimeConfiguration
                .indexConfigurations(named: target.declaration.name)
        )
        var lastKey: ByteString?
        for element in batch {
            let identifier = try itemSubspace.unpack(element.key)
            guard identifier.count > 0,
                  case .signedInteger(let typeCode) = try identifier.value(at: 0),
                  let member = target.membersByTypeCode[typeCode] else {
                throw DatabaseIndexRebuildError.invalidContinuation
            }
            let model = try member.runtime.canonicalized(
                DataAccess.deserializePersistedModel(
                    element.value,
                    expectedEntity: member.runtime.entity.name
                )
            )
            try await maintenance.updateIndexesUntyped(
                runtime: member.runtime,
                oldModel: nil,
                newModel: model,
                id: identifier,
                descriptors: [member.descriptor],
                logicalTypeName: target.group.identifier,
                transaction: transaction
            )
            lastKey = element.key
        }
        return SliceResult(
            processed: UInt64(batch.count),
            lastProcessedKey: lastKey,
            hasMore: hasMore
        )
    }

    private func resolveTarget(
        group groupIdentifier: String,
        index indexName: String,
        expectedIdentity: DatabaseIndexStorageIdentity? = nil,
        transaction: any TransactionAccess
    ) async throws -> Target {
        guard let group = container.schema.polymorphicGroup(
            identifier: groupIdentifier
        ) else {
            throw DatabaseIndexRebuildError.polymorphicGroupNotFound(
                groupIdentifier
            )
        }
        guard let declaration = group.indexes.first(where: {
            $0.name == indexName
        }) else {
            throw DatabaseIndexRebuildError.indexNotFound(
                entity: groupIdentifier,
                index: indexName
            )
        }
        let subspace = try await container.resolvePolymorphicDirectory(
            for: groupIdentifier,
            transaction: transaction
        )
        var membersByTypeCode: [Int64: Member] = [:]
        var finalization: Finalization?
        for memberTypeName in group.memberTypeNames {
            guard let runtime = container.runtimeConfiguration.entityRuntimes
                .registration(named: memberTypeName) else {
                throw DatabaseIndexRebuildError.compiledTypeMissing(
                    memberTypeName
                )
            }
            guard let descriptor = container.schema
                .polymorphicIndexDescriptors(
                    identifier: groupIdentifier,
                    memberTypeName: memberTypeName
                )
                .first(where: { $0.name == indexName }) else {
                throw DatabaseIndexRebuildError.indexNotFound(
                    entity: memberTypeName,
                    index: indexName
                )
            }
            let member = Member(runtime: runtime, descriptor: descriptor)
            let typeCode = PolymorphicTypeCode.value(for: memberTypeName)
            guard membersByTypeCode.updateValue(member, forKey: typeCode) == nil
            else {
                throw DatabaseIndexRebuildError.polymorphicTypeCodeCollision(
                    group: groupIdentifier,
                    typeCode: typeCode
                )
            }
            if finalization == nil {
                finalization = Finalization(
                    runtime: runtime,
                    index: ResolvedIndex(
                        descriptor: descriptor,
                        rootExpression: KeyExpressionFactory.from(
                            keyPaths: descriptor.fieldNames
                        ),
                        itemTypes: Set([groupIdentifier])
                    )
                )
            }
        }
        guard let finalization else {
            throw DatabaseIndexRebuildError.polymorphicGroupHasNoMembers(
                groupIdentifier
            )
        }
        let target = Target(
            group: group,
            declaration: declaration,
            storageIdentity: try DatabaseIndexStorageIdentity.resolve(
                named: indexName,
                in: container.schema,
                physicalLayout: try physicalLayout(for: indexName)
            ),
            subspace: subspace,
            membersByTypeCode: membersByTypeCode,
            finalization: finalization
        )
        if let expectedIdentity,
           target.storageIdentity != expectedIdentity {
            throw DatabaseIndexRebuildError.indexGenerationMismatch(
                indexName
            )
        }
        return target
    }

    private func loadRebuildState(
        key: ByteString,
        scope: String,
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
        guard state.entity == scope, state.index == index else {
            throw DatabaseIndexRebuildError.corruptedRebuildState
        }
        return state
    }

    private func rebuildStateKey(_ target: Target) -> ByteString {
        target.subspace
            .subspace(SubspaceKey.metadata)
            .subspace("index-rebuild")
            .pack(
                Tuple(
                    target.storageIdentity.name,
                    target.storageIdentity.definitionFingerprint.bytes,
                    target.storageIdentity.layoutFingerprint
                )
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
        let group: PolymorphicGroup
        let declaration: IndexDeclaration<String>
        let storageIdentity: DatabaseIndexStorageIdentity
        let subspace: Subspace
        let membersByTypeCode: [Int64: Member]
        let finalization: Finalization
    }

    private struct Member {
        let runtime: EntityRuntimeRegistration
        let descriptor: IndexDescriptor
    }

    private struct Finalization {
        let runtime: EntityRuntimeRegistration
        let index: ResolvedIndex
    }

    private struct SliceResult {
        let processed: UInt64
        let lastProcessedKey: ByteString?
        let hasMore: Bool
    }
}
