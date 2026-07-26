import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseEntityMutationExecutor: Sendable {
    private let container: DBContainer
    private let runtimeLimits: DatabaseRuntimeLimits

    init(
        container: DBContainer,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.container = container
        self.runtimeLimits = runtimeLimits
    }

    func prepare(
        _ changes: [MutationExecuteOperation.Change],
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) throws -> [PreparedChange] {
        guard !changes.isEmpty else {
            throw DatabaseMutationError.emptyMutation
        }
        guard changes.count <= runtimeLimits.maximumMutations else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: changes.count,
                maximum: runtimeLimits.maximumMutations
            )
        }
        guard preconditions.count <= runtimeLimits.maximumPreconditions else {
            throw DatabaseMutationError.preconditionLimitExceeded(
                actual: preconditions.count,
                maximum: runtimeLimits.maximumPreconditions
            )
        }

        var seen = Set<ResolvedEntityReference.Key>()
        var result: [PreparedChange] = []
        result.reserveCapacity(changes.count)
        for change in changes {
            try workMeter.consume(at: .validation)
            let model: (any Persistable)?
            switch change.kind {
            case .delete:
                guard change.fields.isEmpty else {
                    throw DatabaseMutationError.fieldsMustBeEmptyForDelete(change.identity)
                }
                model = nil
            case .insert, .update, .upsert:
                guard !change.fields.isEmpty else {
                    throw DatabaseMutationError.fieldsRequired(change.identity)
                }
                guard let entity = container.schema.entities.first(where: {
                    $0.name == change.identity.entity
                }) else {
                    throw DatabaseMutationError.unknownEntity(change.identity.entity)
                }
                guard let type = container.runtimeConfiguration
                    .persistableTypes.type(named: entity.name) else {
                    throw DatabaseMutationError.entityHasNoPersistableType(change.identity.entity)
                }
                model = try type.decodePersistedObject(change.fields)
            }
            let resolved = try ResolvedEntityReference.resolve(
                change.identity,
                container: container,
                model: model
            )
            let key = ResolvedEntityReference.Key(
                entity: change.identity.entity,
                id: resolved.id.pack(),
                partitionPath: resolved.partitionPath
            )
            guard seen.insert(key).inserted else {
                throw DatabaseMutationError.duplicateChange(change.identity)
            }
            result.append(
                PreparedChange(
                    change: change,
                    resolved: resolved,
                    model: model
                )
            )
        }
        try validatePreconditionSet(preconditions)
        return result
    }

    func execute(
        _ changes: [MutationExecuteOperation.Change],
        preconditions: [MutationExecuteOperation.Precondition] = [],
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseTransaction
    ) async throws -> [MutationExecuteOperation.EntityEffect] {
        try await execute(
            prepare(
                changes,
                preconditions: preconditions,
                workMeter: workMeter
            ),
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction
        )
    }

    func execute(
        _ changes: [PreparedChange],
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseTransaction
    ) async throws -> [MutationExecuteOperation.EntityEffect] {
        var states: [ResolvedEntityReference.Key: DatabaseEntityState] = [:]

        for prepared in changes {
            states[prepared.key] = try await load(
                prepared.resolved,
                transaction: transaction,
                workMeter: workMeter
            )
        }
        for precondition in preconditions {
            let identity = precondition.identity
            let key = try ResolvedEntityReference.key(identity, container: container)
            if states[key] == nil {
                let resolved = try ResolvedEntityReference.resolve(
                    identity,
                    container: container
                )
                states[key] = try await load(
                    resolved,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
        }

        for precondition in preconditions {
            let key = try ResolvedEntityReference.key(
                precondition.identity,
                container: container
            )
            guard let state = states[key] else {
                throw DatabaseMutationError.entityNotFound(precondition.identity)
            }
            try validate(precondition, state: state)
        }
        for prepared in changes {
            guard let state = states[prepared.key] else {
                throw DatabaseMutationError.entityNotFound(prepared.change.identity)
            }
            switch (prepared.change.kind, state) {
            case (.insert, .present):
                throw DatabaseMutationError.entityAlreadyExists(prepared.change.identity)
            case (.update, .missing), (.delete, .missing):
                throw DatabaseMutationError.entityNotFound(prepared.change.identity)
            default:
                break
            }
        }

        var mutations: [PersistableMutation] = []
        mutations.reserveCapacity(changes.count)
        for prepared in changes {
            try workMeter.consume(at: .mutationPlanning)
            switch prepared.change.kind {
            case .insert, .update, .upsert:
                guard let model = prepared.model else {
                    throw DatabaseMutationError.fieldsRequired(
                        prepared.change.identity
                    )
                }
                mutations.append(
                    .save(
                        model: model,
                        precondition: writePrecondition(
                            for: prepared.change,
                            preconditions: preconditions
                        )
                    )
                )
            case .delete:
                guard case .present(let model) = states[prepared.key] else {
                    throw DatabaseMutationError.entityNotFound(
                        prepared.change.identity
                    )
                }
                mutations.append(
                    .delete(
                        model: model,
                        precondition: writePrecondition(
                            for: prepared.change,
                            preconditions: preconditions
                        )
                    )
                )
            }
        }
        try await transaction.apply(mutations)

        let persistedEffects = try await transaction
            .persistedMutationEffects()
        guard persistedEffects.count <= runtimeLimits.maximumMutations else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: persistedEffects.count,
                maximum: runtimeLimits.maximumMutations
            )
        }
        try workMeter.consume(
            UInt64(persistedEffects.count),
            at: .validation
        )
        return try persistedEffects.map { effect in
            MutationExecuteOperation.EntityEffect(
                kind: mutationKind(effect.kind),
                identity: effect.identity,
                version: try effect.model.map(entityVersion)
            )
        }
    }

    private func writePrecondition(
        for change: MutationExecuteOperation.Change,
        preconditions: [MutationExecuteOperation.Precondition]
    ) -> WritePrecondition {
        for precondition in preconditions {
            guard case .expectedVersion(let identity, let version) = precondition,
                  identity == change.identity else {
                continue
            }
            return .matchesStored(version: version)
        }
        if preconditions.contains(
            .mustNotExist(change.identity)
        ) {
            return .notExists
        }
        if preconditions.contains(
            .mustExist(change.identity)
        ) {
            return .exists
        }
        switch change.kind {
        case .insert:
            return .notExists
        case .update:
            return .exists
        case .upsert:
            return .none
        case .delete:
            return .exists
        }
    }

    func validate(
        _ preconditions: [MutationExecuteOperation.Precondition],
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter
    ) async throws {
        guard preconditions.count <= runtimeLimits.maximumPreconditions else {
            throw DatabaseMutationError.preconditionLimitExceeded(
                actual: preconditions.count,
                maximum: runtimeLimits.maximumPreconditions
            )
        }
        try validatePreconditionSet(preconditions)

        for precondition in preconditions {
            let resolved = try ResolvedEntityReference.resolve(
                precondition.identity,
                container: container
            )
            let state = try await load(
                resolved,
                transaction: transaction,
                workMeter: workMeter
            )
            try validate(precondition, state: state)
        }
    }

    private func mutationKind(
        _ kind: PersistableMutationKind
    ) -> MutationExecuteOperation.Kind {
        switch kind {
        case .insert:
            return .insert
        case .update:
            return .update
        case .delete:
            return .delete
        }
    }

    private func validatePreconditionSet(
        _ preconditions: [MutationExecuteOperation.Precondition]
    ) throws {
        struct Flags {
            var requiresExistence = false
            var requiresAbsence = false
            var version: ByteString?
            var values = Set<MutationExecuteOperation.Precondition>()
        }
        var flagsByKey: [ResolvedEntityReference.Key: Flags] = [:]
        for precondition in preconditions {
            let identity = precondition.identity
            let key = try ResolvedEntityReference.key(identity, container: container)
            var flags = flagsByKey[key] ?? Flags()
            guard flags.values.insert(precondition).inserted else {
                throw DatabaseMutationError.duplicatePrecondition(identity)
            }
            switch precondition {
            case .expectedVersion(_, let version):
                flags.requiresExistence = true
                if let current = flags.version, current != version {
                    throw DatabaseMutationError.incompatiblePreconditions(identity)
                }
                flags.version = version
            case .mustExist:
                flags.requiresExistence = true
            case .mustNotExist:
                flags.requiresAbsence = true
            }
            guard !(flags.requiresExistence && flags.requiresAbsence) else {
                throw DatabaseMutationError.incompatiblePreconditions(identity)
            }
            flagsByKey[key] = flags
        }
    }

    private func load(
        _ resolved: ResolvedEntityReference,
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseEntityState {
        try workMeter.consume(at: .storageRow)
        if let model = try await transaction.loadPersistedModel(
            entity: resolved.identity.entity,
            id: resolved.id,
            partition: resolved.partition
        ) {
            return .present(model)
        }
        return .missing
    }

    private func validate(
        _ precondition: MutationExecuteOperation.Precondition,
        state: DatabaseEntityState
    ) throws {
        switch (precondition, state) {
        case (.mustExist(let identity), .missing):
            throw DatabaseMutationError.entityNotFound(identity)
        case (.mustNotExist(let identity), .present):
            throw DatabaseMutationError.entityAlreadyExists(identity)
        case (.expectedVersion(let identity, _), .missing):
            throw DatabaseMutationError.entityNotFound(identity)
        case (.expectedVersion(let identity, let expected), .present(let model)):
            guard try entityVersion(model) == expected else {
                throw DatabaseMutationError.entityVersionMismatch(identity)
            }
        default:
            break
        }
    }

    private func entityVersion(
        _ model: any Persistable
    ) throws -> ByteString {
        let fields = try DatabaseEntityProjection.persistedFields(for: model)
        return try PersistableVersionTokenCodec.digest(fields: fields)
    }

    struct PreparedChange: Sendable {
        let change: MutationExecuteOperation.Change
        let resolved: ResolvedEntityReference
        let model: (any Persistable)?

        var key: ResolvedEntityReference.Key {
            ResolvedEntityReference.Key(
                entity: change.identity.entity,
                id: resolved.id.pack(),
                partitionPath: resolved.partitionPath
            )
        }

    }
}

private extension MutationExecuteOperation.Precondition {
    var identity: EntityReference {
        switch self {
        case .expectedVersion(let identity, _):
            return identity
        case .mustExist(let identity), .mustNotExist(let identity):
            return identity
        }
    }
}
