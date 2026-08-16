import DatabaseKit
import DatabaseTypes
import StorageKit

@_spi(DatabaseExecution)
public struct DatabaseEntityMutationExecutor: Sendable {
    private let container: DBContainer
    private let limits: DatabaseEntityMutationLimits

    public init(
        container: DBContainer,
        limits: DatabaseEntityMutationLimits
    ) {
        self.container = container
        self.limits = limits
    }

    public func resolveReference(
        _ identity: EntityReference,
        model: PersistedModel? = nil
    ) throws -> ResolvedEntityReference {
        try ResolvedEntityReference.resolve(
            identity,
            container: container,
            model: model
        )
    }

    public func prepare(
        _ changes: [EntityMutationChange],
        preconditions: [EntityMutationPrecondition],
        workMeter: DatabaseWorkMeter
    ) throws -> [PreparedChange] {
        guard !changes.isEmpty else {
            throw DatabaseEntityMutationError.emptyMutation
        }
        guard changes.count <= limits.maximumChanges else {
            throw DatabaseEntityMutationError.changeLimitExceeded(
                actual: changes.count,
                maximum: limits.maximumChanges
            )
        }
        guard preconditions.count <= limits.maximumPreconditions else {
            throw DatabaseEntityMutationError.preconditionLimitExceeded(
                actual: preconditions.count,
                maximum: limits.maximumPreconditions
            )
        }

        var seen = ResolvedEntityMap<Void>()
        var result: [PreparedChange] = []
        result.reserveCapacity(changes.count)
        for change in changes {
            try workMeter.consume(at: .validation)
            let model: PersistedModel?
            switch change.kind {
            case .delete:
                guard change.fields.isEmpty else {
                    throw DatabaseEntityMutationError
                        .fieldsMustBeEmptyForDelete(change.identity)
                }
                model = nil
            case .insert, .update, .upsert:
                guard !change.fields.isEmpty else {
                    throw DatabaseEntityMutationError.fieldsRequired(
                        change.identity
                    )
                }
                guard let entity = container.schema.entities.first(where: {
                    $0.name == change.identity.entity
                }) else {
                    throw DatabaseEntityMutationError.unknownEntity(
                        change.identity.entity
                    )
                }
                guard let runtime = container.runtimeConfiguration
                    .entityRuntimes.registration(named: entity.name) else {
                    throw DatabaseEntityMutationError.entityHasNoPersistableType(
                        change.identity.entity
                    )
                }
                model = try runtime.persistedModel(from: change.fields)
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
            guard seen.insert((), for: key) else {
                throw DatabaseEntityMutationError.duplicateChange(
                    change.identity
                )
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

    public func execute(
        _ changes: [EntityMutationChange],
        preconditions: [EntityMutationPrecondition] = [],
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseTransaction
    ) async throws -> [EntityMutationEffect] {
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

    public func execute(
        _ changes: [PreparedChange],
        preconditions: [EntityMutationPrecondition],
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseTransaction
    ) async throws -> [EntityMutationEffect] {
        var states = ResolvedEntityMap<DatabaseEntityState>()

        for prepared in changes {
            states.insert(
                try await load(
                    prepared.resolved,
                    transaction: transaction,
                    workMeter: workMeter
                ),
                for: prepared.key
            )
        }
        for precondition in preconditions {
            let identity = precondition.identity
            let key = try ResolvedEntityReference.key(
                identity,
                container: container
            )
            if states.value(for: key) == nil {
                let resolved = try ResolvedEntityReference.resolve(
                    identity,
                    container: container
                )
                states.insert(
                    try await load(
                        resolved,
                        transaction: transaction,
                        workMeter: workMeter
                    ),
                    for: key
                )
            }
        }

        for precondition in preconditions {
            let key = try ResolvedEntityReference.key(
                precondition.identity,
                container: container
            )
            guard let state = states.value(for: key) else {
                throw DatabaseEntityMutationError.entityNotFound(
                    precondition.identity
                )
            }
            try validate(precondition, state: state)
        }
        for prepared in changes {
            guard let state = states.value(for: prepared.key) else {
                throw DatabaseEntityMutationError.entityNotFound(
                    prepared.change.identity
                )
            }
            switch (prepared.change.kind, state) {
            case (.insert, .present):
                throw DatabaseEntityMutationError.entityAlreadyExists(
                    prepared.change.identity
                )
            case (.update, .missing), (.delete, .missing):
                throw DatabaseEntityMutationError.entityNotFound(
                    prepared.change.identity
                )
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
                    throw DatabaseEntityMutationError.fieldsRequired(
                        prepared.change.identity
                    )
                }
                mutations.append(
                    .save(
                        identity: prepared.change.identity,
                        model: model,
                        precondition: writePrecondition(
                            for: prepared.change,
                            preconditions: preconditions
                        )
                    )
                )
            case .delete:
                guard case .present(let model) = states.value(
                    for: prepared.key
                ) else {
                    throw DatabaseEntityMutationError.entityNotFound(
                        prepared.change.identity
                    )
                }
                mutations.append(
                    .delete(
                        identity: prepared.change.identity,
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
        guard persistedEffects.count <= limits.maximumChanges else {
            throw DatabaseEntityMutationError.changeLimitExceeded(
                actual: persistedEffects.count,
                maximum: limits.maximumChanges
            )
        }
        try workMeter.consume(
            UInt64(persistedEffects.count),
            at: .validation
        )
        return try persistedEffects.map { effect in
            EntityMutationEffect(
                kind: mutationKind(effect.kind),
                identity: effect.identity,
                version: try effect.model.map(entityVersion)
            )
        }
    }

    public func validate(
        _ preconditions: [EntityMutationPrecondition],
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter
    ) async throws {
        guard preconditions.count <= limits.maximumPreconditions else {
            throw DatabaseEntityMutationError.preconditionLimitExceeded(
                actual: preconditions.count,
                maximum: limits.maximumPreconditions
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

    private func writePrecondition(
        for change: EntityMutationChange,
        preconditions: [EntityMutationPrecondition]
    ) -> WritePrecondition {
        for precondition in preconditions {
            guard case .expectedVersion(let identity, let version) = precondition,
                  identity == change.identity else {
                continue
            }
            return .matchesStored(version: version)
        }
        if preconditions.contains(.mustNotExist(change.identity)) {
            return .notExists
        }
        if preconditions.contains(.mustExist(change.identity)) {
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

    private func mutationKind(
        _ kind: PersistableMutationKind
    ) -> EntityMutationKind {
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
        _ preconditions: [EntityMutationPrecondition]
    ) throws {
        struct Flags: Sendable {
            var requiresExistence = false
            var requiresAbsence = false
            var version: ByteString?
            var values: [EntityMutationPrecondition] = []
        }
        var flagsByKey = ResolvedEntityMap<Flags>()
        for precondition in preconditions {
            let identity = precondition.identity
            let key = try ResolvedEntityReference.key(
                identity,
                container: container
            )
            var flags = flagsByKey.value(for: key) ?? Flags()
            guard !flags.values.contains(precondition) else {
                throw DatabaseEntityMutationError.duplicatePrecondition(identity)
            }
            flags.values.append(precondition)
            switch precondition {
            case .expectedVersion(_, let version):
                flags.requiresExistence = true
                if let current = flags.version, current != version {
                    throw DatabaseEntityMutationError
                        .incompatiblePreconditions(identity)
                }
                flags.version = version
            case .mustExist:
                flags.requiresExistence = true
            case .mustNotExist:
                flags.requiresAbsence = true
            }
            guard !(flags.requiresExistence && flags.requiresAbsence) else {
                throw DatabaseEntityMutationError
                    .incompatiblePreconditions(identity)
            }
            flagsByKey.insert(flags, for: key)
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
        _ precondition: EntityMutationPrecondition,
        state: DatabaseEntityState
    ) throws {
        switch (precondition, state) {
        case (.mustExist(let identity), .missing):
            throw DatabaseEntityMutationError.entityNotFound(identity)
        case (.mustNotExist(let identity), .present):
            throw DatabaseEntityMutationError.entityAlreadyExists(identity)
        case (.expectedVersion(let identity, _), .missing):
            throw DatabaseEntityMutationError.entityNotFound(identity)
        case (.expectedVersion(let identity, let expected), .present(let model)):
            guard try entityVersion(model) == expected else {
                throw DatabaseEntityMutationError.entityVersionMismatch(identity)
            }
        default:
            break
        }
    }

    private func entityVersion(
        _ model: PersistedModel
    ) throws -> ByteString {
        try PersistableVersionTokenCodec.digest(fields: model.fields)
    }

    public struct PreparedChange: Sendable {
        let change: EntityMutationChange
        let resolved: ResolvedEntityReference
        let model: PersistedModel?

        var key: ResolvedEntityReference.Key {
            ResolvedEntityReference.Key(
                entity: change.identity.entity,
                id: resolved.id.pack(),
                partitionPath: resolved.partitionPath
            )
        }
    }
}
