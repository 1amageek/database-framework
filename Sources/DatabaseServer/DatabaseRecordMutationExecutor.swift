import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import RelationshipIndex
import StorageKit

struct DatabaseRecordMutationExecutor: Sendable {
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

        var seen = Set<DatabaseResolvedRecordIdentity.Key>()
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
                guard let type = entity.persistableType else {
                    throw DatabaseMutationError.entityHasNoPersistableType(change.identity.entity)
                }
                model = try type.decodeDatabaseRecord(change.fields)
            }
            let resolved = try DatabaseResolvedRecordIdentity.resolve(
                change.identity,
                container: container,
                model: model
            )
            let key = DatabaseResolvedRecordIdentity.Key(
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
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler
    ) async throws -> [MutationExecuteOperation.RecordEffect] {
        try await execute(
            prepare(
                changes,
                preconditions: preconditions,
                workMeter: workMeter
            ),
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction,
            persistence: persistence
        )
    }

    func execute(
        _ changes: [PreparedChange],
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler
    ) async throws -> [MutationExecuteOperation.RecordEffect] {
        var states: [DatabaseResolvedRecordIdentity.Key: DatabaseRecordState] = [:]

        for prepared in changes {
            states[prepared.key] = try await load(
                prepared.resolved,
                persistence: persistence,
                transaction: transaction,
                workMeter: workMeter
            )
        }
        for precondition in preconditions {
            let identity = precondition.identity
            let key = try DatabaseResolvedRecordIdentity.key(identity, container: container)
            if states[key] == nil {
                let resolved = try DatabaseResolvedRecordIdentity.resolve(
                    identity,
                    container: container
                )
                states[key] = try await load(
                    resolved,
                    persistence: persistence,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
        }

        for precondition in preconditions {
            let key = try DatabaseResolvedRecordIdentity.key(
                precondition.identity,
                container: container
            )
            guard let state = states[key] else {
                throw DatabaseMutationError.recordNotFound(precondition.identity)
            }
            try validate(precondition, state: state)
        }
        for prepared in changes {
            guard let state = states[prepared.key] else {
                throw DatabaseMutationError.recordNotFound(prepared.change.identity)
            }
            switch (prepared.change.kind, state) {
            case (.insert, .present):
                throw DatabaseMutationError.recordAlreadyExists(prepared.change.identity)
            case (.update, .missing), (.delete, .missing):
                throw DatabaseMutationError.recordNotFound(prepared.change.identity)
            default:
                break
            }
        }

        let planned = try await DatabaseRelationshipMutationPlanner(
            container: container,
            maximumMutations: runtimeLimits.maximumMutations
        ).plan(
            changes,
            states: states,
            workMeter: workMeter,
            transaction: transaction,
            persistence: persistence
        )

        var markedDeletes: [RecordIdentity] = []
        var seenDeletes = Set<RecordIdentity>()
        for mutation in planned where mutation.newModel == nil {
            guard seenDeletes.insert(mutation.identity).inserted else { continue }
            try RelationshipDeleteMarker.mark(
                mutation.identity,
                transaction: transaction
            )
            markedDeletes.append(mutation.identity)
        }

        do {
            var effects: [MutationExecuteOperation.RecordEffect] = []
            effects.reserveCapacity(planned.count)
            for mutation in planned {
                try workMeter.consume(at: .mutationPlanning)
                if let model = mutation.newModel {
                    try await persistence.save(
                        model,
                        precondition: writePrecondition(for: mutation),
                        transaction: transaction
                    )
                    effects.append(
                        MutationExecuteOperation.RecordEffect(
                            kind: mutation.effectKind,
                            identity: mutation.identity,
                            version: try recordVersion(model)
                        )
                    )
                } else {
                    guard let oldModel = mutation.oldModel else {
                        continue
                    }
                    try await persistence.delete(
                        oldModel,
                        precondition: .exists,
                        transaction: transaction
                    )
                    effects.append(
                        MutationExecuteOperation.RecordEffect(
                            kind: .delete,
                            identity: mutation.identity,
                            version: nil
                        )
                    )
                }
            }
            try await persistence.validateFinalState(
                of: planned.compactMap(\.newModel),
                transaction: transaction
            )
            try workMeter.consume(
                UInt64(planned.count),
                at: .validation
            )
            for identity in markedDeletes.reversed() {
                try RelationshipDeleteMarker.clear(
                    identity,
                    transaction: transaction
                )
            }
            return effects
        } catch {
            throw error
        }
    }

    private func writePrecondition(
        for mutation: DatabaseRelationshipMutationPlanner.PlannedMutation
    ) -> WritePrecondition {
        switch mutation.explicitKind {
        case .insert:
            return .notExists
        case .update:
            return .exists
        case .upsert:
            return .none
        case .delete:
            return .exists
        case nil:
            return mutation.oldModel == nil ? .notExists : .exists
        }
    }

    func validate(
        _ preconditions: [MutationExecuteOperation.Precondition],
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
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
            let resolved = try DatabaseResolvedRecordIdentity.resolve(
                precondition.identity,
                container: container
            )
            let state = try await load(
                resolved,
                persistence: persistence,
                transaction: transaction,
                workMeter: workMeter
            )
            try validate(precondition, state: state)
        }
    }

    private func validatePreconditionSet(
        _ preconditions: [MutationExecuteOperation.Precondition]
    ) throws {
        struct Flags {
            var requiresExistence = false
            var requiresAbsence = false
            var version: DatabaseBytes?
            var values = Set<MutationExecuteOperation.Precondition>()
        }
        var flagsByKey: [DatabaseResolvedRecordIdentity.Key: Flags] = [:]
        for precondition in preconditions {
            let identity = precondition.identity
            let key = try DatabaseResolvedRecordIdentity.key(identity, container: container)
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
        _ resolved: DatabaseResolvedRecordIdentity,
        persistence: any ModelPersistenceHandler,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRecordState {
        try workMeter.consume(at: .storageRow)
        if let model = try await persistence.load(
            resolved.identity.entity,
            id: resolved.id,
            partition: resolved.partition,
            transaction: transaction
        ) {
            return .present(model)
        }
        return .missing
    }

    private func validate(
        _ precondition: MutationExecuteOperation.Precondition,
        state: DatabaseRecordState
    ) throws {
        switch (precondition, state) {
        case (.mustExist(let identity), .missing):
            throw DatabaseMutationError.recordNotFound(identity)
        case (.mustNotExist(let identity), .present):
            throw DatabaseMutationError.recordAlreadyExists(identity)
        case (.expectedVersion(let identity, _), .missing):
            throw DatabaseMutationError.recordNotFound(identity)
        case (.expectedVersion(let identity, let expected), .present(let model)):
            guard try recordVersion(model) == expected else {
                throw DatabaseMutationError.recordVersionMismatch(identity)
            }
        default:
            break
        }
    }

    private func recordVersion(
        _ model: any Persistable
    ) throws -> DatabaseBytes {
        let fields = try DatabaseRecordProjection.fields(for: model)
        return try RecordVersionTokenCodec.digest(fields: fields)
    }

    struct PreparedChange: Sendable {
        let change: MutationExecuteOperation.Change
        let resolved: DatabaseResolvedRecordIdentity
        let model: (any Persistable)?

        var key: DatabaseResolvedRecordIdentity.Key {
            DatabaseResolvedRecordIdentity.Key(
                entity: change.identity.entity,
                id: resolved.id.pack(),
                partitionPath: resolved.partitionPath
            )
        }

    }
}

private extension MutationExecuteOperation.Precondition {
    var identity: RecordIdentity {
        switch self {
        case .expectedVersion(let identity, _):
            return identity
        case .mustExist(let identity), .mustNotExist(let identity):
            return identity
        }
    }
}
