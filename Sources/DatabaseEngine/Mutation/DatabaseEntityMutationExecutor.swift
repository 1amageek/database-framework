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
    ) throws -> DatabasePreparedEntityMutation {
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

        var preparedChanges = try DatabaseRetainedArrayBuilder<PreparedChange>(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(PreparedChange.self),
            expectedCount: changes.count
        )
        var identities = try DatabaseRetainedArrayBuilder<
            DatabasePreparedEntityMutation.Identity
        >(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(DatabasePreparedEntityMutation.Identity.self
            ),
            expectedCount: changes.count
        )

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
            try identities.append(
                footprint: try DatabaseEntityMutationFootprintMeter.footprint(
                    of: change.identity,
                    workMeter: workMeter
                ),
                make: {
                    DatabasePreparedEntityMutation.Identity(
                        key: key,
                        identity: change.identity
                    )
                }
            )
            try preparedChanges.append(
                footprint: try DatabaseEntityMutationFootprintMeter.footprint(
                    identity: change.identity,
                    model: model,
                    workMeter: workMeter
                ),
                make: {
                    PreparedChange(
                        kind: change.kind,
                        identity: change.identity,
                        resolved: resolved,
                        model: model
                    )
                }
            )
        }

        let sortedIdentities = try identities.finish().sortingElements {
            lhs,
            rhs in
            try workMeter.consume(at: .sortComparison)
            return lhs.key < rhs.key
        }
        try sortedIdentities.withSpan { identities in
            for index in identities.indices.dropFirst() {
                let previous = identities[index - 1]
                let current = identities[index]
                guard previous.key != current.key else {
                    throw DatabaseEntityMutationError.duplicateChange(
                        current.identity
                    )
                }
            }
        }

        return DatabasePreparedEntityMutation(
            changes: try preparedChanges.finish().moveToSharedOwnership(
                at: .validation
            ),
            identities: try sortedIdentities.moveToSharedOwnership(
                at: .validation
            ),
            preconditions: try preparePreconditions(
                preconditions,
                workMeter: workMeter
            ),
            workMeter: workMeter
        )
    }

    public func execute(
        _ changes: [EntityMutationChange],
        preconditions: [EntityMutationPrecondition] = [],
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseTransaction
    ) async throws -> [EntityMutationEffect] {
        let prepared = try prepare(
            changes,
            preconditions: preconditions,
            workMeter: workMeter
        )
        return try await execute(
            prepared,
            workMeter: workMeter,
            transaction: transaction
        )
    }

    public func execute(
        _ preparedMutation: DatabasePreparedEntityMutation,
        workMeter: DatabaseWorkMeter,
        transaction: DatabaseTransaction
    ) async throws -> [EntityMutationEffect] {
        guard preparedMutation.workMeter === workMeter else {
            throw DatabaseEntityMutationError.workMeterMismatch
        }
        return try await transaction.withEntityMutationOperation(
            workMeter: workMeter
        ) { operationID in
            var mutations = try DatabaseRetainedArrayBuilder<
                PersistableMutation
            >(
                workMeter: workMeter,
                stage: .mutationPlanning,
                layout: try DatabaseRetainedArrayLayout.forElement(PersistableMutation.self),
                expectedCount: preparedMutation.changes.count
            )

            for prepared in preparedMutation.changes {
                try workMeter.consume(at: .mutationPlanning)
                let state = try await load(
                    prepared.resolved,
                    transaction: transaction,
                    workMeter: workMeter,
                    operationID: operationID
                )
                try validatePreconditions(
                    for: prepared.key,
                    state: state,
                    in: preparedMutation.preconditions
                )
                try validate(
                    prepared.kind,
                    identity: prepared.identity,
                    state: state
                )

                let mutation: PersistableMutation
                switch prepared.kind {
                case .insert, .update, .upsert:
                    guard let model = prepared.model else {
                        throw DatabaseEntityMutationError.fieldsRequired(
                            prepared.identity
                        )
                    }
                    mutation = .save(
                        identity: prepared.identity,
                        model: model,
                        precondition: writePrecondition(
                            for: prepared.kind,
                            key: prepared.key,
                            preconditions: preparedMutation.preconditions
                        )
                    )
                case .delete:
                    guard case .present(let model) = state else {
                        throw DatabaseEntityMutationError.entityNotFound(
                            prepared.identity
                        )
                    }
                    mutation = .delete(
                        identity: prepared.identity,
                        model: model,
                        precondition: writePrecondition(
                            for: prepared.kind,
                            key: prepared.key,
                            preconditions: preparedMutation.preconditions
                        )
                    )
                }
                try mutations.append(
                    footprint: try DatabaseEntityMutationFootprintMeter
                        .footprint(
                            of: mutation,
                            workMeter: workMeter
                        ),
                    make: { mutation }
                )
            }

            try await validateUnchangedPreconditions(
                preparedMutation,
                transaction: transaction,
                workMeter: workMeter,
                operationID: operationID
            )

            let retainedMutations = try mutations.finish()
                .moveToSharedOwnership(at: .mutationPlanning)
            try await retainedMutations.withElements { mutations in
                try await transaction.apply(
                    mutations,
                    within: operationID
                )
            }

            let persistedEffectCount = try await transaction
                .persistedMutationEffectCount(within: operationID)
            guard persistedEffectCount <= limits.maximumChanges else {
                throw DatabaseEntityMutationError.changeLimitExceeded(
                    actual: persistedEffectCount,
                    maximum: limits.maximumChanges
                )
            }
            guard let outputCount = UInt32(exactly: persistedEffectCount) else {
                throw DatabaseWorkLimitError.maximumRows(
                    stage: .resultMaterialization,
                    consumed: workMeter.consumedRows,
                    requested: UInt32.max,
                    maximum: workMeter.budget.maximumRows
                )
            }
            try workMeter.consume(
                UInt64(outputCount),
                at: .validation
            )
            try workMeter.recordOutputRows(
                outputCount,
                at: .resultMaterialization
            )
            return try await transaction.mapPersistedMutationEffects(
                within: operationID
            ) { effect in
                EntityMutationEffect(
                    kind: mutationKind(effect.kind),
                    identity: effect.identity,
                    version: try effect.model.map(entityVersion)
                )
            }
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
        let prepared = try preparePreconditions(
            preconditions,
            workMeter: workMeter
        )
        try await transaction.withEntityValidationOperation { operationID in
            var index = prepared.startIndex
            while index < prepared.endIndex {
                let range = preconditionRange(
                    for: prepared[index].key,
                    in: prepared
                )
                let identity = prepared[index].value.identity
                let resolved = try ResolvedEntityReference.resolve(
                    identity,
                    container: container
                )
                let state = try await load(
                    resolved,
                    transaction: transaction,
                    workMeter: workMeter,
                    operationID: operationID
                )
                for preconditionIndex in range {
                    try validate(
                        prepared[preconditionIndex].value,
                        state: state
                    )
                }
                index = range.upperBound
            }
        }
    }

    private func preparePreconditions(
        _ preconditions: [EntityMutationPrecondition],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseSharedRetainedArray<
        DatabasePreparedEntityMutation.Precondition
    > {
        var prepared = try DatabaseRetainedArrayBuilder<
            DatabasePreparedEntityMutation.Precondition
        >(
            workMeter: workMeter,
            stage: .validation,
            layout: try DatabaseRetainedArrayLayout.forElement(DatabasePreparedEntityMutation.Precondition.self
            ),
            expectedCount: preconditions.count
        )
        for precondition in preconditions {
            try workMeter.consume(at: .validation)
            let key = try ResolvedEntityReference.key(
                precondition.identity,
                container: container
            )
            try prepared.append(
                footprint: try DatabaseEntityMutationFootprintMeter.footprint(
                    of: precondition,
                    workMeter: workMeter
                ),
                make: {
                    DatabasePreparedEntityMutation.Precondition(
                        key: key,
                        value: precondition
                    )
                }
            )
        }

        let sorted = try prepared.finish().sortingElements { lhs, rhs in
            try workMeter.consume(at: .sortComparison)
            return lhs.key < rhs.key
        }
        try sorted.withSpan(validatePreconditionSet)
        return try sorted.moveToSharedOwnership(at: .validation)
    }

    private func validatePreconditionSet(
        _ preconditions: Span<DatabasePreparedEntityMutation.Precondition>
    ) throws {
        var index = 0
        while index < preconditions.count {
            let key = preconditions[index].key
            let identity = preconditions[index].value.identity
            var requiresExistence = false
            var requiresAbsence = false
            var expectedVersion: ByteString?
            var sawMustExist = false
            var sawMustNotExist = false

            while index < preconditions.count,
                  preconditions[index].key == key {
                switch preconditions[index].value {
                case .expectedVersion(_, let version):
                    if let expectedVersion {
                        if expectedVersion == version {
                            throw DatabaseEntityMutationError
                                .duplicatePrecondition(identity)
                        }
                        throw DatabaseEntityMutationError
                            .incompatiblePreconditions(identity)
                    }
                    expectedVersion = version
                    requiresExistence = true
                case .mustExist:
                    guard !sawMustExist else {
                        throw DatabaseEntityMutationError
                            .duplicatePrecondition(identity)
                    }
                    sawMustExist = true
                    requiresExistence = true
                case .mustNotExist:
                    guard !sawMustNotExist else {
                        throw DatabaseEntityMutationError
                            .duplicatePrecondition(identity)
                    }
                    sawMustNotExist = true
                    requiresAbsence = true
                }
                guard !(requiresExistence && requiresAbsence) else {
                    throw DatabaseEntityMutationError
                        .incompatiblePreconditions(identity)
                }
                index += 1
            }
        }
    }

    private func validateUnchangedPreconditions(
        _ preparedMutation: DatabasePreparedEntityMutation,
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter,
        operationID: UInt64
    ) async throws {
        let preconditions = preparedMutation.preconditions
        var index = preconditions.startIndex
        while index < preconditions.endIndex {
            let key = preconditions[index].key
            let range = preconditionRange(for: key, in: preconditions)
            if !containsIdentity(key, in: preparedMutation.identities) {
                let identity = preconditions[index].value.identity
                let resolved = try ResolvedEntityReference.resolve(
                    identity,
                    container: container
                )
                let state = try await load(
                    resolved,
                    transaction: transaction,
                    workMeter: workMeter,
                    operationID: operationID
                )
                for preconditionIndex in range {
                    try validate(
                        preconditions[preconditionIndex].value,
                        state: state
                    )
                }
            }
            index = range.upperBound
        }
    }

    private func validatePreconditions(
        for key: ResolvedEntityReference.Key,
        state: DatabaseEntityState,
        in preconditions: DatabaseSharedRetainedArray<
            DatabasePreparedEntityMutation.Precondition
        >
    ) throws {
        for index in preconditionRange(for: key, in: preconditions) {
            try validate(preconditions[index].value, state: state)
        }
    }

    private func preconditionRange(
        for key: ResolvedEntityReference.Key,
        in preconditions: DatabaseSharedRetainedArray<
            DatabasePreparedEntityMutation.Precondition
        >
    ) -> Range<Int> {
        var lower = preconditions.startIndex
        var upper = preconditions.endIndex
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            if preconditions[middle].key < key {
                lower = middle + 1
            } else {
                upper = middle
            }
        }
        let start = lower
        upper = preconditions.endIndex
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            if key < preconditions[middle].key {
                upper = middle
            } else {
                lower = middle + 1
            }
        }
        return start..<lower
    }

    private func containsIdentity(
        _ key: ResolvedEntityReference.Key,
        in identities: DatabaseSharedRetainedArray<
            DatabasePreparedEntityMutation.Identity
        >
    ) -> Bool {
        var lower = identities.startIndex
        var upper = identities.endIndex
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            let candidate = identities[middle].key
            if candidate < key {
                lower = middle + 1
            } else {
                upper = middle
            }
        }
        return lower < identities.endIndex && identities[lower].key == key
    }

    private func writePrecondition(
        for kind: EntityMutationKind,
        key: ResolvedEntityReference.Key,
        preconditions: DatabaseSharedRetainedArray<
            DatabasePreparedEntityMutation.Precondition
        >
    ) -> WritePrecondition {
        var mustExist = false
        var mustNotExist = false
        for index in preconditionRange(for: key, in: preconditions) {
            switch preconditions[index].value {
            case .expectedVersion(_, let version):
                return .matchesStored(version: version)
            case .mustExist:
                mustExist = true
            case .mustNotExist:
                mustNotExist = true
            }
        }
        if mustNotExist { return .notExists }
        if mustExist { return .exists }
        switch kind {
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

    private func load(
        _ resolved: ResolvedEntityReference,
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter,
        operationID: UInt64
    ) async throws -> DatabaseEntityState {
        try workMeter.consume(at: .storageRow)
        if let model = try await transaction.loadPersistedModel(
            entity: resolved.identity.entity,
            id: resolved.id,
            partition: resolved.partition,
            within: operationID
        ) {
            return .present(model)
        }
        return .missing
    }

    private func validate(
        _ kind: EntityMutationKind,
        identity: EntityReference,
        state: DatabaseEntityState
    ) throws {
        switch (kind, state) {
        case (.insert, .present):
            throw DatabaseEntityMutationError.entityAlreadyExists(
                identity
            )
        case (.update, .missing), (.delete, .missing):
            throw DatabaseEntityMutationError.entityNotFound(identity)
        default:
            break
        }
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

    package struct PreparedChange: Sendable {
        let kind: EntityMutationKind
        let identity: EntityReference
        let resolved: ResolvedEntityReference
        let model: PersistedModel?

        var key: ResolvedEntityReference.Key {
            ResolvedEntityReference.Key(
                entity: identity.entity,
                id: resolved.id.pack(),
                partitionPath: resolved.partitionPath
            )
        }
    }
}
