import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Relationship
import RelationshipIndex
import StorageKit

/// Expands an atomic record mutation into its final relationship-consistent write set.
struct DatabaseRelationshipMutationPlanner: Sendable {
    private let container: DBContainer
    private let maximumMutations: Int

    init(container: DBContainer, maximumMutations: Int) {
        self.container = container
        self.maximumMutations = maximumMutations
    }

    func plan(
        _ changes: [DatabaseRecordMutationExecutor.PreparedChange],
        states: [DatabaseResolvedRecordIdentity.Key: DatabaseRecordState],
        workMeter: DatabaseWorkMeter,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler
    ) async throws -> [PlannedMutation] {
        var planning = PlanningState(workMeter: workMeter)

        for prepared in changes {
            guard let state = states[prepared.key] else {
                throw DatabaseMutationError.recordNotFound(prepared.change.identity)
            }
            let canonicalIdentity: RecordIdentity
            if let model = prepared.model ?? state.model {
                canonicalIdentity = try DatabaseRecordProjection.identity(for: model)
            } else {
                canonicalIdentity = prepared.change.identity
            }
            planning.entries[prepared.key] = Entry(
                identity: canonicalIdentity,
                oldModel: state.model,
                newModel: prepared.model,
                explicitKind: prepared.change.kind
            )
            planning.order.append(prepared.key)
            planning.mutatedKeys.insert(prepared.key)
        }

        for prepared in changes where prepared.change.kind == .delete {
            guard let model = states[prepared.key]?.model else {
                throw DatabaseMutationError.recordNotFound(prepared.change.identity)
            }
            try await planDeletion(
                of: model,
                planning: &planning,
                transaction: transaction,
                persistence: persistence
            )
        }

        try await validateFinalReferences(
            planning: &planning,
            transaction: transaction,
            persistence: persistence
        )

        let saveOrder = planning.order.filter {
            planning.entries[$0]?.newModel != nil
        }
        let mutationOrder = saveOrder + planning.deleteOrder
        return mutationOrder.compactMap { key in
            guard planning.mutatedKeys.contains(key),
                  let entry = planning.entries[key] else {
                return nil
            }
            return PlannedMutation(
                identity: entry.identity,
                oldModel: entry.oldModel,
                newModel: entry.newModel,
                explicitKind: entry.explicitKind
            )
        }
    }

    private func planDeletion(
        of targetModel: any Persistable,
        planning: inout PlanningState,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler
    ) async throws {
        let target = try DatabaseRecordProjection.identity(for: targetModel)
        guard planning.visitedDeletes.insert(target).inserted else {
            return
        }

        for entity in container.schema.entities {
            guard let ownerType = entity.persistableType else { continue }
            for descriptor in ownerType.relationshipDescriptors
                where descriptor.relatedTypeName == target.entity {
                guard descriptor.deleteRule != .noAction else { continue }

                let catalogOwners = try await catalogReferrers(
                    target: target,
                    descriptor: descriptor,
                    planning: &planning,
                    transaction: transaction
                )
                var ownerKeys = Set<DatabaseResolvedRecordIdentity.Key>()
                for identity in catalogOwners {
                    let key = try DatabaseResolvedRecordIdentity.key(
                        identity,
                        container: container
                    )
                    ownerKeys.insert(key)
                    if planning.entries[key] == nil {
                        try planning.consumeWork()
                        let resolved = try DatabaseResolvedRecordIdentity.resolve(
                            identity,
                            container: container
                        )
                        guard let model = try await persistence.load(
                            identity.entity,
                            id: resolved.id,
                            partition: resolved.partition,
                            transaction: transaction
                        ) else {
                            throw DatabaseMutationError.relationshipCatalogCorrupted(
                                identity
                            )
                        }
                        planning.entries[key] = Entry(
                            identity: identity,
                            oldModel: model,
                            newModel: model,
                            explicitKind: nil
                        )
                        planning.order.append(key)
                    }
                }

                let overlay = planning.entries.map { ($0.key, $0.value.newModel) }
                let resolver = RelationshipReferenceResolver(schema: container.schema)
                for (key, proposedModel) in overlay {
                    guard let proposedModel,
                          type(of: proposedModel).persistableType == ownerType.persistableType else {
                        continue
                    }
                    try planning.consumeWork()
                    if try resolver.references(
                        from: proposedModel,
                        descriptor: descriptor
                    ).contains(target) {
                        ownerKeys.insert(key)
                    }
                }

                var activeOwners: [(DatabaseResolvedRecordIdentity.Key, any Persistable)] = []
                for key in ownerKeys {
                    guard let owner = planning.entries[key]?.newModel else { continue }
                    try planning.consumeWork()
                    guard try resolver.references(
                        from: owner,
                        descriptor: descriptor
                    ).contains(target) else {
                        continue
                    }
                    activeOwners.append((key, owner))
                }

                if descriptor.deleteRule == .deny, !activeOwners.isEmpty {
                    throw RelationshipError.deleteRuleDenied(
                        itemType: target.entity,
                        relationshipType: ownerType.persistableType,
                        propertyName: descriptor.propertyName,
                        count: activeOwners.count
                    )
                }

                for (key, owner) in activeOwners {
                    switch descriptor.deleteRule {
                    case .cascade:
                        guard let entry = planning.entries[key] else { continue }
                        if let explicitKind = entry.explicitKind,
                           explicitKind != .delete {
                            throw DatabaseMutationError.relationshipMutationConflict(
                                entry.identity
                            )
                        }
                        planning.entries[key]?.newModel = nil
                        try planning.markMutated(key, maximum: maximumMutations)
                        try await planDeletion(
                            of: owner,
                            planning: &planning,
                            transaction: transaction,
                            persistence: persistence
                        )
                    case .nullify:
                        let editor = RelationshipRecordEditor(schema: container.schema)
                        planning.entries[key]?.newModel = try editor.removingReference(
                            to: target,
                            from: owner,
                            descriptor: descriptor
                        )
                        try planning.markMutated(key, maximum: maximumMutations)
                    case .deny, .noAction:
                        break
                    }
                }
            }
        }
        let targetKey = try DatabaseResolvedRecordIdentity.key(
            target,
            container: container
        )
        if planning.entries[targetKey]?.newModel == nil,
           planning.orderedDeletes.insert(targetKey).inserted {
            planning.deleteOrder.append(targetKey)
        }
    }

    private func catalogReferrers(
        target: RecordIdentity,
        descriptor: RelationshipDescriptor,
        planning: inout PlanningState,
        transaction: any Transaction
    ) async throws -> [RecordIdentity] {
        let requested = try planning.workMeter.storageReadLimitWithSentinel(
            at: .mutationPlanning
        )
        let identities = try await RelationshipReferenceCatalog.referrers(
            of: target,
            descriptor: descriptor,
            limit: requested,
            transaction: transaction
        )
        try planning.consumeWork(UInt64(identities.count))
        return identities
    }

    private func validateFinalReferences(
        planning: inout PlanningState,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler
    ) async throws {
        let mutated = planning.mutatedKeys.compactMap { planning.entries[$0] }
        let resolver = RelationshipReferenceResolver(schema: container.schema)

        for entry in mutated {
            guard let model = entry.newModel else { continue }
            for descriptor in type(of: model).relationshipDescriptors {
                let references = try resolver.references(
                    from: model,
                    descriptor: descriptor
                )
                for target in references {
                    let targetKey = try DatabaseResolvedRecordIdentity.key(
                        target,
                        container: container
                    )
                    if let targetEntry = planning.entries[targetKey] {
                        if targetEntry.newModel != nil || descriptor.deleteRule == .noAction {
                            continue
                        }
                        throw DatabaseMutationError.relationshipTargetNotFound(
                            owner: entry.identity,
                            target: target
                        )
                    }

                    try planning.consumeWork()
                    let resolved = try DatabaseResolvedRecordIdentity.resolve(
                        target,
                        container: container
                    )
                    let stored = try await persistence.load(
                        target.entity,
                        id: resolved.id,
                        partition: resolved.partition,
                        transaction: transaction
                    )
                    if stored == nil, descriptor.deleteRule != .noAction {
                        throw DatabaseMutationError.relationshipTargetNotFound(
                            owner: entry.identity,
                            target: target
                        )
                    }
                }
            }
        }
    }

    struct PlannedMutation: Sendable {
        let identity: RecordIdentity
        let oldModel: (any Persistable)?
        let newModel: (any Persistable)?
        let explicitKind: MutationExecuteOperation.Kind?

        var effectKind: MutationExecuteOperation.Kind {
            if newModel == nil { return .delete }
            if let explicitKind { return explicitKind }
            return oldModel == nil ? .insert : .update
        }
    }

    private struct Entry: Sendable {
        let identity: RecordIdentity
        let oldModel: (any Persistable)?
        var newModel: (any Persistable)?
        let explicitKind: MutationExecuteOperation.Kind?
    }

    private struct PlanningState: Sendable {
        let workMeter: DatabaseWorkMeter
        var entries: [DatabaseResolvedRecordIdentity.Key: Entry] = [:]
        var order: [DatabaseResolvedRecordIdentity.Key] = []
        var deleteOrder: [DatabaseResolvedRecordIdentity.Key] = []
        var orderedDeletes = Set<DatabaseResolvedRecordIdentity.Key>()
        var mutatedKeys = Set<DatabaseResolvedRecordIdentity.Key>()
        var visitedDeletes = Set<RecordIdentity>()

        init(workMeter: DatabaseWorkMeter) {
            self.workMeter = workMeter
        }

        func consumeWork(_ amount: UInt64 = 1) throws {
            try workMeter.consume(amount, at: .mutationPlanning)
        }

        mutating func markMutated(
            _ key: DatabaseResolvedRecordIdentity.Key,
            maximum: Int
        ) throws {
            mutatedKeys.insert(key)
            guard mutatedKeys.count <= maximum else {
                throw DatabaseMutationError.mutationLimitExceeded(
                    actual: mutatedKeys.count,
                    maximum: maximum
                )
            }
        }
    }
}
