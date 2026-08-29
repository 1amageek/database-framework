import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Enforces relationship delete rules through the canonical inverse-reference catalog.
public final class RelationshipMaintainer: Sendable {
    private let schema: Schema
    private let maximumMutations: Int
    private let maximumWorkUnits: UInt64

    public init(
        schema: Schema,
        maximumMutations: Int = 1_000,
        maximumWorkUnits: UInt64 = 1_000_000
    ) {
        self.schema = schema
        self.maximumMutations = maximumMutations
        self.maximumWorkUnits = maximumWorkUnits
    }

    public func enforceDeleteRules(
        for item: PersistedModel,
        identity: EntityReference,
        context: borrowing PersistableMutationContext
    ) async throws {
        var state = EnforcementState(maximumWorkUnits: maximumWorkUnits)
        try await enforceDeleteRules(
            for: item,
            identity: identity,
            context: context,
            state: &state
        )
    }

    private func enforceDeleteRules(
        for item: PersistedModel,
        identity: EntityReference,
        context: borrowing PersistableMutationContext,
        state: inout EnforcementState
    ) async throws {
        let target = identity
        guard !state.visited.contains(target),
              try await !RelationshipDeleteMarker.isMarked(
                target,
                dataRoot: context.dataRoot,
                transaction: context.storageAccess
              ) else {
            return
        }
        state.visited.insert(target)
        try RelationshipDeleteMarker.mark(
            target,
            dataRoot: context.dataRoot,
            transaction: context.storageAccess
        )

        for entity in schema.entities {
            for descriptor in entity.relationships
                where descriptor.relatedTypeName == target.entity {
                guard descriptor.deleteRule != .noAction else { continue }

                let owners = try await referrers(
                    of: target,
                    descriptor: descriptor,
                    context: context,
                    state: &state
                )
                var active: [(EntityReference, PersistedModel)] = []
                let resolver = RelationshipReferenceResolver(schema: schema)
                for identity in owners {
                    if try await context.isDeletionScheduled(for: identity) {
                        continue
                    }
                    if try await RelationshipDeleteMarker.isMarked(
                        identity,
                        dataRoot: context.dataRoot,
                        transaction: context.storageAccess
                    ) {
                        continue
                    }
                    try state.consumeWork()
                    guard let owner = try await context.fetch(identity) else {
                        throw RelationshipError.catalogOwnerMissing(identity)
                    }
                    guard try resolver.references(
                        from: owner,
                        descriptor: descriptor
                    ).contains(target) else {
                        continue
                    }
                    active.append((identity, owner))
                }

                if descriptor.deleteRule == .deny, !active.isEmpty {
                    throw RelationshipError.deleteRuleDenied(
                        itemType: target.entity,
                        relationshipType: entity.name,
                        propertyName: descriptor.propertyName,
                        count: active.count
                    )
                }

                for (identity, owner) in active {
                    try state.markMutation(identity, maximum: maximumMutations)
                    switch descriptor.deleteRule {
                    case .cascade:
                        try await enforceDeleteRules(
                            for: owner,
                            identity: identity,
                            context: context,
                            state: &state
                        )
                        try RelationshipDeleteMarker.mark(
                            identity,
                            dataRoot: context.dataRoot,
                            transaction: context.storageAccess
                        )
                        try await context.delete(
                            owner,
                            precondition: .exists
                        )
                        try RelationshipDeleteMarker.clear(
                            identity,
                            dataRoot: context.dataRoot,
                            transaction: context.storageAccess
                        )
                    case .nullify:
                        let updated = try RelationshipEntityEditor(
                            schema: schema
                        ).removingReference(
                            to: target,
                            from: owner,
                            descriptor: descriptor
                        )
                        try await context.save(
                            updated,
                            precondition: .exists
                        )
                    case .deny, .noAction:
                        break
                    }
                }
            }
        }

        try RelationshipDeleteMarker.clear(
            target,
            dataRoot: context.dataRoot,
            transaction: context.storageAccess
        )
    }

    private func referrers(
        of target: EntityReference,
        descriptor: RelationshipDescriptor,
        context: borrowing PersistableMutationContext,
        state: inout EnforcementState
    ) async throws -> [EntityReference] {
        let remaining = state.remainingWork
        guard remaining > 0 else {
            throw RelationshipError.workLimitExceeded(
                maximum: state.maximumWorkUnits
            )
        }
        let requested = min(remaining + 1, UInt64(Int.max))
        let identities = try await RelationshipReferenceCatalog.referrers(
            of: target,
            descriptor: descriptor,
            limit: Int(requested),
            dataRoot: context.dataRoot,
            transaction: context.storageAccess
        )
        guard UInt64(identities.count) <= remaining else {
            throw RelationshipError.workLimitExceeded(
                maximum: state.maximumWorkUnits
            )
        }
        try state.consumeWork(UInt64(identities.count))
        return identities
    }

    private struct EnforcementState: Sendable {
        let maximumWorkUnits: UInt64
        var consumedWorkUnits: UInt64 = 0
        var visited = Set<EntityReference>()
        var mutations = Set<EntityReference>()

        var remainingWork: UInt64 {
            maximumWorkUnits - consumedWorkUnits
        }

        mutating func consumeWork(_ amount: UInt64 = 1) throws {
            guard amount <= remainingWork else {
                throw RelationshipError.workLimitExceeded(
                    maximum: maximumWorkUnits
                )
            }
            consumedWorkUnits += amount
        }

        mutating func markMutation(
            _ identity: EntityReference,
            maximum: Int
        ) throws {
            mutations.insert(identity)
            guard mutations.count <= maximum else {
                throw RelationshipError.mutationLimitExceeded(
                    actual: mutations.count,
                    maximum: maximum
                )
            }
        }
    }
}
