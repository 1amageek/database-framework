import Core
import DatabaseEngine
import DatabaseValue
import Relationship
import StorageKit

/// Enforces relationship delete rules through the canonical inverse-reference catalog.
public final class RelationshipMaintainer: Sendable {
    private let container: DBContainer
    private let schema: Schema
    private let maximumMutations: Int
    private let maximumWorkUnits: UInt64

    public init(
        container: DBContainer,
        schema: Schema,
        maximumMutations: Int = 1_000,
        maximumWorkUnits: UInt64 = 1_000_000
    ) {
        self.container = container
        self.schema = schema
        self.maximumMutations = maximumMutations
        self.maximumWorkUnits = maximumWorkUnits
    }

    public func enforceDeleteRules(
        for item: any Persistable,
        transaction: any Transaction,
        handler: ModelPersistenceHandler
    ) async throws {
        var state = EnforcementState(maximumWorkUnits: maximumWorkUnits)
        try await enforceDeleteRules(
            for: item,
            transaction: transaction,
            handler: handler,
            state: &state
        )
    }

    private func enforceDeleteRules(
        for item: any Persistable,
        transaction: any Transaction,
        handler: ModelPersistenceHandler,
        state: inout EnforcementState
    ) async throws {
        let target = try DatabaseRecordIdentityEncoder.encode(item)
        guard !state.visited.contains(target),
              try await !RelationshipDeleteMarker.isMarked(
                target,
                transaction: transaction
              ) else {
            return
        }
        state.visited.insert(target)
        try RelationshipDeleteMarker.mark(target, transaction: transaction)

        for entity in schema.entities {
            guard let ownerType = entity.persistableType else { continue }
            for descriptor in ownerType.relationshipDescriptors
                where descriptor.relatedTypeName == target.entity {
                guard descriptor.deleteRule != .noAction else { continue }

                let owners = try await referrers(
                    of: target,
                    descriptor: descriptor,
                    transaction: transaction,
                    state: &state
                )
                var active: [(RecordIdentity, any Persistable)] = []
                let resolver = RelationshipReferenceResolver(schema: schema)
                for identity in owners {
                    if try await RelationshipDeleteMarker.isMarked(
                        identity,
                        transaction: transaction
                    ) {
                        continue
                    }
                    try state.consumeWork()
                    let resolved = try CanonicalRelationshipIdentity.resolve(
                        identity,
                        container: container
                    )
                    guard let owner = try await handler.load(
                        identity.entity,
                        id: resolved.id,
                        partition: resolved.partition,
                        transaction: transaction
                    ) else {
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
                        relationshipType: ownerType.persistableType,
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
                            transaction: transaction,
                            handler: handler,
                            state: &state
                        )
                        try RelationshipDeleteMarker.mark(
                            identity,
                            transaction: transaction
                        )
                        try await handler.delete(
                            owner,
                            precondition: .exists,
                            transaction: transaction
                        )
                        try RelationshipDeleteMarker.clear(
                            identity,
                            transaction: transaction
                        )
                    case .nullify:
                        let updated = try RelationshipRecordEditor(
                            schema: schema
                        ).removingReference(
                            to: target,
                            from: owner,
                            descriptor: descriptor
                        )
                        try await handler.save(
                            updated,
                            precondition: .exists,
                            transaction: transaction
                        )
                    case .deny, .noAction:
                        break
                    }
                }
            }
        }

        try RelationshipDeleteMarker.clear(
            target,
            transaction: transaction
        )
    }

    private func referrers(
        of target: RecordIdentity,
        descriptor: RelationshipDescriptor,
        transaction: any Transaction,
        state: inout EnforcementState
    ) async throws -> [RecordIdentity] {
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
            transaction: transaction
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
        var visited = Set<RecordIdentity>()
        var mutations = Set<RecordIdentity>()

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
            _ identity: RecordIdentity,
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
