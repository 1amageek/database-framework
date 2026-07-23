#if !os(WASI)
#if FOUNDATION_DB
import Core
@testable import DatabaseEngine
import DatabaseRuntime
import DatabaseValue
import FDBStorage
import Foundation
import Relationship
@testable import RelationshipIndex
import StorageKit
import Testing
import TestSupport

@Suite("Typed relationship runtime", .serialized, .heartbeat)
struct RelationshipReferenceE2ETests {
    @Test("A target and owner may be inserted in either order in one transaction")
    func sameBatchReferenceValidation() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            var target = RelationshipTarget(name: "Target")
            target.id = uniqueID("target")
            var owner = RelationshipOptionalOwner(
                name: "Owner",
                target: try context.reference(to: target)
            )
            owner.id = uniqueID("owner")

            context.insert(owner)
            context.insert(target)
            try await context.save()

            let loaded = try await context.related(owner, \.target)
            #expect(loaded?.id == target.id)
        }
    }

    @Test("Mutually referencing records validate against final transaction state")
    func cyclicReferences() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            let firstID = uniqueID("cycle-a")
            let secondID = uniqueID("cycle-b")
            let firstReference = try RelationshipReferenceFactory.make(
                RelationshipCycleNode.self,
                id: firstID
            )
            let secondReference = try RelationshipReferenceFactory.make(
                RelationshipCycleNode.self,
                id: secondID
            )
            var first = RelationshipCycleNode(peer: secondReference)
            first.id = firstID
            var second = RelationshipCycleNode(peer: firstReference)
            second.id = secondID

            context.insert(first)
            context.insert(second)
            try await context.save()

            #expect(try await context.related(first, \.peer)?.id == secondID)
            #expect(try await context.related(second, \.peer)?.id == firstID)
        }
    }

    @Test("Missing targets reject and roll back the complete save")
    func missingTargetRollsBack() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            let ownerID = uniqueID("missing-owner")
            var owner = RelationshipOptionalOwner(
                name: "Invalid",
                target: try RelationshipReferenceFactory.make(
                    RelationshipTarget.self,
                    id: uniqueID("missing-target")
                )
            )
            owner.id = ownerID
            context.insert(owner)

            await #expect(throws: RelationshipReferenceError.self) {
                try await context.save()
            }

            let verification = container.newContext()
            #expect(
                try await verification.model(
                    for: ownerID,
                    as: RelationshipOptionalOwner.self
                ) == nil
            )
        }
    }

    @Test("Typed joins and inverse pagination use the canonical catalog")
    func joinAndInversePagination() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            var target = RelationshipTarget(name: "Paged")
            target.id = uniqueID("paged-target")
            let targetReference = try context.reference(to: target)
            let owners = (0..<5).map { offset in
                var owner = RelationshipOptionalOwner(
                    name: "Owner \(offset)",
                    target: targetReference
                )
                owner.id = uniqueID("paged-owner-\(offset)")
                return owner
            }
            context.insert(target)
            for owner in owners {
                context.insert(owner)
            }
            try await context.save()

            let snapshots = try await context
                .fetch(RelationshipOptionalOwner.self)
                .joining(\.target)
                .execute()
            let joinedIDs = Set(snapshots.compactMap { $0.ref(\.target)?.id })
            #expect(joinedIDs == [target.id])

            let resolver = context.inverseRelationshipResolver()
            let firstPage = try await resolver.referencedBy(
                targetReference,
                from: RelationshipOptionalOwner.self,
                via: \.target,
                limit: 2
            )
            let continuation = try #require(firstPage.continuation)
            let secondPage = try await resolver.referencedBy(
                targetReference,
                from: RelationshipOptionalOwner.self,
                via: \.target,
                limit: 2,
                continuation: continuation
            )
            guard let secondContinuation = secondPage.continuation else {
                Issue.record("Expected a continuation after the second page")
                return
            }
            let thirdPage = try await resolver.referencedBy(
                targetReference,
                from: RelationshipOptionalOwner.self,
                via: \.target,
                limit: 2,
                continuation: secondContinuation
            )
            let ownerIDs = Set(
                (firstPage.records + secondPage.records + thirdPage.records)
                    .map(\.id)
            )

            #expect(firstPage.records.count == 2)
            #expect(secondPage.records.count == 2)
            #expect(thirdPage.records.count == 1)
            #expect(thirdPage.continuation == nil)
            #expect(ownerIDs == Set(owners.map(\.id)))
        }
    }

    @Test("Nullify removes optional and array references atomically")
    func nullifyDeleteRule() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            var retained = RelationshipTarget(name: "Retained")
            retained.id = uniqueID("retained")
            var removed = RelationshipTarget(name: "Removed")
            removed.id = uniqueID("removed")
            var optionalOwner = RelationshipOptionalOwner(
                name: "Optional",
                target: try context.reference(to: removed)
            )
            optionalOwner.id = uniqueID("optional")
            var arrayOwner = RelationshipArrayOwner(
                targets: [
                    try context.reference(to: retained),
                    try context.reference(to: removed),
                ]
            )
            arrayOwner.id = uniqueID("array")
            context.insert(retained)
            context.insert(removed)
            context.insert(optionalOwner)
            context.insert(arrayOwner)
            try await context.save()

            context.delete(removed)
            try await context.save()

            let loadedOptional = try await context.model(
                for: optionalOwner.id,
                as: RelationshipOptionalOwner.self
            )
            let loadedArray = try await context.model(
                for: arrayOwner.id,
                as: RelationshipArrayOwner.self
            )
            #expect(loadedOptional?.target == nil)
            #expect(loadedArray?.targets.map(\.identity) == [
                try context.reference(to: retained).identity,
            ])
        }
    }

    @Test("Deny keeps both target and owner when deletion is rejected")
    func denyDeleteRule() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            var target = RelationshipTarget(name: "Protected")
            target.id = uniqueID("deny-target")
            var owner = RelationshipDenyOwner(
                target: try context.reference(to: target)
            )
            owner.id = uniqueID("deny-owner")
            context.insert(target)
            context.insert(owner)
            try await context.save()

            context.delete(target)
            await #expect(throws: RelationshipError.self) {
                try await context.save()
            }

            let verification = container.newContext()
            #expect(
                try await verification.model(
                    for: target.id,
                    as: RelationshipTarget.self
                ) != nil
            )
            #expect(
                try await verification.model(
                    for: owner.id,
                    as: RelationshipDenyOwner.self
                ) != nil
            )
        }
    }

    @Test("Cascade deletes incoming owners in the same transaction")
    func cascadeDeleteRule() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            var target = RelationshipTarget(name: "Root")
            target.id = uniqueID("cascade-target")
            var owner = RelationshipCascadeOwner(
                target: try context.reference(to: target)
            )
            owner.id = uniqueID("cascade-owner")
            context.insert(target)
            context.insert(owner)
            try await context.save()

            context.delete(target)
            try await context.save()

            let verification = container.newContext()
            #expect(
                try await verification.model(
                    for: target.id,
                    as: RelationshipTarget.self
                ) == nil
            )
            #expect(
                try await verification.model(
                    for: owner.id,
                    as: RelationshipCascadeOwner.self
                ) == nil
            )
        }
    }

    @Test("References distinguish records with equal IDs in different partitions")
    func partitionIdentity() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await makeContainer()
            let context = container.newContext()
            let sharedID = uniqueID("partitioned-target")
            var first = RelationshipPartitionedTarget(
                tenantID: "tenant-a",
                name: "First"
            )
            first.id = sharedID
            var second = RelationshipPartitionedTarget(
                tenantID: "tenant-b",
                name: "Second"
            )
            second.id = sharedID
            var owner = RelationshipPartitionedOwner(
                target: try context.reference(to: second)
            )
            owner.id = uniqueID("partitioned-owner")
            context.insert(first)
            context.insert(second)
            context.insert(owner)
            try await context.save()

            let loaded = try await context.related(owner, \.target)
            #expect(loaded?.tenantID == "tenant-b")
            #expect(loaded?.name == "Second")
            #expect(owner.target.identity.partitions.count == 1)
            #expect(owner.target.identity.partitions.first?.name == "tenantID")
            #expect(owner.target.identity.partitions.first?.value == .string("tenant-b"))
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = Schema(
            [
                RelationshipTarget.self,
                RelationshipOptionalOwner.self,
                RelationshipArrayOwner.self,
                RelationshipDenyOwner.self,
                RelationshipCascadeOwner.self,
                RelationshipCycleNode.self,
                RelationshipPartitionedTarget.self,
                RelationshipPartitionedOwner.self,
            ],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString)"
    }
}
#endif
#endif
