import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseValue
import DatabaseWire
import RelationshipIndex
import StorageKit
import Testing
@testable import DatabaseServer

@Suite("Database relationship mutation planner")
struct DatabaseRelationshipMutationPlannerTests {
    @Test("Cascade effects include the complete recursive write set")
    func cascadeEffects() async throws {
        let container = try await makeContainer()
        var target = RelationshipPlannerTarget(name: "target")
        target.id = "target"
        var owner = RelationshipPlannerCascadeOwner()
        owner.id = "owner"
        owner.target = try reference(to: target)
        var grandchild = RelationshipPlannerGrandchild()
        grandchild.id = "grandchild"
        grandchild.owner = try reference(to: owner)
        try await seed([target, owner, grandchild], container: container)

        let effects = try await delete(
            target,
            container: container
        )

        #expect(Set(effects.map(\.identity.entity)) == Set([
            RelationshipPlannerTarget.persistableType,
            RelationshipPlannerCascadeOwner.persistableType,
            RelationshipPlannerGrandchild.persistableType,
        ]))
        #expect(effects.allSatisfy { $0.kind == .delete })
        #expect(try await load(target, container: container) == nil)
        #expect(try await load(owner, container: container) == nil)
        #expect(try await load(grandchild, container: container) == nil)
    }

    @Test("Nullify updates optional and to-many fields without deleting owners")
    func nullifyEffects() async throws {
        let container = try await makeContainer()
        var first = RelationshipPlannerTarget(name: "first")
        first.id = "first"
        var second = RelationshipPlannerTarget(name: "second")
        second.id = "second"
        var owner = RelationshipPlannerNullifyOwner()
        owner.id = "owner"
        owner.target = try reference(to: first)
        var arrayOwner = RelationshipPlannerArrayOwner()
        arrayOwner.id = "array-owner"
        let secondReference = try reference(to: second)
        arrayOwner.targets = [try reference(to: first), secondReference]
        try await seed([first, second, owner, arrayOwner], container: container)

        let effects = try await delete(first, container: container)

        #expect(effects.filter { $0.kind == .update }.count == 2)
        let loadedOwner: RelationshipPlannerNullifyOwner? = try await load(
            owner,
            container: container
        )
        let loadedArray: RelationshipPlannerArrayOwner? = try await load(
            arrayOwner,
            container: container
        )
        #expect(loadedOwner?.target == nil)
        #expect(loadedArray?.targets == [secondReference])
    }

    @Test("Deny aborts the entire transaction")
    func denyRollsBack() async throws {
        let container = try await makeContainer()
        var target = RelationshipPlannerTarget(name: "target")
        target.id = "deny-target"
        var owner = RelationshipPlannerDenyOwner()
        owner.id = "deny-owner"
        owner.target = try reference(to: target)
        try await seed([target, owner], container: container)

        await #expect(throws: RelationshipError.self) {
            _ = try await delete(target, container: container)
        }
        #expect(try await load(target, container: container) != nil)
        #expect(try await load(owner, container: container) != nil)
    }

    @Test("Cascade cycles terminate and emit one effect per record")
    func cascadeCycle() async throws {
        let container = try await makeContainer()
        var first = RelationshipPlannerCycleNode()
        first.id = "cycle-a"
        var second = RelationshipPlannerCycleNode()
        second.id = "cycle-b"
        first.parent = try reference(to: second)
        second.parent = try reference(to: first)
        try await seed([first, second], container: container)

        let effects = try await delete(first, container: container)

        #expect(effects.count == 2)
        #expect(Set(effects.map(\.identity.id)) == Set([
            .string(first.id),
            .string(second.id),
        ]))
    }

    @Test("Partition identity isolates equal IDs in different partitions")
    func partitionIsolation() async throws {
        let container = try await makeContainer()
        var firstTarget = RelationshipPlannerPartitionedTarget(runID: "run-a")
        firstTarget.id = "shared"
        var secondTarget = RelationshipPlannerPartitionedTarget(runID: "run-b")
        secondTarget.id = "shared"
        var firstOwner = RelationshipPlannerPartitionedOwner(runID: "run-a")
        firstOwner.id = "owner-a"
        firstOwner.target = try reference(to: firstTarget)
        var secondOwner = RelationshipPlannerPartitionedOwner(runID: "run-b")
        secondOwner.id = "owner-b"
        secondOwner.target = try reference(to: secondTarget)
        try await seed(
            [firstTarget, secondTarget, firstOwner, secondOwner],
            container: container
        )

        let effects = try await delete(firstTarget, container: container)

        #expect(effects.count == 2)
        #expect(try await load(firstOwner, container: container) == nil)
        #expect(try await load(secondOwner, container: container) != nil)
        #expect(try await load(secondTarget, container: container) != nil)
    }

    @Test("Implicit mutations share the explicit mutation limit")
    func implicitMutationLimit() async throws {
        let container = try await makeContainer()
        var target = RelationshipPlannerTarget(name: "target")
        target.id = "limited"
        var first = RelationshipPlannerCascadeOwner()
        first.id = "limited-a"
        first.target = try reference(to: target)
        var second = RelationshipPlannerCascadeOwner()
        second.id = "limited-b"
        second.target = try reference(to: target)
        try await seed([target, first, second], container: container)

        await #expect(throws: DatabaseMutationError.self) {
            _ = try await delete(
                target,
                container: container,
                maximumMutations: 2
            )
        }
        #expect(try await load(target, container: container) != nil)
        #expect(try await load(first, container: container) != nil)
        #expect(try await load(second, container: container) != nil)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer(
            for: Schema([
                RelationshipPlannerTarget.self,
                RelationshipPlannerCascadeOwner.self,
                RelationshipPlannerGrandchild.self,
                RelationshipPlannerNullifyOwner.self,
                RelationshipPlannerArrayOwner.self,
                RelationshipPlannerDenyOwner.self,
                RelationshipPlannerCycleNode.self,
                RelationshipPlannerPartitionedTarget.self,
                RelationshipPlannerPartitionedOwner.self,
            ]),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }

    private func seed(
        _ models: [any Persistable],
        container: DBContainer
    ) async throws {
        let context = container.newContext()
        for model in models {
            context.insert(model)
        }
        try await context.save()
    }

    private func reference<Target: Persistable>(
        to target: Target
    ) throws -> DatabaseReference<Target> {
        try DatabaseReference(
            identity: DatabaseRecordIdentityEncoder.encode(target)
        )
    }

    private func delete<T: Persistable>(
        _ model: T,
        container: DBContainer,
        maximumMutations: Int = 1_000
    ) async throws -> [MutationExecuteOperation.RecordEffect] {
        let executor = DatabaseRecordMutationExecutor(
            container: container,
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10_000,
                maximumWorkUnits: 1_000_000,
                maximumTimeoutMilliseconds: 30_000,
                maximumMutations: maximumMutations
            )
        )
        let identity = try DatabaseRecordProjection.identity(for: model)
        let persistence = container.newContext().makePersistenceHandler()
        return try await container.engine.withTransaction(
            configuration: .batch
        ) { transaction in
            try await executor.execute(
                [MutationExecuteOperation.Change(kind: .delete, identity: identity)],
                workMeter: DatabaseWorkMeter(
                    budget: DatabaseExecutionBudget(
                        maximumWorkUnits: 1_000_000
                    )
                ),
                transaction: transaction,
                persistence: persistence
            )
        }
    }

    private func load<T: Persistable>(
        _ model: T,
        container: DBContainer
    ) async throws -> T? {
        let identity = try DatabaseRecordProjection.identity(for: model)
        let resolved = try DatabaseResolvedRecordIdentity.resolve(
            identity,
            container: container
        )
        let persistence = container.newContext().makePersistenceHandler()
        return try await container.engine.withTransaction { transaction in
            try await persistence.load(
                identity.entity,
                id: resolved.id,
                partition: resolved.partition,
                transaction: transaction
            ) as? T
        }
    }
}
