import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Database entity mutation executor")
struct DatabaseEntityMutationExecutorTests {
    @Test("entity changes are applied through the framework transaction")
    func appliesEntityChanges() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let executor = try makeExecutor(container: container)
        let model = EntityMutationFixture(id: "one", title: "Original")
        let persisted = try PersistedModel(model)
        let identity = try EntityReference(
            entity: EntityMutationFixture.persistableType,
            id: .string(model.id)
        )
        let fields = try DatabaseEntityProjection.fieldObject(for: persisted)

        let effects = try await context.withTransaction { transaction in
            try await executor.execute(
                [
                    EntityMutationChange(
                        kind: .insert,
                        identity: identity,
                        fields: fields
                    )
                ],
                preconditions: [.mustNotExist(identity)],
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget(),
                    monotonicClock: container.monotonicClock
                ),
                transaction: transaction
            )
        }

        #expect(effects.count == 1)
        #expect(effects.first?.kind == .insert)
        #expect(effects.first?.identity == identity)
        #expect(effects.first?.version != nil)
        #expect(
            try await context.model(
                for: model.id,
                as: EntityMutationFixture.self
            ) == model
        )
    }

    @Test("entity preconditions reject conflicting state")
    func rejectsConflictingPreconditions() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let executor = try makeExecutor(container: container)
        let model = EntityMutationFixture(id: "existing", title: "Original")
        try await context.withTransaction { transaction in
            try await transaction.save(model, precondition: .notExists)
        }
        let identity = try EntityReference(
            entity: EntityMutationFixture.persistableType,
            id: .string(model.id)
        )

        await #expect(
            throws: DatabaseEntityMutationError.entityAlreadyExists(identity)
        ) {
            try await context.withTransaction { transaction in
                try await executor.validate(
                    [.mustNotExist(identity)],
                    transaction: transaction,
                    workMeter: DatabaseWorkMeter(
                        budget: ExecutionBudget(),
                        monotonicClock: container.monotonicClock
                    )
                )
            }
        }
    }

    private func makeExecutor(
        container: DBContainer
    ) throws -> DatabaseEntityMutationExecutor {
        DatabaseEntityMutationExecutor(
            container: container,
            limits: try DatabaseEntityMutationLimits(
                maximumChanges: 16,
                maximumPreconditions: 16
            )
        )
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try EntityMutationFixture.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        EntityMutationFixture.self
                    )
                ]
            ),
            security: .testingDisabled
        )
    }
}
