import DatabaseKit
import DatabaseRuntime
import DatabaseWire
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Database entity statement mutation executor")
struct DatabaseEntityStatementMutationExecutorTests {
    @Test("insert update and delete share the framework mutation path")
    func appliesStatementMutationLifecycle() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let executor = try makeExecutor(container: container)
        let entity = EntityMutationFixture.persistableType

        let insertEffects = try await execute(
            .insert(
                InsertQuery(
                    target: TableRef(entity),
                    columns: ["id", "title"],
                    source: .values([[
                        .string("event-1"),
                        .string("Original"),
                    ]])
                )
            ),
            executor: executor,
            container: container
        )
        #expect(insertEffects.count == 1)
        #expect(insertEffects.first?.kind == .insert)
        #expect(
            try await context.model(
                for: "event-1",
                as: EntityMutationFixture.self
            )?.title == "Original"
        )

        let updateEffects = try await execute(
            .update(
                UpdateQuery(
                    target: TableRef(entity),
                    assignments: [
                        Assignment(column: "title", value: .string("Updated"))
                    ],
                    filter: .equal(.col("id"), .string("event-1"))
                )
            ),
            executor: executor,
            container: container
        )
        #expect(updateEffects.count == 1)
        #expect(updateEffects.first?.kind == .update)
        #expect(
            try await context.model(
                for: "event-1",
                as: EntityMutationFixture.self
            )?.title == "Updated"
        )

        let deleteEffects = try await execute(
            .delete(
                DeleteQuery(
                    target: TableRef(entity),
                    filter: .equal(.col("id"), .string("event-1"))
                )
            ),
            executor: executor,
            container: container
        )
        #expect(deleteEffects.count == 1)
        #expect(deleteEffects.first?.kind == .delete)
        #expect(
            try await context.model(
                for: "event-1",
                as: EntityMutationFixture.self
            ) == nil
        )
    }

    @Test("statement mutations enforce preconditions in their transaction")
    func enforcesStatementPreconditions() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let executor = try makeExecutor(container: container)
        let entity = EntityMutationFixture.persistableType
        let identity = try EntityReference(
            entity: entity,
            id: .string("event-2")
        )

        _ = try await execute(
            .insert(
                InsertQuery(
                    target: TableRef(entity),
                    columns: ["id", "title"],
                    source: .values([[
                        .string("event-2"),
                        .string("Original"),
                    ]])
                )
            ),
            executor: executor,
            container: container
        )

        await #expect(
            throws: DatabaseEntityMutationError.entityAlreadyExists(identity)
        ) {
            try await execute(
                .update(
                    UpdateQuery(
                        target: TableRef(entity),
                        assignments: [
                            Assignment(
                                column: "title",
                                value: .string("Changed")
                            )
                        ],
                        filter: .equal(.col("id"), .string("event-2"))
                    )
                ),
                preconditions: [.mustNotExist(identity)],
                executor: executor,
                container: container
            )
        }

        #expect(
            try await context.model(
                for: "event-2",
                as: EntityMutationFixture.self
            )?.title == "Original"
        )
    }

    @Test("statement mutations reserve each retained change before execution")
    func enforcesRetainedChangeRowBudget() async throws {
        let container = try await makeContainer()
        let context = container.testBaseContext()
        let executor = try makeExecutor(container: container)
        let entity = EntityMutationFixture.persistableType

        for identifier in ["event-a", "event-b"] {
            _ = try await execute(
                .insert(
                    InsertQuery(
                        target: TableRef(entity),
                        columns: ["id", "title"],
                        source: .values([[
                            .string(identifier),
                            .string("Original"),
                        ]])
                    )
                ),
                executor: executor,
                container: container
            )
        }

        await #expect(
            throws: DatabaseWorkLimitError.maximumIntermediateRows(
                stage: .mutationPlanning,
                consumed: 1,
                requested: 1,
                maximum: 1
            )
        ) {
            try await execute(
                .update(
                    UpdateQuery(
                        target: TableRef(entity),
                        assignments: [
                            Assignment(
                                column: "title",
                                value: .string("Changed")
                            )
                        ]
                    )
                ),
                executor: executor,
                container: container,
                budget: ExecutionBudget(
                    maximumRows: 16,
                    maximumWorkUnits: 1_000,
                    maximumIntermediateRows: 1,
                    maximumIntermediateBytes: 1 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            )
        }

        await #expect(
            throws: DatabaseEntityStatementMutationError.scanLimitExceeded(
                actual: 2,
                maximum: 1
            )
        ) {
            try await execute(
                .delete(DeleteQuery(target: TableRef(entity))),
                executor: executor,
                container: container,
                budget: ExecutionBudget(
                    maximumRows: 1,
                    maximumWorkUnits: 1_000,
                    maximumIntermediateRows: 16,
                    maximumIntermediateBytes: 1 * 1_024 * 1_024,
                    timeoutMilliseconds: 30_000
                )
            )
        }

        #expect(
            try await context.model(
                for: "event-a",
                as: EntityMutationFixture.self
            )?.title == "Original"
        )
        #expect(
            try await context.model(
                for: "event-b",
                as: EntityMutationFixture.self
            )?.title == "Original"
        )

        let effects = try await execute(
            .update(
                UpdateQuery(
                    target: TableRef(entity),
                    assignments: [
                        Assignment(
                            column: "title",
                            value: .string("Changed")
                        )
                    ],
                    filter: .equal(.col("id"), .string("event-a"))
                )
            ),
            executor: executor,
            container: container,
            budget: ExecutionBudget(
                maximumRows: 16,
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 3,
                maximumIntermediateBytes: 1 * 1_024 * 1_024,
                timeoutMilliseconds: 30_000
            )
        )
        #expect(effects.count == 1)
        #expect(
            try await context.model(
                for: "event-a",
                as: EntityMutationFixture.self
            )?.title == "Changed"
        )
        #expect(
            try await context.model(
                for: "event-b",
                as: EntityMutationFixture.self
            )?.title == "Original"
        )
    }

    private func makeExecutor(
        container: DBContainer
    ) throws -> DatabaseEntityStatementMutationExecutor {
        DatabaseEntityStatementMutationExecutor(
            container: container,
            limits: try DatabaseEntityMutationLimits(
                maximumChanges: 16,
                maximumPreconditions: 16
            )
        )
    }

    private func execute(
        _ statement: QueryStatement,
        preconditions: [EntityMutationPrecondition] = [],
        executor: DatabaseEntityStatementMutationExecutor,
        container: DBContainer,
        budget: ExecutionBudget = ExecutionBudget()
    ) async throws -> [EntityMutationEffect] {
        let context = container.testBaseContext()
        return try await context.withTransaction(configuration: .batch) {
            transaction in
            try await executor.execute(
                statement,
                preconditions: preconditions,
                transaction: transaction,
                workMeter: DatabaseWorkMeter(
                    budget: budget,
                    monotonicClock: container.monotonicClock
                )
            )
        }
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
