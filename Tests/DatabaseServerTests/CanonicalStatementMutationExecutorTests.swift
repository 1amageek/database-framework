import DatabaseKit
import DatabaseRuntime
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import Testing
@testable import DatabaseServer

@Suite("Canonical statement mutation executor")
struct CanonicalStatementMutationExecutorTests {
    @Test("Statement mutation preparation uses configured structural limits")
    func mutationPreparationUsesConfiguredStructuralLimits() throws {
        let admission = DatabaseStatementAdmission(
            structuralLimits: QueryStructuralLimits(
                maximumCollectionElements: 1
            )
        )
        let statement = QueryStatement.insert(
            InsertQuery(
                target: TableRef(DatabaseEndpointEntity.persistableType),
                columns: ["id", "title"],
                source: .defaultValues
            )
        )

        do {
            _ = try admission.admit(
                .ir(statement),
                parameters: []
            )
            Issue.record("Expected the collection limit to reject the mutation")
        } catch QueryParameterBindingError.invalidStructure(let error) {
            #expect(
                error == .resourceLimitExceeded(
                    resource: .collectionElements,
                    actual: 2,
                    maximum: 1
                )
            )
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Mutation parameters are rejected before recursive binding")
    func mutationParametersAreValidatedBeforeBinding() throws {
        var value = FieldValue.object(FieldObject())
        for _ in 0..<7 {
            value = .array([value])
        }
        let admission = DatabaseStatementAdmission(
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 6
            )
        )
        let statement = QueryStatement.insert(
            InsertQuery(
                target: TableRef(DatabaseEndpointEntity.persistableType),
                columns: ["title"],
                source: .values([[.parameter(.position(1))]])
            )
        )

        do {
            _ = try admission.admit(
                .ir(statement),
                parameters: [
                    QueryParameter(
                        position: 1,
                        name: "value",
                        value: value
                    ),
                ]
            )
            Issue.record("Expected parameter preflight to reject recursive binding")
        } catch QueryParameterBindingError.invalidStructure(let error) {
            #expect(
                error == .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 7,
                    maximum: 6
                )
            )
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("INSERT UPDATE and DELETE share the canonical entity mutation path")
    func sqlDataModificationLifecycle() async throws {
        let container = try await makeContainer()
        let context = DatabaseOperationContext(
            container: container,
            requestID: 1,
            metadata: OperationRequestMetadata(),
            requestPayload: []
        )
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let entity = DatabaseEndpointEntity.persistableType

        let insert = InsertQuery(
            target: TableRef(entity),
            columns: ["id", "title", "priority"],
            source: .values([[
                .parameter(.position(1)),
                .parameter(.name("title")),
                .int(4),
            ]])
        )
        let insertEffects = try await execute(
            .insert(insert),
            parameters: [
                QueryParameter(
                    position: 1,
                    name: "id",
                    value: .string("event-1")
                ),
                QueryParameter(
                    position: 2,
                    name: "title",
                    value: .string("Runtime")
                ),
            ],
            executor: executor,
            context: context
        )
        #expect(insertEffects.count == 1)
        #expect(insertEffects[0].kind == .insert)

        let inserted = try await load("event-1", container: container)
        #expect(inserted?.title == "Runtime")
        #expect(inserted?.priority == 4)

        let update = UpdateQuery(
            target: TableRef(entity),
            assignments: [
                Assignment(
                    column: "priority",
                    value: .add(.col("priority"), .parameter(.position(2)))
                ),
            ],
            filter: .equal(.col("id"), .parameter(.position(1)))
        )
        let updateEffects = try await execute(
            .update(update),
            parameters: [
                QueryParameter(
                    position: 1,
                    name: "id",
                    value: .string("event-1")
                ),
                QueryParameter(
                    position: 2,
                    name: "increment",
                    value: .int64(3)
                ),
            ],
            executor: executor,
            context: context
        )
        #expect(updateEffects.count == 1)
        #expect(updateEffects[0].kind == MutationExecuteOperation.Kind.update)
        #expect(try await load("event-1", container: container)?.priority == 7)

        let delete = DeleteQuery(
            target: TableRef(entity),
            filter: .equal(.col("id"), .parameter(.name("id")))
        )
        let deleteEffects = try await execute(
            .delete(delete),
            parameters: [
                QueryParameter(
                    position: 1,
                    name: "id",
                    value: .string("event-1")
                ),
            ],
            executor: executor,
            context: context
        )
        #expect(deleteEffects.count == 1)
        #expect(deleteEffects[0].kind == MutationExecuteOperation.Kind.delete)
        #expect(try await load("event-1", container: container) == nil)
    }

    @Test("Statement mutations enforce entity preconditions in the mutation transaction")
    func statementPreconditionsAreEnforced() async throws {
        let container = try await makeContainer()
        let context = DatabaseOperationContext(
            container: container,
            requestID: 2,
            metadata: OperationRequestMetadata(),
            requestPayload: []
        )
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let entity = DatabaseEndpointEntity.persistableType
        let identity = try EntityReference(entity: entity, id: .string("event-2"))

        _ = try await execute(
            .insert(InsertQuery(
                target: TableRef(entity),
                columns: ["id", "title", "priority"],
                source: .values([[
                    .string("event-2"),
                    .string("Original"),
                    .int(1),
                ]])
            )),
            parameters: [],
            executor: executor,
            context: context
        )

        do {
            _ = try await execute(
                .update(UpdateQuery(
                    target: TableRef(entity),
                    assignments: [Assignment(column: "title", value: .string("Changed"))],
                    filter: .equal(.col("id"), .string("event-2"))
                )),
                parameters: [],
                preconditions: [.mustNotExist(identity)],
                executor: executor,
                context: context
            )
            Issue.record("Expected the statement precondition to reject the update")
        } catch DatabaseMutationError.entityAlreadyExists(let rejectedIdentity) {
            #expect(rejectedIdentity == identity)
        }

        #expect(try await load("event-2", container: container)?.title == "Original")

        let missingIdentity = try EntityReference(entity: entity, id: .string("missing"))
        do {
            _ = try await execute(
                .update(UpdateQuery(
                    target: TableRef(entity),
                    assignments: [Assignment(column: "title", value: .string("Unused"))],
                    filter: .equal(.col("id"), .string("does-not-match"))
                )),
                parameters: [],
                preconditions: [.mustExist(missingIdentity)],
                executor: executor,
                context: context
            )
            Issue.record("Expected preconditions to run when a statement matches no entities")
        } catch DatabaseMutationError.entityNotFound(let rejectedIdentity) {
            #expect(rejectedIdentity == missingIdentity)
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [DatabaseEndpointEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseEndpointEntity.self]
            ),
            security: .disabled
        )
    }

    private func execute(
        _ statement: QueryStatement,
        parameters: [QueryParameter],
        preconditions: [MutationExecuteOperation.Precondition] = [],
        executor: CanonicalDatabaseStatementMutationExecutor,
        context: DatabaseOperationContext
    ) async throws -> [MutationExecuteOperation.EntityEffect] {
        let statement = try DatabaseStatementAdmission(
            structuralLimits: .default
        ).admit(
            .ir(statement),
            parameters: parameters
        )
        let prepared = try await executor.prepare(
            statement,
            context: context
        )
        let database = context.container.newContext()
        let result = try await database.withTransaction(configuration: .batch) { transaction in
            try await executor.execute(
                prepared,
                preconditions: preconditions,
                graphPartitions: FieldObject(),
                context: context,
                transaction: transaction
            )
        }
        guard case .entities(let effects) = result else {
            throw DatabaseMutationError.unsupportedStatement(
                "Expected an entity mutation result"
            )
        }
        return effects
    }

    private func load(
        _ id: String,
        container: DBContainer
    ) async throws -> DatabaseEndpointEntity? {
        let database = container.newContext()
        return try await database.withTransaction { transaction in
            try await transaction.loadPersistedModel(
                entity: DatabaseEndpointEntity.persistableType,
                id: Tuple(id),
                partition: nil
            ) as? DatabaseEndpointEntity
        }
    }
}
