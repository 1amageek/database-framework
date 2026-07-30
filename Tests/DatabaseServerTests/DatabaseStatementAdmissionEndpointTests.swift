import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Suite("Statement admission endpoint", .serialized)
struct DatabaseStatementAdmissionEndpointTests {
    @Test("Mutation admission cannot be replaced by a custom executor")
    func customExecutorCannotBypassAdmission() async throws {
        let container = try await makeContainer()
        let runtimeLimits = try DatabaseRuntimeLimits(
            maximumRows: 10_000,
            maximumWorkUnits: 1_000_000,
            maximumTimeoutMilliseconds: 30_000,
            queryStructuralLimits: QueryStructuralLimits(
                maximumCollectionElements: 1
            )
        )
        let handler = MutationExecuteHandler(
            stateStore: try await DatabaseMutationStateStore(
                container: container
            ),
            statementExecutor: AnyDatabaseStatementMutationExecutor(
                UnreachableStatementMutationExecutor()
            ),
            runtimeLimits: runtimeLimits
        )
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.mutationExecute]
        )
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            )
        )
        let request = MutationExecuteOperation.Request(
            input: .statement(
                .ir(
                    .insert(
                        InsertQuery(
                            target: TableRef(
                                DatabaseEndpointEntity.persistableType
                            ),
                            columns: ["id", "title"],
                            source: .defaultValues
                        )
                    )
                ),
                parameters: []
            )
        )
        let frame = try DatabaseWireEncoder().encodeRequest(
            DatabaseOperations.mutationExecute,
            requestID: 1,
            metadata: OperationRequestMetadata(
                idempotencyKey: "statement-admission"
            ),
            request: request
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.mutationExecute,
            from: try await endpoint.execute(frame),
            matching: 1
        )

        guard case .failure(let error) = response else {
            Issue.record("Expected canonical admission to reject the statement")
            return
        }
        #expect(error.category == .resourceLimit)
        #expect(error.code == "QUERY_RESOURCE_LIMIT")
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [try DatabaseEndpointEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(
                backend: .custom(InMemoryEngine())
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .disabled
        )
    }
}

private struct UnreachableStatementMutationExecutor:
    DatabaseStatementMutationExecutor {
    struct Prepared: Sendable {}

    func prepare(
        _ statement: ValidatedDatabaseStatement,
        budget: ExecutionBudget,
        context: DatabaseOperationContext
    ) async throws -> Prepared {
        throw UnreachableStatementMutationExecutorError.prepareCalled
    }

    func execute(
        _ prepared: Prepared,
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: FieldObject,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> MutationExecuteOperation.Result {
        throw UnreachableStatementMutationExecutorError.executeCalled
    }
}

private enum UnreachableStatementMutationExecutorError: Error {
    case prepareCalled
    case executeCalled
}
