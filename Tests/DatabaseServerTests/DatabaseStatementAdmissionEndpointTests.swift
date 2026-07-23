import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseValue
import DatabaseWire
import QueryIR
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
        let frame = try DatabaseEnvelopeCodec.encodeRequest(
            MutationExecuteOperation.self,
            requestID: 1,
            metadata: DatabaseRequestMetadata(
                idempotencyKey: "statement-admission"
            ),
            request: request
        )
        let response = try DatabaseEnvelopeCodec.decodeResponse(
            try await endpoint.execute(frame)
        )

        guard case .failure(let error) = response.payload else {
            Issue.record("Expected canonical admission to reject the statement")
            return
        }
        #expect(error.category == .resourceLimit)
        #expect(error.code == "QUERY_RESOURCE_LIMIT")
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: Schema(
                [DatabaseEndpointEntity.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                backend: .custom(InMemoryEngine())
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }
}

private struct UnreachableStatementMutationExecutor:
    DatabaseStatementMutationExecutor {
    struct Prepared: Sendable {}

    func prepare(
        _ statement: ValidatedDatabaseStatement,
        budget: DatabaseExecutionBudget,
        context: DatabaseOperationContext
    ) async throws -> Prepared {
        throw UnreachableStatementMutationExecutorError.prepareCalled
    }

    func execute(
        _ prepared: Prepared,
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: [DatabaseObjectField],
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
