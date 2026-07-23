import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import RelationshipIndex
import StorageKit

public struct MutationExecuteHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.mutationExecute

    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let statementExecutor: AnyDatabaseStatementMutationExecutor
    private let statementAdmission: DatabaseStatementAdmission
    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits

    public init(
        stateStore: DatabaseMutationStateStore,
        statementExecutor: AnyDatabaseStatementMutationExecutor,
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.coordinator = DatabaseTransactionalOperationCoordinator(
            stateStore: stateStore,
            runtimeLimits: runtimeLimits,
            wireLimits: wireLimits
        )
        self.statementExecutor = statementExecutor
        self.statementAdmission = DatabaseStatementAdmission(
            structuralLimits: runtimeLimits.queryStructuralLimits
        )
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
    }

    public func invoke(
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        let request = try DatabaseEnvelopeCodec.decode(
            MutationExecuteOperation.Request.self,
            from: payload,
            limits: limits
        )
        try runtimeLimits.validate(request.budget)
        let requestPayload = context.requestPayload

        let recordExecutor = DatabaseRecordMutationExecutor(
            container: context.container,
            runtimeLimits: runtimeLimits
        )
        switch request.input {
        case .records(let changes):
            guard request.graphPartitions.isEmpty else {
                throw DatabaseMutationError.invalidGraphPartitions(
                    "record mutations do not consume graph partitions"
                )
            }
            let persistence = context.container.newContext()
                .makePersistenceHandler()
            return try await coordinator.executeStaged(
                operation: .mutationExecute,
                requestPayload: requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds,
                prepare: {
                    let workMeter = DatabaseWorkMeter(budget: request.budget)
                    let preparedChanges = try recordExecutor.prepare(
                        changes,
                        preconditions: request.preconditions,
                        workMeter: workMeter
                    )
                    return PreparedRecordMutation(
                        changes: preparedChanges,
                        workMeter: workMeter
                    )
                },
                body: { prepared, transactionContext in
                    MutationExecuteOperation.Result.records(
                        try await recordExecutor.execute(
                            prepared.changes,
                            preconditions: request.preconditions,
                            workMeter: prepared.workMeter,
                            transaction: transactionContext.rawTransaction,
                            persistence: persistence
                        )
                    )
                },
                makeResponse: { result, commitVersion in
                    DatabaseOperationResponseEncoder(
                        MutationExecuteOperation.Response(
                            commitVersion: commitVersion,
                            result: result
                        )
                    )
                }
            ).result

        case .statement(let input, let parameters):
            guard request.preconditions.count
                    <= runtimeLimits.maximumPreconditions else {
                throw DatabaseMutationError.preconditionLimitExceeded(
                    actual: request.preconditions.count,
                    maximum: runtimeLimits.maximumPreconditions
                )
            }
            return try await coordinator.executeStaged(
                operation: .mutationExecute,
                requestPayload: requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds,
                prepare: {
                    let statement = try statementAdmission.admit(
                        input,
                        parameters: parameters
                    )
                    return try await statementExecutor.prepare(
                        statement,
                        budget: request.budget,
                        context: context
                    )
                },
                body: { prepared, transactionContext in
                    try await prepared.execute(
                        preconditions: request.preconditions,
                        graphPartitions: request.graphPartitions,
                        context: context,
                        transaction: transactionContext.rawTransaction
                    )
                },
                makeResponse: { result, commitVersion in
                    DatabaseOperationResponseEncoder(
                        MutationExecuteOperation.Response(
                            commitVersion: commitVersion,
                            result: result
                        )
                    )
                }
            ).result
        }
    }
}

private struct PreparedRecordMutation: Sendable {
    let changes: [DatabaseRecordMutationExecutor.PreparedChange]
    let workMeter: DatabaseWorkMeter
}
