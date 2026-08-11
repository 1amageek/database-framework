import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

public protocol DatabaseStatementMutationExecutor: Sendable {
    associatedtype PreparedStatementMutation: Sendable

    func prepare(
        _ statement: ValidatedDatabaseStatement,
        budget: ExecutionBudget,
        context: DatabaseOperationContext
    ) async throws -> PreparedStatementMutation

    func execute(
        _ prepared: PreparedStatementMutation,
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: FieldObject,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> MutationExecuteOperation.Result
}
