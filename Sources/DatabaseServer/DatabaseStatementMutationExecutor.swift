import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit

public protocol DatabaseStatementMutationExecutor: Sendable {
    associatedtype PreparedStatementMutation: Sendable

    func prepare(
        _ statement: ValidatedDatabaseStatement,
        budget: DatabaseExecutionBudget,
        context: DatabaseOperationContext
    ) async throws -> PreparedStatementMutation

    func execute(
        _ prepared: PreparedStatementMutation,
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: [DatabaseObjectField],
        context: DatabaseOperationContext,
        transaction: any Transaction
    ) async throws -> MutationExecuteOperation.Result
}
