import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit

/// A type-erased, immutable statement preparation reusable across transaction retries.
public final class PreparedDatabaseStatementMutation: Sendable {
    private let executeMutation: @Sendable (
        [MutationExecuteOperation.Precondition],
        [DatabaseObjectField],
        DatabaseOperationContext,
        any Transaction
    ) async throws -> MutationExecuteOperation.Result

    init<Executor: DatabaseStatementMutationExecutor>(
        executor: Executor,
        prepared: Executor.PreparedStatementMutation
    ) {
        self.executeMutation = {
            preconditions,
            graphPartitions,
            context,
            transaction in
            try await executor.execute(
                prepared,
                preconditions: preconditions,
                graphPartitions: graphPartitions,
                context: context,
                transaction: transaction
            )
        }
    }

    public func execute(
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: [DatabaseObjectField],
        context: DatabaseOperationContext,
        transaction: any Transaction
    ) async throws -> MutationExecuteOperation.Result {
        try await executeMutation(
            preconditions,
            graphPartitions,
            context,
            transaction
        )
    }
}
