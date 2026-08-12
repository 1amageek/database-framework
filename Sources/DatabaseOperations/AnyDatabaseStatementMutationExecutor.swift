import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

/// Type-erased statement mutation executor for runtime composition.
public final class AnyDatabaseStatementMutationExecutor:
    Sendable {
    private let prepareMutation: @Sendable (
        ValidatedDatabaseStatement,
        ExecutionBudget,
        DatabaseOperationContext
    ) async throws -> PreparedDatabaseStatementMutation

    public init<Executor: DatabaseStatementMutationExecutor>(
        _ executor: Executor
    ) {
        self.prepareMutation = {
            statement,
            budget,
            context in
            let prepared = try await executor.prepare(
                statement,
                budget: budget,
                context: context
            )
            return PreparedDatabaseStatementMutation(
                executor: executor,
                prepared: prepared
            )
        }
    }

    public func prepare(
        _ statement: ValidatedDatabaseStatement,
        budget: ExecutionBudget,
        context: DatabaseOperationContext
    ) async throws -> PreparedDatabaseStatementMutation {
        try await prepareMutation(
            statement,
            budget,
            context
        )
    }
}
