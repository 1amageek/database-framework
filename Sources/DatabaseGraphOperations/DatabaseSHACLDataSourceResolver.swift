import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
@_spi(DatabaseOperations) import DatabaseWire
import GraphIndex
import StorageKit

public protocol DatabaseSHACLDataSourceResolver: Sendable {
    func resolve(
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> DatabaseSHACLResolvedDataSource
}
#endif
