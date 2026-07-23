import DatabaseWire
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
