import DatabaseWire
import GraphIndex
import StorageKit

public protocol DatabaseSHACLProcessor: Sendable {
    func replace(
        graph: String,
        quads: [DatabaseRDFQuad],
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws

    func delete(
        graph: String,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws

    func validate(
        shapesGraph: String,
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        page: QueryExecuteOperation.Page,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> DatabaseValidationReport
}
