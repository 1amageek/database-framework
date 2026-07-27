import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire
import GraphIndex
import StorageKit

public protocol DatabaseSHACLProcessor: Sendable {
    func validateShapes(
        graph: String,
        quads: [RDFQuad],
        workBudget: SHACLValidationWorkBudget
    ) throws

    func validate(
        shapesGraph: String,
        data: SHACLExecuteOperation.DataSource,
        focus: SHACLExecuteOperation.Focus,
        entailment: SHACLExecuteOperation.Entailment,
        page: QueryExecuteOperation.Page,
        workBudget: SHACLValidationWorkBudget,
        transaction: any TransactionAccess
    ) async throws -> ValidationReport
}
