import DatabaseWire
import StorageKit

public protocol DatabaseOntologyProcessor: Sendable {
    func replace(
        _ document: OntologyExecuteOperation.Document,
        budget: DatabaseExecutionBudget,
        transaction: any TransactionAccess
    ) async throws

    func delete(
        ontology: String,
        budget: DatabaseExecutionBudget,
        transaction: any TransactionAccess
    ) async throws

    func reason(
        ontology: String,
        profile: OntologyExecuteOperation.ReasoningProfile,
        page: QueryExecuteOperation.Page,
        budget: DatabaseExecutionBudget,
        transaction: any TransactionAccess
    ) async throws -> OntologyExecuteOperation.InferencePage

    func hierarchy(
        ontology: String,
        resource: String,
        resourceKind: OntologyExecuteOperation.HierarchyResourceKind,
        direction: OntologyExecuteOperation.HierarchyDirection,
        maximumDepth: UInt32,
        page: QueryExecuteOperation.Page,
        budget: DatabaseExecutionBudget,
        transaction: any TransactionAccess
    ) async throws -> OntologyExecuteOperation.HierarchyPage

    func validateSchema(
        ontology: String,
        page: QueryExecuteOperation.Page,
        budget: DatabaseExecutionBudget,
        transaction: any TransactionAccess
    ) async throws -> DatabaseValidationReport
}
