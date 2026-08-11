#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

public protocol DatabaseOntologyProcessor: Sendable {
    func replace(
        _ document: OntologyExecuteOperation.Document,
        budget: ExecutionBudget,
        transaction: any TransactionAccess
    ) async throws

    func delete(
        ontology: String,
        budget: ExecutionBudget,
        transaction: any TransactionAccess
    ) async throws

    func reason(
        ontology: String,
        profile: OntologyExecuteOperation.ReasoningProfile,
        page: QueryExecuteOperation.Page,
        budget: ExecutionBudget,
        transaction: any TransactionAccess
    ) async throws -> OntologyExecuteOperation.InferencePage

    func hierarchy(
        ontology: String,
        resource: String,
        resourceKind: OntologyExecuteOperation.HierarchyResourceKind,
        direction: OntologyExecuteOperation.HierarchyDirection,
        maximumDepth: UInt32,
        page: QueryExecuteOperation.Page,
        budget: ExecutionBudget,
        transaction: any TransactionAccess
    ) async throws -> OntologyExecuteOperation.HierarchyPage

    func validateSchema(
        ontology: String,
        page: QueryExecuteOperation.Page,
        budget: ExecutionBudget,
        transaction: any TransactionAccess
    ) async throws -> ValidationReport
}

#endif
