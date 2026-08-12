import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
public protocol SPARQLLoadSource: Sendable {
    func load(_ request: SPARQLLoadRequest) async throws -> SPARQLLoadDocument
}

#endif
