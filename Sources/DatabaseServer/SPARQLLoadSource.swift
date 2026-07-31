#if DATABASE_SERVER_GRAPH_INDEXES
public protocol SPARQLLoadSource: Sendable {
    func load(_ request: SPARQLLoadRequest) async throws -> SPARQLLoadDocument
}

#endif
