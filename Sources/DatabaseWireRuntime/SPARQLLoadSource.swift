#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
public protocol SPARQLLoadSource: Sendable {
    func load(_ request: SPARQLLoadRequest) async throws -> SPARQLLoadDocument
}

#endif
