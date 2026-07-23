public protocol SPARQLLoadSource: Sendable {
    func load(_ request: SPARQLLoadRequest) async throws -> SPARQLLoadDocument
}
