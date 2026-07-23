public final class AnySPARQLLoadSource: SPARQLLoadSource, Sendable {
    private let loadDocument: @Sendable (
        SPARQLLoadRequest
    ) async throws -> SPARQLLoadDocument

    public init<Source: SPARQLLoadSource>(_ source: Source) {
        self.loadDocument = { request in
            try await source.load(request)
        }
    }

    public func load(
        _ request: SPARQLLoadRequest
    ) async throws -> SPARQLLoadDocument {
        try await loadDocument(request)
    }

    public static var unconfigured: AnySPARQLLoadSource {
        AnySPARQLLoadSource(UnconfiguredSPARQLLoadSource())
    }
}

private struct UnconfiguredSPARQLLoadSource: SPARQLLoadSource {
    func load(_ request: SPARQLLoadRequest) async throws -> SPARQLLoadDocument {
        throw SPARQLLoadSourceError.notConfigured
    }
}
