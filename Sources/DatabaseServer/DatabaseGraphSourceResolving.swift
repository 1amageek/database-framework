import DatabaseWire

public protocol DatabaseGraphSourceResolving: Sendable {
    func resolve(
        _ source: GraphAlgorithmOperation.Source
    ) async throws -> ResolvedDatabaseGraphSource
}
