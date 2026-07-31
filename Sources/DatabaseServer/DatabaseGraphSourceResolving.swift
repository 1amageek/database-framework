#if DATABASE_SERVER_GRAPH_INDEXES
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

public protocol DatabaseGraphSourceResolving: Sendable {
    func resolve(
        _ source: GraphAlgorithmOperation.Source,
        transaction: any TransactionAccess
    ) async throws -> ResolvedDatabaseGraphSource
}
#endif
