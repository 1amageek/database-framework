#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

public protocol DatabaseGraphSourceResolving: Sendable {
    func resolve(
        _ source: GraphAlgorithmOperation.Source,
        transaction: any TransactionAccess
    ) async throws -> ResolvedDatabaseGraphSource
}
#endif
