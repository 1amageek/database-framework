import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit

public protocol DatabaseGraphSourceResolving: Sendable {
    func resolve(
        _ source: GraphAlgorithmOperation.Source,
        transaction: any TransactionAccess
    ) async throws -> ResolvedDatabaseGraphSource
}
#endif
