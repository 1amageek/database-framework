import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseWire
import StorageKit

extension DatabaseContext {
    /// Opens a logical query scope over this context's existing authority.
    /// One concurrent execution is the default; explicit multiplexing is bounded.
    public func openConnection(maximumConcurrentExecutions: Int = 1,
        limits: DatabaseWireLimits = .default,
        structuralLimits: QueryStructuralLimits = .default,
        checkpointCeiling: TransactionCheckpointLimits? = nil) async throws -> QueryConnection {
        guard maximumConcurrentExecutions > 0 else { throw QueryConnectionError.invalidConcurrencyLimit }
        return try await withDataOperation {
            QueryConnection(context: self, wireLimits: limits, structuralLimits: structuralLimits,
                checkpointCeiling: checkpointCeiling, concurrencyLimit: maximumConcurrentExecutions)
        }
    }
}
