import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine
import StorageKit

/// A rewritten SQL SELECT whose dynamically inlined values remain request
/// accounted for exactly as long as the executable query can borrow them.
@_spi(DatabaseExecution)
public struct DatabasePreparedSQLSelect: Sendable {
    private let query: SelectQuery
    private let workMeter: DatabaseWorkMeter
    private let retainedStorage: DatabasePreparedSQLSelectStorage?

    package init(
        query: SelectQuery,
        workMeter: DatabaseWorkMeter,
        retainedStorage: DatabasePreparedSQLSelectStorage? = nil
    ) {
        self.query = query
        self.workMeter = workMeter
        self.retainedStorage = retainedStorage
    }

    /// Executes the prepared query without exposing its copyable syntax value.
    /// Dynamic literals therefore cannot outlive the reservation retained by
    /// this owner. Preparation and execution must share one request meter.
    public func execute(
        in session: DatabaseReadSession,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        guard execution.workMeter === workMeter else {
            throw DatabasePreparedSQLSelectError.workMeterMismatch
        }
        let lifetimeOwner = retainedStorage
        defer { withExtendedLifetime(lifetimeOwner) {} }
        return try await session.executeCanonical(
            query,
            execution: execution,
            graphPartitions: graphPartitions
        )
    }

}
