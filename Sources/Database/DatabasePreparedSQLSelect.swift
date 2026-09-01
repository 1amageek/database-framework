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

    /// Executes the prepared query for the complete visible result and moves
    /// it into shared ownership.
    ///
    /// Complete staging disables the client-facing page window rather than
    /// paging, so the returned rows are the whole visible result and never a
    /// truncated prefix. The request row budget still applies through the
    /// shared work meter and reports its own typed limit failure. The
    /// returned rows hold the request reservation until their last owner is
    /// released, so a durable query snapshot may count them and emit bounded
    /// pages after the read snapshot that produced them has closed.
    public func stageCompleteRows(
        in session: DatabaseReadSession,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> DatabaseSharedRetainedQueryRows {
        guard execution.workMeter === workMeter else {
            throw DatabasePreparedSQLSelectError.workMeterMismatch
        }
        let lifetimeOwner = retainedStorage
        defer { withExtendedLifetime(lifetimeOwner) {} }
        let page = try await session.retainedCanonicalPage(
            query,
            execution: execution.withoutExternalPageWindow(),
            graphPartitions: graphPartitions
        )
        guard page.continuation == nil else {
            throw DatabasePreparedSQLSelectError.stagedResultIsIncomplete
        }
        return try DatabaseSharedRetainedQueryRows(
            rows: page.takeRows(),
            at: .resultMaterialization
        )
    }
}
