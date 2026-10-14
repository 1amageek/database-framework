#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

/// Source-family boundary used by the domain-neutral Composition row merger.
///
/// The conforming feature package owns source validation, Base-local execution,
/// and any identity qualification required before a row crosses a Base
/// boundary. DatabaseEngine owns only the global merge, window, aggregate, and
/// provenance mechanics.
@_spi(DatabaseExecution)
public protocol CompositionMemberQueryExecutor: Sendable {
    func validate(_ query: SelectQuery) throws

    /// Binds the complete output before any member starts fetching rows.
    func columns(
        session: DatabaseReadSession,
        query: SelectQuery
    ) throws -> [QueryColumn]

    func execute(
        session: DatabaseReadSession,
        query: SelectQuery,
        execution: ReadExecutionContext
    ) async throws -> DatabaseRetainedQueryPage

    /// Measures the exact destination row before `prepare` allocates it.
    func preparedFootprint(
        of row: borrowing QueryRow,
        sourceBaseID: Base.ID,
        workMeter: DatabaseWorkMeter
    ) throws -> (rows: UInt64, bytes: UInt64)

    /// Measures the actual prepared destination under the same payload-sharing
    /// contract as the prospective footprint.
    func observedPreparedFootprint(
        of row: borrowing QueryRow,
        workMeter: DatabaseWorkMeter
    ) throws -> (rows: UInt64, bytes: UInt64)

    func prepare(
        _ row: QueryRow,
        columns: [QueryColumn],
        sourceBaseID: Base.ID
    ) throws -> QueryRow
}

extension CompositionMemberQueryExecutor {
    /// Arbitrary conformers conservatively own their complete prepared row.
    public func observedPreparedFootprint(
        of row: borrowing QueryRow,
        workMeter: DatabaseWorkMeter
    ) throws -> (rows: UInt64, bytes: UInt64) {
        let footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: row, workMeter: workMeter, stage: .resultMaterialization)
        return (footprint.rows, footprint.bytes)
    }
}
#endif
