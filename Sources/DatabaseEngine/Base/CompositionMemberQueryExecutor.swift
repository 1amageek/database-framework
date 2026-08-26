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

    func execute(
        session: DatabaseReadSession,
        query: SelectQuery,
        execution: ReadExecutionContext
    ) async throws -> QueryResponse

    func prepare(
        _ row: QueryRow,
        sourceBaseID: Base.ID
    ) throws -> QueryRow
}
#endif
