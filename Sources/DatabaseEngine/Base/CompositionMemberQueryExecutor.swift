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

    func admitLogicalRead(
        context: DatabaseContext,
        query: SelectQuery,
        restrictingTo entityNames: Set<String>?
    ) throws -> DatabaseReadAuthorizationAdmission

    func execute(
        context: DatabaseContext,
        query: SelectQuery,
        execution: ReadExecutionContext,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryResponse

    func prepare(
        _ row: DatabaseRetainedQueryRow,
        sourceBaseID: Base.ID
    ) throws -> QueryRow
}
#endif
