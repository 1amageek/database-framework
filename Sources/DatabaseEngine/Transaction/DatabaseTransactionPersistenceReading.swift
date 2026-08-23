import DatabaseKit

/// Package capability for dependent persistence reads that resolve a
/// schema-qualified identity without exposing transaction mutation methods.
package protocol DatabaseTransactionPersistenceReading:
    DatabaseTransactionReading
{
    func fetchPersistedModel(
        identifiedBy identity: EntityReference
    ) async throws -> PersistedModel?
}
