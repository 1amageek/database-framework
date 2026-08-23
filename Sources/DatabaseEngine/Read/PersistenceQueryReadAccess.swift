import DatabaseKit
import StorageKit

/// Persistence-only query capability bound to one hidden database snapshot.
///
/// Feature targets can resolve declared entities without receiving root-wide
/// physical storage access or mutation authority.
package protocol PersistenceQueryReadAccess: Sendable {
    func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple
}
