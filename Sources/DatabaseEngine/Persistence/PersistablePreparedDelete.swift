import DatabaseKit
import DatabaseTypes
import StorageKit

/// Read-only preparation for one canonical delete. It retains the observed
/// model until the transaction has admitted its mutation ownership.
struct PersistablePreparedDelete: Sendable {
    let persistedModel: PersistedModel
    let identifier: Tuple
    let key: ByteString
    let storage: ItemStorage
    let runtime: EntityRuntimeRegistration
    let transientReservation: DatabaseIntermediateReservation?
}
