import DatabaseKit
import DatabaseTypes

/// Read-only preparation for one canonical write. No storage or index mutation
/// occurs until `DatabaseDataStore.commitPreparedWrite` is called.
struct PersistablePreparedWrite: Sendable {
    let result: PersistableWriteResult
    let key: ByteString
    let storage: ItemStorageWriter
    let runtime: EntityRuntimeRegistration
    let transientReservation: DatabaseIntermediateReservation?
}
