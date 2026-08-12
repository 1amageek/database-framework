import DatabaseOperationCore
import DatabaseTypes

/// Application-owned state that a resumable job can persist between slices.
///
/// The public contract is expressed only in DatabaseTypes primitives. The
/// operation runtime owns the bounded canonical serialization used for storage.
public protocol PersistentJobPayload: Sendable {
    func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue

    init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError)
}
