import DatabaseTypes
import DatabaseKit
import StorageKit

/// Result of persisting one compiled model in the primary store.
///
/// `encodedValue` retains the canonical storage buffer so derived projections
/// can reuse it without encoding or copying the payload again.
struct PersistableWriteResult: Sendable {
    let model: any Persistable
    let previousModel: (any Persistable)?
    let canonicalModel: PersistedModel
    let previousCanonicalModel: PersistedModel?
    let encodedValue: ByteString
    let identifier: Tuple
}
