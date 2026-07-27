import DatabaseKit
import DatabaseTypes
import StorageKit

public extension Persistable {
    /// Resolves this model's identifier through the canonical storage-key codec.
    func persistableIdentifierTuple(
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        try PersistableIdentifierKeyCodec.tuple(for: self, limits: limits)
    }
}
