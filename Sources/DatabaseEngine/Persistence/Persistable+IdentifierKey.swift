import Core
import DatabaseValue
import StorageKit

public extension Persistable {
    /// Resolves this model's identifier through the canonical storage-key codec.
    func persistableIdentifierTuple(
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierValidationError) -> Tuple {
        try PersistableIdentifierKeyCodec.tuple(for: self, limits: limits)
    }
}
