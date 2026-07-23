import Core
import DatabaseValue
import StorageKit

public extension Persistable {
    /// Resolves this model's identifier through the canonical storage-key codec.
    func recordIdentifierTuple(
        limits: RecordIdentifierLimits = .default
    ) throws(RecordIdentifierValidationError) -> Tuple {
        try RecordIdentifierKeyCodec.tuple(for: self, limits: limits)
    }
}
