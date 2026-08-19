import DatabaseTypes
import StorageKit

/// Decodes one index type's physical key layout.
public protocol IndexPhysicalEntryDecoder: Sendable {
    func decode(
        key: ByteString,
        in indexSubspace: Subspace,
        index: ResolvedIndex
    ) throws -> IndexPhysicalEntry
}
