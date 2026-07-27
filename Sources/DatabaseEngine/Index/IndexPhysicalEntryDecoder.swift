import DatabaseTypes
import StorageKit

/// Decodes one index kind's physical key layout.
public protocol IndexPhysicalEntryDecoder: Sendable {
    func decode(
        key: ByteString,
        in indexSubspace: Subspace,
        index: Index
    ) throws -> IndexPhysicalEntry
}
