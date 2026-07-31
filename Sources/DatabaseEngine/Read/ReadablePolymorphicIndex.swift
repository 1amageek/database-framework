import DatabaseKit
import StorageKit

/// A polymorphic index admitted for reading in the caller's transaction.
public struct ReadablePolymorphicIndex: Sendable {
    public let descriptor: PolymorphicIndexMetadata
    public let subspace: Subspace

    package init(
        descriptor: PolymorphicIndexMetadata,
        subspace: Subspace
    ) {
        self.descriptor = descriptor
        self.subspace = subspace
    }
}
