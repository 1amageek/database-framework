import DatabaseKit
import StorageKit

/// A schema-declared index admitted for reading in the caller's transaction.
public struct ReadableIndex: Sendable {
    public let descriptor: IndexDescriptor
    public let subspace: Subspace

    package init(
        descriptor: IndexDescriptor,
        subspace: Subspace
    ) {
        self.descriptor = descriptor
        self.subspace = subspace
    }
}
