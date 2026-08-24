import DatabaseKit
import StorageKit

/// A schema-declared index admitted for reading in the caller's transaction.
public struct ReadableIndex: Sendable {
    public let descriptor: IndexDescriptor
    public let physicalLayout: IndexPhysicalLayout
    public let subspace: Subspace

    package init(
        descriptor: IndexDescriptor,
        physicalLayout: IndexPhysicalLayout,
        subspace: Subspace
    ) {
        self.descriptor = descriptor
        self.physicalLayout = physicalLayout
        self.subspace = subspace
    }
}
