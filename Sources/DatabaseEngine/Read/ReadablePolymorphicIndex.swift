import DatabaseKit
import StorageKit

/// A polymorphic index admitted for reading in the caller's transaction.
public struct ReadablePolymorphicIndex: Sendable {
    public let descriptor: IndexDeclaration<String>
    public let subspace: Subspace

    package init(
        descriptor: IndexDeclaration<String>,
        subspace: Subspace
    ) {
        self.descriptor = descriptor
        self.subspace = subspace
    }
}
