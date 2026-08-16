import StorageKit

@_spi(DatabaseExecution)
public struct DatabaseMutationStateBinding: Sendable {
    package let root: Subspace

    package init(root: Subspace) {
        self.root = root
    }
}
