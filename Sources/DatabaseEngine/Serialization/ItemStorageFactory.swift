import StorageKit

/// Container-scoped factory that binds the validated storage policy to a transaction.
public struct ItemStorageFactory: Sendable {
    public let configuration: ItemStorageConfiguration

    public init(configuration: ItemStorageConfiguration) {
        self.configuration = configuration
    }

    public func make(
        transaction: any Transaction,
        blobsSubspace: Subspace
    ) -> ItemStorage {
        ItemStorage(
            transaction: transaction,
            blobsSubspace: blobsSubspace,
            configuration: configuration
        )
    }
}
