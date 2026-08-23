import StorageKit

/// Container-scoped factory that binds the validated storage policy to a transaction.
public struct ItemStorageFactory: Sendable {
    public let configuration: ItemStorageConfiguration

    public init(configuration: ItemStorageConfiguration) {
        self.configuration = configuration
    }

    public func makeWriter(
        transaction: any TransactionAccess,
        blobsSubspace: Subspace
    ) -> ItemStorageWriter {
        ItemStorageWriter(
            transaction: transaction,
            blobsSubspace: blobsSubspace,
            configuration: configuration
        )
    }

    public func makeReader(
        transaction: any TransactionReadAccess,
        blobsSubspace: Subspace
    ) -> ItemStorageReader {
        ItemStorageReader(
            transaction: transaction,
            blobsSubspace: blobsSubspace,
            configuration: configuration
        )
    }
}
