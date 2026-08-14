import DatabaseTypes

@_spi(DatabaseExecution)
public struct DatabasePartitionCatalogItem: Sendable {
    public let entity: String
    public let partitions: FieldObject

    package init(entity: String, partitions: FieldObject) {
        self.entity = entity
        self.partitions = partitions
    }
}

@_spi(DatabaseExecution)
public struct DatabasePartitionCatalogPage: Sendable {
    public let entries: [DatabasePartitionCatalogItem]
    public let continuation: ByteString?

    package init(
        entries: [DatabasePartitionCatalogItem],
        continuation: ByteString?
    ) {
        self.entries = entries
        self.continuation = continuation
    }
}
