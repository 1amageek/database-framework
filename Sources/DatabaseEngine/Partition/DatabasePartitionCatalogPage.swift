import DatabaseTypes

package struct DatabasePartitionCatalogPage: Sendable {
    package let entries: [DatabasePartitionCatalogEntry]
    package let continuation: ByteString?
}
