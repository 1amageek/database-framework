import DatabaseValue

package struct DatabasePartitionCatalogPage: Sendable {
    package let entries: [DatabasePartitionCatalogEntry]
    package let continuation: DatabaseBytes?
}
