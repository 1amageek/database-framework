import StorageKit

enum PermutedQueryResultAssembler {
    static func requireItems<Item>(
        identifiers: [Tuple],
        fetchedItems: [Item?],
        indexName: String
    ) throws -> [Item] {
        guard identifiers.count == fetchedItems.count else {
            throw PermutedQueryError.fetchedItemCountMismatch(
                index: indexName,
                expected: identifiers.count,
                actual: fetchedItems.count
            )
        }

        var items: [Item] = []
        items.reserveCapacity(fetchedItems.count)
        for (identifier, item) in zip(identifiers, fetchedItems) {
            guard let item else {
                throw PermutedQueryError.indexedItemMissing(
                    index: indexName,
                    primaryKey: identifier.pack()
                )
            }
            items.append(item)
        }
        return items
    }
}
