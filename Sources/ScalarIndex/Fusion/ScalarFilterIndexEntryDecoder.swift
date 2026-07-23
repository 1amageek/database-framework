import StorageKit

enum ScalarFilterIndexEntryDecoder {
    static func primaryKey(
        from tuple: Tuple,
        remainingIndexedFieldCount: Int,
        fieldName: String
    ) throws -> Tuple {
        guard remainingIndexedFieldCount >= 0,
              tuple.count > remainingIndexedFieldCount else {
            throw FilterError.malformedIndexEntry(
                fieldName: fieldName,
                indexedFieldCount: remainingIndexedFieldCount,
                elementCount: tuple.count
            )
        }

        var primaryKeyElements: [any TupleElement] = []
        primaryKeyElements.reserveCapacity(
            tuple.count - remainingIndexedFieldCount
        )
        for index in remainingIndexedFieldCount..<tuple.count {
            do {
                primaryKeyElements.append(try tuple.element(at: index))
            } catch {
                throw FilterError.malformedIndexEntry(
                    fieldName: fieldName,
                    indexedFieldCount: remainingIndexedFieldCount,
                    elementCount: tuple.count
                )
            }
        }
        return Tuple(primaryKeyElements)
    }
}
