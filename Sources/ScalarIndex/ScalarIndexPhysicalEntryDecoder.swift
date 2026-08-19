import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

public struct ScalarIndexPhysicalEntryDecoder: IndexPhysicalEntryDecoder {
    public init() {}

    public func decode(
        key: ByteString,
        in indexSubspace: Subspace,
        index: ResolvedIndex
    ) throws -> IndexPhysicalEntry {
        let indexedFieldCount = index.fieldNames.count
        guard indexedFieldCount > 0 else {
            throw ScalarIndexPhysicalEntryError.invalidIndexedFieldCount(
                indexedFieldCount
            )
        }

        var cursor = try indexSubspace.tupleCursor(for: key)
        var indexedValues: [FieldValue] = []
        indexedValues.reserveCapacity(indexedFieldCount)
        for fieldIndex in 0..<indexedFieldCount {
            let element = try cursor.requireNext()
            do {
                indexedValues.append(try FieldValue(tupleElement: element))
            } catch let error {
                throw ScalarIndexPhysicalEntryError.invalidIndexedValue(
                    fieldIndex: fieldIndex,
                    reason: error
                )
            }
        }

        var primaryKeyElements: [any TupleElement] = []
        while let element = try cursor.next() {
            primaryKeyElements.append(element)
        }
        guard !primaryKeyElements.isEmpty else {
            throw ScalarIndexPhysicalEntryError.missingPrimaryKey
        }
        return IndexPhysicalEntry(
            indexedValues: indexedValues,
            primaryKey: Tuple(primaryKeyElements)
        )
    }
}
