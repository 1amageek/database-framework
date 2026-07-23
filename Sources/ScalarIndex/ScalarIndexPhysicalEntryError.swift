import DatabaseEngine

public enum ScalarIndexPhysicalEntryError: Error, Sendable, Equatable {
    case invalidIndexedFieldCount(Int)
    case invalidIndexedValue(
        fieldIndex: Int,
        reason: FieldValueTupleCodecError
    )
    case missingPrimaryKey
}
