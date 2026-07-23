public enum StatisticsCollectionError: Error, Sendable, Equatable {
    case unsupportedPhysicalLayout(
        indexName: String,
        kindIdentifier: String
    )
    case invalidPhysicalEntry(indexName: String, reason: String)
    case entryCountOverflow(indexName: String)
    case invalidRuntimeValue(field: String, reason: TypeConversionError)
    case missingFieldCollector(String)
    case keyOutsideSubspace
    case invalidElementCount(expectedAtLeast: Int, actual: Int)
    case invalidTerm
    case invalidSpatialCell
    case invalidVectorDimensions(expected: Int, actual: Int)
    case invalidFieldValue(
        fieldIndex: Int,
        reason: FieldValueTupleCodecError
    )
}
