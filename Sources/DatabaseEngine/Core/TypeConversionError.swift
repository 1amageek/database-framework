import DatabaseKit

public indirect enum TypeConversionError: Error, Sendable, Equatable {
    case unsupportedType(String)
    case invalidFieldValue(PersistableEncodingError)
    case invalidCollectionElement(index: Int, reason: TypeConversionError)
}
