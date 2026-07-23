public indirect enum TypeConversionError: Error, Sendable, Equatable {
    case unsupportedType(String)
    case invalidCollectionElement(index: Int, reason: TypeConversionError)
}
