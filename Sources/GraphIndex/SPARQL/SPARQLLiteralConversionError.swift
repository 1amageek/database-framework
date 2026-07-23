public enum SPARQLLiteralConversionError: Error, Sendable, Equatable {
    case nullTermUnsupported
    case arrayTermUnsupported
    case invalidLexicalForm(value: String, datatype: String)
    case literalTooLarge(requiredUTF8Count: UInt64, maximumUTF8Count: UInt64)
}
