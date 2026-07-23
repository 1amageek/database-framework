public enum LiteralConversionError: Error, Sendable, Equatable,
    CustomStringConvertible
{
    public enum FieldValueUnsupportedLiteralKind: String, Sendable, Equatable {
        case decimal
        case date
        case timestamp
        case uuid
    }

    case invalidRDFLiteral(datatype: String)
    case invalidLanguageTag(String)
    case invalidBaseDirection(String)
    case fieldValueUnsupported(kind: FieldValueUnsupportedLiteralKind)
    case fieldValueConversionInvariantViolation

    public var description: String {
        switch self {
        case .invalidRDFLiteral(let datatype):
            return "Invalid RDF literal datatype: \(datatype)"
        case .invalidLanguageTag(let language):
            return "Invalid RDF language tag: \(language)"
        case .invalidBaseDirection(let direction):
            return "Invalid RDF base direction: \(direction)"
        case .fieldValueUnsupported(let kind):
            return "FieldValue cannot preserve QueryIR literal kind: \(kind.rawValue)"
        case .fieldValueConversionInvariantViolation:
            return "Literal conversion produced a non-RDF canonical value for an RDF literal"
        }
    }
}
