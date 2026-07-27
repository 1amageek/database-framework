public enum LiteralConversionError: Error, Sendable, Equatable,
    CustomStringConvertible
{
    public enum FieldValueUnsupportedLiteralKind: String, Sendable, Equatable {
        case object
        case reference
        case time
        case dateTime
        case timeSpan
        case calendarPeriod
        case geographicPoint
        case geographicPosition
        case vector
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
            return "FieldValue kind has no QueryIR literal representation: \(kind.rawValue)"
        case .fieldValueConversionInvariantViolation:
            return "Literal conversion produced a non-RDF canonical value for an RDF literal"
        }
    }
}
