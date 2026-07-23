enum CompiledFacet: Sendable {
    case minInclusive(XSDParsedValue)
    case maxInclusive(XSDParsedValue)
    case minExclusive(XSDParsedValue)
    case maxExclusive(XSDParsedValue)
    case length(XSDDecimalValue)
    case minLength(XSDDecimalValue)
    case maxLength(XSDDecimalValue)
    case pattern(XSDRegularExpression)
    case totalDigits(XSDDecimalValue)
    case fractionDigits(XSDDecimalValue)
    case languageRange(RDFLanguageRange)
}
