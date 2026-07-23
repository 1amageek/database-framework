/// Datatype map selected for ontology validation.
public enum XSDDatatypeProfile: Sendable, Equatable {
    /// OWL 2 datatype map. XSD-only datatypes are rejected explicitly.
    case owl2
    /// OWL 2 datatype map extended with the RDF 1.1 `rdf:langString` datatype.
    case owl2RDF11
    /// XSD 1.1 subset represented by `XSDDatatypeKind`.
    case extendedXSD11

    func supports(_ kind: XSDDatatypeKind) -> Bool {
        switch self {
        case .extendedXSD11:
            switch kind {
            case .owlReal, .owlRational, .rdfsLiteral, .rdfLangString,
                 .rdfPlainLiteral, .rdfXMLLiteral:
                return false
            default:
                return true
            }
        case .owl2, .owl2RDF11:
            switch kind {
            case .duration, .time, .date:
                return false
            case .rdfLangString:
                return self == .owl2RDF11
            default:
                return true
            }
        }
    }
}
