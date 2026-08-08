public enum OWLClassRDFIndexError: Error, Sendable, Equatable, CustomStringConvertible {
    case rootDoesNotConform(typeName: String)
    case rootMismatch(expected: String, actual: String)
    case projectionConfigurationMismatch(typeName: String)
    case entityMismatch(expected: String, actual: String)
    case missingIdentifier(entity: String)
    case missingPropertyField(entity: String, field: String)
    case missingOWLClassBinding(entity: String)

    public var description: String {
        switch self {
        case .rootDoesNotConform(let typeName):
            return "OWL RDF index root '\(typeName)' does not conform to OWLClassEntity"
        case .rootMismatch(let expected, let actual):
            return "OWL RDF index root mismatch: expected '\(expected)', got '\(actual)'"
        case .projectionConfigurationMismatch(let typeName):
            return "OWL RDF projection metadata does not match compiled type '\(typeName)'"
        case .entityMismatch(let expected, let actual):
            return "OWL RDF projection expected entity '\(expected)', got '\(actual)'"
        case .missingIdentifier(let entity):
            return "OWL RDF projection entity '\(entity)' has no canonical id field"
        case .missingPropertyField(let entity, let field):
            return "OWL RDF projection entity '\(entity)' has no canonical field '\(field)'"
        case .missingOWLClassBinding(let entity):
            return "OWL RDF projection entity '\(entity)' has no OWL class binding"
        }
    }
}
